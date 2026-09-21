"""Tests for the ``/readyz`` route's two operating shapes.

The route branches on whether API3 service-account credentials are
configured:

- both ``client_id`` and ``client_secret`` set → live login/logout cycle
  via :meth:`LookerClient.check_connectivity` (service-account mode).
- either credential missing → no-auth HEAD against ``base_url`` via
  :meth:`LookerClient.check_reachability` (external-identity mode, e.g.
  OAuth pass-through, where per-request user tokens supply auth).

The route must always 503 when ``base_url`` itself is empty regardless
of which mode applies.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
import respx
from starlette.applications import Starlette
from starlette.testclient import TestClient

from looker_mcp_server.config import LookerConfig
from looker_mcp_server.server import create_server


def _config(**overrides: Any) -> LookerConfig:
    base: dict[str, Any] = {
        "base_url": "https://test.looker.com",
        "client_id": "test-id",
        "client_secret": "test-secret",
        "sudo_as_user": False,
    }
    base.update(overrides)
    return LookerConfig(_env_file=None, **base)  # type: ignore[call-arg]


@pytest.fixture
def starlette_app_factory():
    """Build the Starlette ASGI app for a given config + close the
    LookerClient at teardown so per-test httpx state doesn't leak.
    """
    pending: list[Any] = []

    def _build(config: LookerConfig) -> Starlette:
        mcp, client = create_server(config)
        app = mcp.http_app()
        assert isinstance(app, Starlette)
        pending.append(client)
        return app

    yield _build

    # The Starlette ``TestClient`` ``with`` block has already exited by
    # the time fixture teardown runs, so no event loop is active here.
    # ``asyncio.run`` is the simple, correct way to drive the async
    # close from a synchronous teardown.
    for client in pending:
        asyncio.run(client.close())


class TestReadyzBaseUrlGuard:
    """``base_url`` is the one piece of config readyz enforces in every
    mode — without it the server has nothing to probe at all.
    """

    def test_returns_503_when_base_url_unset(self, starlette_app_factory):
        app = starlette_app_factory(_config(base_url=""))
        with TestClient(app) as http:
            resp = http.get("/readyz")
        assert resp.status_code == 503
        assert resp.json() == {
            "status": "not_ready",
            "reason": "LOOKER_BASE_URL not configured",
        }


class TestReadyzServiceAccountMode:
    """When both API3 credentials are configured, readiness exercises
    a real login/logout cycle so a broken credential pair fails the
    probe early rather than at first tool invocation.
    """

    @respx.mock
    def test_returns_ready_when_login_logout_succeeds(self, starlette_app_factory):
        config = _config()
        respx.post(f"{config.api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "tok"})
        )
        respx.delete(f"{config.api_url}/logout").mock(return_value=httpx.Response(204))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 200
        assert resp.json() == {"status": "ready"}

    @respx.mock
    def test_returns_503_when_login_fails(self, starlette_app_factory):
        config = _config()
        respx.post(f"{config.api_url}/login").mock(
            return_value=httpx.Response(401, json={"message": "bad creds"})
        )

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 503
        assert resp.json() == {
            "status": "not_ready",
            "reason": "Cannot connect to Looker",
        }


class TestReadyzExternalIdentityMode:
    """When API3 credentials are absent the server is operating in an
    external-identity shape (OAuth pass-through / sudo header / etc.)
    and has nothing to log in with. Readiness collapses to a no-auth
    reachability check against the configured ``base_url``.
    """

    @respx.mock
    def test_returns_ready_when_base_url_responds(self, starlette_app_factory):
        config = _config(client_id="", client_secret="")
        # Any HTTP response — including a 401 from the unauthenticated
        # web root — proves the instance is reachable. Readiness does
        # not care about the status code.
        respx.head(config.base_url).mock(return_value=httpx.Response(401))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 200
        assert resp.json() == {"status": "ready"}

    @respx.mock
    def test_returns_ready_on_2xx(self, starlette_app_factory):
        config = _config(client_id="", client_secret="")
        respx.head(config.base_url).mock(return_value=httpx.Response(200))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 200

    @respx.mock
    def test_returns_503_when_base_url_unreachable(self, starlette_app_factory):
        config = _config(client_id="", client_secret="")
        respx.head(config.base_url).mock(side_effect=httpx.ConnectError("refused"))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 503
        assert resp.json() == {
            "status": "not_ready",
            "reason": "Looker base URL unreachable",
        }

    @respx.mock
    def test_succeeds_when_only_one_credential_is_set(self, starlette_app_factory):
        """A half-configured cred pair (one field set, the other empty)
        is still external-identity mode by the route's branch test —
        the API3 login flow needs both halves to be usable. Readiness
        therefore falls back to the reachability path rather than 503-ing
        on a degenerate login attempt.
        """
        config = _config(client_id="only-id", client_secret="")
        respx.head(config.base_url).mock(return_value=httpx.Response(200))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 200


class TestReadyzIsNotTraced:
    """The probe's own outbound call must not become a span.

    A kubelet calls ``/readyz`` on a fixed period and carries no inbound
    trace context, so in a deployment that auto-instruments httpx each
    probe becomes a new ROOT trace. At ``periodSeconds: 10`` that is
    ~8.6k traces/day per replica; on one real deployment it reached 98%
    of the trace store and buried the user traffic the store existed to
    record.

    This package does not depend on OpenTelemetry — the spans are created
    by the embedding deployment. These tests therefore stand in a fake
    ``opentelemetry.instrumentation.utils`` rather than adding a
    dependency, and assert the contract this package actually owns:
    that the probe's HTTP call happens INSIDE the suppression window.
    """

    @staticmethod
    def _install_fake_otel(monkeypatch, events: list[str]) -> None:
        """Put a recording ``suppress_instrumentation`` on the import path.

        Every parent package is injected too: ``from a.b.c import d``
        resolves ``a`` then ``a.b`` then ``a.b.c``, so seeding only the
        leaf would still attempt a real import of the parents and fail.
        """
        import sys
        from contextlib import contextmanager
        from types import ModuleType

        @contextmanager
        def _suppress():
            events.append("suppress:enter")
            try:
                yield
            finally:
                events.append("suppress:exit")

        utils = ModuleType("opentelemetry.instrumentation.utils")
        utils.suppress_instrumentation = _suppress  # type: ignore[attr-defined]
        instrumentation = ModuleType("opentelemetry.instrumentation")
        instrumentation.utils = utils  # type: ignore[attr-defined]
        root = ModuleType("opentelemetry")
        root.instrumentation = instrumentation  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, "opentelemetry", root)
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation", instrumentation)
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.utils", utils)

    @respx.mock
    def test_the_probe_runs_inside_the_suppression_window(self, starlette_app_factory, monkeypatch):
        """The load-bearing assertion, and it is about ORDER.

        Asserting merely that suppression was entered would still pass if
        the ``with`` block were moved to wrap nothing — which is exactly
        the regression that would silently restore ~8.6k traces/day. The
        event sequence pins the probe between enter and exit.
        """
        events: list[str] = []
        self._install_fake_otel(monkeypatch, events)

        config = _config(client_id="", client_secret="")

        def _record_probe(request):
            events.append("probe")
            return httpx.Response(200)

        respx.head(config.base_url).mock(side_effect=_record_probe)

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert resp.status_code == 200
        assert events == ["suppress:enter", "probe", "suppress:exit"]

    @respx.mock
    def test_service_account_mode_is_suppressed_too(self, starlette_app_factory, monkeypatch):
        """Both readiness shapes reach Looker over HTTP, so covering only
        the reachability branch would leave a service-account deployment
        emitting the same per-probe trace.
        """
        events: list[str] = []
        self._install_fake_otel(monkeypatch, events)

        config = _config()

        def _record_login(request):
            events.append("probe")
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600})

        respx.post(f"{config.base_url}/api/4.0/login").mock(side_effect=_record_login)
        respx.delete(f"{config.base_url}/api/4.0/logout").mock(return_value=httpx.Response(204))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            http.get("/readyz")

        assert events[0] == "suppress:enter"
        assert events[-1] == "suppress:exit"
        assert "probe" in events

    @respx.mock
    def test_readiness_is_unchanged_when_opentelemetry_is_absent(
        self, starlette_app_factory, monkeypatch
    ):
        """The positive control: the no-op path must still probe.

        Without this, the tests above could pass against a helper that
        swallowed the probe entirely rather than merely untracing it.

        The unavailability is forced rather than assumed. A bare
        ``find_spec("opentelemetry")`` would be the wrong precondition —
        ``opentelemetry`` (the API) arrives transitively here as a
        namespace package while ``opentelemetry.instrumentation`` does
        not, so asserting on the parent tests the wrong thing and couples
        this control to whatever a transitive dependency happens to pull
        in. Setting the module to ``None`` makes the import raise, which
        is the condition the guarded import actually handles.
        """
        import sys

        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.utils", None)

        config = _config(client_id="", client_secret="")
        route = respx.head(config.base_url).mock(side_effect=httpx.ConnectError("refused"))

        app = starlette_app_factory(config)
        with TestClient(app) as http:
            resp = http.get("/readyz")

        assert route.called, "the probe was skipped, not merely untraced"
        assert resp.status_code == 503
        assert resp.json()["reason"] == "Looker base URL unreachable"
