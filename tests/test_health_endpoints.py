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
