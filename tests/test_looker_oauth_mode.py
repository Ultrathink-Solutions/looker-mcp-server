"""Tests for ``LOOKER_MCP_MODE=looker_oauth`` — Looker-as-its-own-authorization-
server, opaque-token posture.

Covers:
- :class:`LookerUserIntrospector` — opaque-token verification via Looker
  ``GET /user`` (accept iff Looker returns a user; reject on 401/403, non-200,
  non-JSON, missing id, or transport failure).
- :class:`LookerOAuthAuthMiddleware` — the ASGI gate (valid token passes
  through with the user stashed; missing/malformed/invalid → 401; bearer-in-
  query → 400; bypass paths anonymous).
- ``create_server`` provider selection (no-cred ``OAuthIdentityProvider``).
- ``create_server`` PRM advertises Looker as the authorization server.
- ``build_looker_oauth_mode_middleware`` wiring.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

from looker_mcp_server.config import LookerConfig, LookerMcpMode
from looker_mcp_server.identity import OAuthIdentityProvider
from looker_mcp_server.oidc import (
    LookerOAuthAuthMiddleware,
    LookerUserIntrospector,
    OpaqueTokenVerificationError,
)
from looker_mcp_server.server import (
    PRM_PATH,
    build_looker_oauth_mode_middleware,
    build_public_mode_middleware,
    create_server,
)

BASE_URL = "https://test.looker.com"
USER_URL = f"{BASE_URL}/api/4.0/user"
# This MCP server's own public URI (RFC 9728 resource) — distinct from Looker.
RESOURCE_URI = "https://mcp.test.example.com"


def _looker_oauth_config(**overrides: Any) -> LookerConfig:
    base: dict[str, Any] = {
        "base_url": BASE_URL,
        "mcp_mode": LookerMcpMode.LOOKER_OAUTH,
        "mcp_resource_uri": RESOURCE_URI,
    }
    base.update(overrides)
    return LookerConfig(_env_file=None, **base)  # type: ignore[call-arg]


# ── Introspector ─────────────────────────────────────────────────────


class TestLookerUserIntrospector:
    @pytest.mark.asyncio
    @respx.mock
    async def test_valid_token_returns_user(self):
        respx.get(USER_URL).respond(
            status_code=200,
            json={"id": 123, "email": "user@example.com", "display_name": "Test User"},
        )
        introspector = LookerUserIntrospector(BASE_URL)
        user = await introspector.verify("good-token")
        assert user.id == "123"
        assert user.email == "user@example.com"
        assert user.display_name == "Test User"

    @pytest.mark.asyncio
    @respx.mock
    async def test_forwards_token_as_looker_auth_header(self):
        """The introspection call presents the opaque token in the Looker
        ``Authorization: token <...>`` form."""
        route = respx.get(USER_URL).respond(status_code=200, json={"id": 1})
        introspector = LookerUserIntrospector(BASE_URL)
        await introspector.verify("the-opaque-token")
        assert route.called
        sent = route.calls.last.request
        assert sent.headers["authorization"] == "token the-opaque-token"

    @pytest.mark.asyncio
    async def test_empty_token_rejected(self):
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("")

    @pytest.mark.asyncio
    @respx.mock
    @pytest.mark.parametrize("status", [401, 403])
    async def test_looker_rejects_token(self, status: int):
        respx.get(USER_URL).respond(status_code=status, json={"message": "Not authenticated"})
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("expired-token")

    @pytest.mark.asyncio
    @respx.mock
    async def test_unexpected_status_rejected(self):
        respx.get(USER_URL).respond(status_code=500, text="boom")
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("token")

    @pytest.mark.asyncio
    @respx.mock
    async def test_non_json_response_rejected(self):
        respx.get(USER_URL).respond(status_code=200, text="<html>not json</html>")
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("token")

    @pytest.mark.asyncio
    @respx.mock
    async def test_missing_user_id_rejected(self):
        respx.get(USER_URL).respond(status_code=200, json={"email": "x@example.com"})
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("token")

    @pytest.mark.asyncio
    @respx.mock
    async def test_transport_failure_fails_closed(self):
        respx.get(USER_URL).mock(side_effect=httpx.ConnectError("refused"))
        introspector = LookerUserIntrospector(BASE_URL)
        with pytest.raises(OpaqueTokenVerificationError):
            await introspector.verify("token")

    @pytest.mark.asyncio
    @respx.mock
    async def test_honors_api_version(self):
        url = f"{BASE_URL}/api/4.1/user"
        respx.get(url).respond(status_code=200, json={"id": 7})
        introspector = LookerUserIntrospector(BASE_URL, api_version="4.1")
        user = await introspector.verify("token")
        assert user.id == "7"


# ── Middleware ───────────────────────────────────────────────────────


async def _echo_app(scope, receive, send):
    """Tiny ASGI app: emits 200 with the stashed looker user id (if present)."""
    user = (scope.get("state") or {}).get("looker_user")
    body = json.dumps({"looker_user_id": user.id if user is not None else None}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _drive(
    mw, *, method: str = "POST", path: str = "/mcp", headers=None, query_string: bytes = b""
):
    """Minimal ASGI driver — return (status, response_headers_dict, body_bytes)."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(k.encode().lower(), v.encode()) for k, v in (headers or {}).items()],
    }
    received: dict[str, Any] = {"status": None, "headers": {}, "body": b""}

    async def _recv():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message):
        if message["type"] == "http.response.start":
            received["status"] = message["status"]
            received["headers"] = {k.decode(): v.decode() for k, v in message["headers"]}
        elif message["type"] == "http.response.body":
            received["body"] += message.get("body", b"")

    await mw(scope, _recv, _send)
    return received["status"], received["headers"], received["body"]


def _mw() -> LookerOAuthAuthMiddleware:
    return LookerOAuthAuthMiddleware(
        _echo_app,
        introspector=LookerUserIntrospector(BASE_URL),
        realm=BASE_URL,
        prm_url=f"{BASE_URL}{PRM_PATH}",
    )


class TestLookerOAuthAuthMiddleware:
    @pytest.mark.asyncio
    @respx.mock
    async def test_valid_token_passes_through_with_user_stashed(self):
        respx.get(USER_URL).respond(status_code=200, json={"id": 55})
        status, _, body = await _drive(_mw(), headers={"Authorization": "Bearer good-token"})
        assert status == 200
        assert json.loads(body) == {"looker_user_id": "55"}

    @pytest.mark.asyncio
    @respx.mock
    async def test_authorization_header_intact_for_downstream(self):
        """The gate must NOT strip/mutate the Authorization header — the
        identity provider downstream forwards the same opaque token to
        Looker as the session token."""
        respx.get(USER_URL).respond(status_code=200, json={"id": 1})

        captured: dict[str, Any] = {}

        async def _capture_app(scope, receive, send):
            captured["authorization"] = None
            for k, v in scope.get("headers") or []:
                if k.lower() == b"authorization":
                    captured["authorization"] = v.decode()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        mw = LookerOAuthAuthMiddleware(
            _capture_app,
            introspector=LookerUserIntrospector(BASE_URL),
            realm=BASE_URL,
            prm_url=f"{BASE_URL}{PRM_PATH}",
        )
        status, _, _ = await _drive(mw, headers={"Authorization": "Bearer keep-me"})
        assert status == 200
        assert captured["authorization"] == "Bearer keep-me"

    @pytest.mark.asyncio
    @respx.mock
    async def test_looker_rejected_token_401(self):
        respx.get(USER_URL).respond(status_code=401, json={"message": "no"})
        status, headers, body = await _drive(_mw(), headers={"Authorization": "Bearer bad-token"})
        assert status == 401
        assert json.loads(body)["error"] == "invalid_token"
        www = headers.get("www-authenticate") or ""
        assert www.startswith("Bearer ")
        assert f'realm="{BASE_URL}"' in www
        assert f'resource_metadata="{BASE_URL}{PRM_PATH}"' in www

    @pytest.mark.asyncio
    @respx.mock
    async def test_missing_authorization_401_with_challenge(self):
        status, headers, body = await _drive(_mw())
        assert status == 401
        assert (headers.get("www-authenticate") or "").startswith("Bearer ")
        assert json.loads(body)["error"] == "invalid_token"

    @pytest.mark.asyncio
    @respx.mock
    async def test_malformed_scheme_401(self):
        status, _, _ = await _drive(_mw(), headers={"Authorization": "Basic abcd"})
        assert status == 401

    @pytest.mark.asyncio
    @respx.mock
    async def test_transport_failure_yields_401(self):
        respx.get(USER_URL).mock(side_effect=httpx.ConnectError("refused"))
        status, _, body = await _drive(_mw(), headers={"Authorization": "Bearer x"})
        assert status == 401
        assert json.loads(body)["error"] == "invalid_token"

    @pytest.mark.asyncio
    @respx.mock
    async def test_bearer_in_query_rejected_400(self):
        status, _, body = await _drive(_mw(), query_string=b"access_token=attacker")
        assert status == 400
        assert json.loads(body)["error"] == "invalid_request"

    @pytest.mark.asyncio
    @respx.mock
    async def test_well_known_bypasses_auth(self):
        status, _, _ = await _drive(_mw(), path="/.well-known/oauth-protected-resource")
        assert status == 200

    @pytest.mark.asyncio
    @respx.mock
    async def test_healthz_bypasses_auth(self):
        status, _, _ = await _drive(_mw(), path="/healthz")
        assert status == 200

    @pytest.mark.asyncio
    @respx.mock
    async def test_introspect_bypasses_auth(self):
        """The gateway-discovery ``/_introspect`` route runs its own guard."""
        status, _, _ = await _drive(_mw(), path="/_introspect")
        assert status == 200

    @pytest.mark.asyncio
    async def test_lifespan_scope_passes_through(self):
        mw = _mw()
        received: list[dict] = []

        async def recv():
            return {"type": "lifespan.startup"}

        async def send(msg):
            received.append(msg)

        await mw({"type": "lifespan"}, recv, send)
        statuses = [m.get("status") for m in received if m.get("type") == "http.response.start"]
        assert 401 not in statuses
        assert 400 not in statuses

    def test_empty_realm_rejected_at_construct(self):
        with pytest.raises(ValueError, match="realm"):
            LookerOAuthAuthMiddleware(
                _echo_app,
                introspector=LookerUserIntrospector(BASE_URL),
                realm="",
                prm_url=f"{BASE_URL}{PRM_PATH}",
            )


# ── build_looker_oauth_mode_middleware ───────────────────────────────


class TestBuildLookerOAuthModeMiddleware:
    def test_non_looker_oauth_mode_returns_none(self):
        dev = LookerConfig(_env_file=None, base_url=BASE_URL, mcp_mode=LookerMcpMode.DEV)  # type: ignore[call-arg]
        assert build_looker_oauth_mode_middleware(dev) is None

    def test_public_mode_returns_none(self):
        """The two gates are mutually exclusive — public-mode config does not
        produce a looker_oauth gate (and vice versa)."""
        pub = LookerConfig(
            _env_file=None,  # type: ignore[call-arg]
            base_url=BASE_URL,
            mcp_mode=LookerMcpMode.PUBLIC,
            mcp_jwks_uri="https://as.example.com/.well-known/jwks.json",
            mcp_issuer_url="https://as.example.com",
            mcp_resource_uri="https://looker.example.com/mcp",
        )
        assert build_looker_oauth_mode_middleware(pub) is None
        # And the public-mode builder returns None for a looker_oauth config.
        assert build_public_mode_middleware(_looker_oauth_config()) is None

    def test_looker_oauth_mode_returns_wrapped_middleware(self):
        mw = build_looker_oauth_mode_middleware(_looker_oauth_config())
        assert mw is not None
        assert mw.cls is LookerOAuthAuthMiddleware
        # realm + PRM URL are built from the MCP server's own resource URI
        # (NOT Looker's base URL — the MCP server serves the PRM).
        assert mw.kwargs["realm"] == RESOURCE_URI
        assert mw.kwargs["prm_url"] == f"{RESOURCE_URI}{PRM_PATH}"
        assert mw.kwargs["introspector"] is not None


# ── create_server: provider selection + PRM ──────────────────────────


class TestLookerOAuthProviderSelection:
    def test_selects_no_cred_oauth_provider(self):
        """In looker_oauth mode, the default identity provider is a no-cred
        ``OAuthIdentityProvider`` reading the Authorization-header bearer —
        NOT a DualMode/ApiKey/ArgumentSudo chain that needs admin creds."""
        mcp, client = create_server(_looker_oauth_config())
        try:
            provider = client._identity_provider  # type: ignore[attr-defined]
            assert isinstance(provider, OAuthIdentityProvider)
            # No fallback credentials — a tokenless request must fail.
            assert provider._fallback_id is None  # type: ignore[attr-defined]
            assert provider._fallback_secret is None  # type: ignore[attr-defined]
            # Reads the standard Authorization header and strips Bearer.
            assert provider._header == "authorization"  # type: ignore[attr-defined]
            assert provider._strip_bearer_scheme is True  # type: ignore[attr-defined]
        finally:
            import asyncio

            asyncio.run(client.close())

    @pytest.mark.asyncio
    async def test_resolves_opaque_token_to_oauth_identity(self):
        """End-to-end through the provider: an Authorization-header bearer
        resolves to a Looker ``oauth`` identity carrying the bare token."""
        from looker_mcp_server.identity import RequestContext

        mcp, client = create_server(_looker_oauth_config())
        try:
            provider = client._identity_provider  # type: ignore[attr-defined]
            ctx = RequestContext(
                headers={"authorization": "Bearer opaque-xyz"},
                tool_name="run_query",
                tool_group="query",
            )
            identity = await provider.resolve(ctx)
            assert identity.mode == "oauth"
            assert identity.access_token == "opaque-xyz"
        finally:
            await client.close()

    def test_api3_creds_do_not_re_enable_sudo_wrapper(self):
        """Security: even with admin API3 creds present, looker_oauth must NOT
        wrap the provider in ``ArgumentSudoIdentityProvider`` — doing so would
        re-enable the ``act_as_user`` escalation the posture forbids."""
        from looker_mcp_server.identity import ArgumentSudoIdentityProvider

        mcp, client = create_server(
            _looker_oauth_config(
                client_id="admin-id",
                client_secret="admin-secret",
                sudo_as_user=True,
            )
        )
        try:
            provider = client._identity_provider  # type: ignore[attr-defined]
            assert isinstance(provider, OAuthIdentityProvider)
            assert not isinstance(provider, ArgumentSudoIdentityProvider)
        finally:
            import asyncio

            asyncio.run(client.close())


class TestLookerOAuthPrmRoute:
    @pytest.mark.asyncio
    async def test_prm_advertises_looker_as_authorization_server(self):
        """The looker_oauth-mode PRM lists the Looker base URL as the
        authorization server so the client runs a Looker PKCE flow."""
        from starlette.applications import Starlette
        from starlette.testclient import TestClient

        mcp, client = create_server(_looker_oauth_config())
        try:
            app = mcp.http_app()
            assert isinstance(app, Starlette)
            with TestClient(app) as http:
                resp = http.get(PRM_PATH)
                assert resp.status_code == 200
                doc = resp.json()
                # Looker IS the authorization server in this posture.
                assert doc["authorization_servers"] == [BASE_URL]
                # Resource defaults to the Looker base URL when unset.
                assert doc["resource"] == RESOURCE_URI
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_prm_honors_explicit_resource_uri(self):
        from starlette.applications import Starlette
        from starlette.testclient import TestClient

        resource = "https://looker-mcp.example.com/mcp"
        mcp, client = create_server(_looker_oauth_config(mcp_resource_uri=resource))
        try:
            app = mcp.http_app()
            assert isinstance(app, Starlette)
            with TestClient(app) as http:
                # Suffix-variant path (RFC 9728 §3) for a path-qualified resource.
                resp = http.get(f"{PRM_PATH}/mcp")
                assert resp.status_code == 200
                doc = resp.json()
                assert doc["resource"] == resource
                assert doc["authorization_servers"] == [BASE_URL]
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_dev_mode_does_not_register_prm(self):
        dev = LookerConfig(_env_file=None, base_url=BASE_URL, mcp_mode=LookerMcpMode.DEV)  # type: ignore[call-arg]
        mcp, client = create_server(dev)
        try:
            app = mcp.http_app() if hasattr(mcp, "http_app") else None
            if app is not None:
                route_paths = {getattr(r, "path", None) for r in getattr(app, "routes", [])}
                assert PRM_PATH not in route_paths
        finally:
            await client.close()


class TestRunTransportGuard:
    """``run()`` fails fast when an HTTP-only auth posture is paired with a
    non-HTTP transport — otherwise the auth gate would be silently omitted."""

    @pytest.mark.asyncio
    async def test_looker_oauth_with_stdio_transport_aborts(self):
        from looker_mcp_server.main import run

        cfg = _looker_oauth_config(transport="stdio")
        with pytest.raises(SystemExit):
            await run(cfg, set())

    @pytest.mark.asyncio
    async def test_public_with_stdio_transport_aborts(self):
        from looker_mcp_server.main import run

        pub = LookerConfig(
            _env_file=None,  # type: ignore[call-arg]
            base_url=BASE_URL,
            transport="stdio",
            mcp_mode=LookerMcpMode.PUBLIC,
            mcp_jwks_uri="https://as.example.com/.well-known/jwks.json",
            mcp_issuer_url="https://as.example.com",
            mcp_resource_uri="https://mcp.test.example.com",
        )
        with pytest.raises(SystemExit):
            await run(pub, set())
