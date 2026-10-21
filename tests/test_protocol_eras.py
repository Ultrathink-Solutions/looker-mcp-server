"""Dual-era serving: 2025-11-25 handshake clients and 2026-07-28 sessionless clients.

FastMCP 4 (MCP SDK 2) serves both protocol eras from the one ``/mcp``
endpoint. A handshake-era client opens a session with ``initialize`` and sends
``Mcp-Session-Id`` on every later request. A 2026-07-28 client never sends
``initialize``: each POST is self-contained and carries its protocol version
in ``params._meta`` and the ``MCP-Protocol-Version`` header.

Nothing in this server may therefore depend on ``initialize`` or on session
state for authentication or identity. These tests drive the production HTTP
stack (:func:`looker_mcp_server.main.build_http_middleware` and
:func:`looker_mcp_server.main.build_server_auth`, the same wiring ``run()``
uses) with a real MCP client in each era and prove that:

- each era negotiates its own protocol version against the same app;
- every auth posture (``looker_oauth`` opaque-token introspection, ``public``
  JWT validation, and the static ``LOOKER_MCP_AUTH_TOKEN`` bearer) rejects an
  unauthenticated or invalid request identically in both eras;
- the Looker identity a tool runs as comes from the credential on *that* HTTP
  request, so switching or revoking the credential mid-connection takes
  effect on the next request in both eras;
- traffic that is not a routable tool request (notifications, unknown
  methods, malformed ``tools/call`` params) passes through the auth gate and
  the header-capture layer without breaking them, and is challenged like any
  other request when unauthenticated.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import jwt as pyjwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastmcp import Client
from mcp.types import TextContent
from starlette.applications import Starlette

from looker_mcp_server.client import LookerClient
from looker_mcp_server.config import LookerConfig, LookerMcpMode
from looker_mcp_server.main import build_http_middleware, build_server_auth
from looker_mcp_server.server import PRM_PATH, create_server
from tests._mcp_http import (
    ERAS,
    LEGACY,
    MODERN,
    NEGOTIATED_VERSION,
    SwitchableBearer,
    era_message,
    http_client,
    jsonrpc_reply,
    open_session,
    recorded_methods,
    serving,
)

LOOKER_URL = "https://test.looker.com"
API_URL = f"{LOOKER_URL}/api/4.0"
RESOURCE_URI = "https://mcp.test.example.com/mcp"
PRM_URL = f"https://mcp.test.example.com{PRM_PATH}/mcp"
ISSUER = "https://as.example.com"
JWKS_URI = f"{ISSUER}/.well-known/jwks.json"
STATIC_TOKEN = "static-test-token"

#: Opaque Looker tokens the mocked ``GET /user`` accepts, and who they belong to.
LOOKER_USERS = {
    "token-a": {"id": "101", "email": "analyst-a@example.com"},
    "token-b": {"id": "202", "email": "analyst-b@example.com"},
}


# ── Configs and app wiring ───────────────────────────────────────────


def _looker_oauth_config() -> LookerConfig:
    return LookerConfig(
        _env_file=None,  # type: ignore[call-arg]
        base_url=LOOKER_URL,
        transport="streamable-http",
        mcp_mode=LookerMcpMode.LOOKER_OAUTH,
        mcp_resource_uri=RESOURCE_URI,
    )


def _public_config() -> LookerConfig:
    return LookerConfig(
        _env_file=None,  # type: ignore[call-arg]
        base_url=LOOKER_URL,
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        transport="streamable-http",
        mcp_mode=LookerMcpMode.PUBLIC,
        mcp_jwks_uri=JWKS_URI,
        mcp_issuer_url=ISSUER,
        mcp_resource_uri=RESOURCE_URI,
    )


def _dev_config(**overrides: Any) -> LookerConfig:
    return LookerConfig(
        _env_file=None,  # type: ignore[call-arg]
        base_url=LOOKER_URL,
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        transport="streamable-http",
        **overrides,
    )


def _static_bearer_config() -> LookerConfig:
    # The static bearer is deprecated in favour of ``public`` mode but still
    # served; its config warns on construction.
    with pytest.warns(DeprecationWarning, match="LOOKER_MCP_AUTH_TOKEN"):
        return _dev_config(mcp_auth_token=STATIC_TOKEN)


def _app(config: LookerConfig) -> tuple[Starlette, LookerClient]:
    """Build the HTTP app exactly as ``main.run`` composes it for ``config``."""
    mcp, looker = create_server(config, enabled_groups={"identity"}, auth=build_server_auth(config))
    return mcp.http_app(middleware=build_http_middleware(config)), looker


def _looker_user_by_token(request: httpx.Request) -> httpx.Response:
    """Mock Looker ``GET /user``: answer for the opaque token the call presents."""
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    user = LOOKER_USERS.get(token)
    if scheme.lower() != "token" or user is None:
        return httpx.Response(401, json={"message": "Requires authentication."})
    return httpx.Response(200, json=user)


def _mock_api_key_session(user_id: str = "1") -> None:
    respx.post(f"{API_URL}/login").respond(200, json={"access_token": "service-session"})
    respx.delete(f"{API_URL}/logout").respond(204)
    respx.get(f"{API_URL}/user").respond(200, json={"id": user_id})


async def _whoami(client: Client) -> dict[str, Any]:
    result = await client.call_tool("whoami", {})
    content = result.content[0]
    assert isinstance(content, TextContent)
    return json.loads(content.text)


def _first_request(era: str) -> tuple[dict[str, str], dict[str, Any]]:
    """The first message a client in ``era`` sends to ``/mcp``."""
    if era == LEGACY:
        return era_message(era, "initialize")
    return era_message(era, "tools/call", {"name": "whoami", "arguments": {}})


def _assert_challenged(resp: Any) -> None:
    assert resp.status_code == 401, resp.text
    challenge = resp.headers.get("www-authenticate", "")
    assert challenge.lower().startswith("bearer")
    assert f'resource_metadata="{PRM_URL}"' in challenge


# ── Era negotiation ──────────────────────────────────────────────────


class TestEraNegotiation:
    """Both eras reach the same server, in process and over HTTP."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("era", ERAS)
    @respx.mock
    async def test_in_process_client_negotiates_each_era(self, era: str):
        _mock_api_key_session(user_id="7")
        mcp, looker = create_server(_dev_config(), enabled_groups={"identity"})
        try:
            async with Client(mcp, mode=era) as client:
                assert client.protocol_version == NEGOTIATED_VERSION[era]
                if era == LEGACY:
                    assert client.initialize_result is not None
                    assert client.initialize_result.server_info.name == "looker-mcp-server"
                else:
                    # A 2026-07-28 connection has no InitializeResult: a client
                    # pinned to the version adopts it without any handshake.
                    assert client.initialize_result is None
                assert "whoami" in {tool.name for tool in await client.list_tools()}
                assert (await _whoami(client))["id"] == "7"
        finally:
            await looker.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("era", ERAS)
    @respx.mock
    async def test_http_client_negotiates_each_era(self, era: str):
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        methods, hook = recorded_methods()
        try:
            async with serving(app):
                async with http_client(app, mode=era, auth="token-a", on_request=hook) as client:
                    assert client.protocol_version == NEGOTIATED_VERSION[era]
                    assert (await _whoami(client))["id"] == "101"
        finally:
            await looker.close()
        if era == LEGACY:
            assert methods[0] == "initialize"
        else:
            assert "initialize" not in methods

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_mode_client_selects_the_modern_era(self):
        """A FastMCP 4 client's default ``mode="auto"`` probes ``server/discover``
        and, because this server answers it, speaks 2026-07-28."""
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        methods, hook = recorded_methods()
        try:
            async with serving(app):
                async with http_client(app, mode="auto", auth="token-a", on_request=hook) as client:
                    assert client.protocol_version == MODERN
        finally:
            await looker.close()
        assert methods[0] == "server/discover"
        assert "initialize" not in methods


# ── looker_oauth posture: gate + per-request identity ────────────────


@pytest.mark.parametrize("era", ERAS)
class TestLookerOAuthAcrossEras:
    """Opaque Looker tokens are introspected per request, and the tool runs as
    the token's owner, in both eras."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_without_token_is_challenged(self, era: str):
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                headers, body = _first_request(era)
                resp = await raw.post("/mcp", headers=headers, json=body)
        finally:
            await looker.close()
        _assert_challenged(resp)

    @pytest.mark.asyncio
    @respx.mock
    async def test_token_looker_rejects_is_challenged(self, era: str):
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                headers, body = _first_request(era)
                resp = await raw.post(
                    "/mcp", headers={**headers, "authorization": "Bearer unknown"}, json=body
                )
        finally:
            await looker.close()
        _assert_challenged(resp)

    @pytest.mark.asyncio
    @respx.mock
    async def test_identity_follows_the_credential_on_each_request(self, era: str):
        """Switching the bearer between two calls on one client connection
        changes the Looker user the second call runs as. In the handshake era
        both calls share one session opened by a single ``initialize``, so the
        identity is not captured at ``initialize`` or held in session state."""
        user_route = respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        auth = SwitchableBearer("token-a")
        methods, hook = recorded_methods()
        try:
            async with serving(app):
                async with http_client(app, mode=era, auth=auth, on_request=hook) as client:
                    first = await _whoami(client)
                    auth.token = "token-b"
                    second = await _whoami(client)
        finally:
            await looker.close()

        assert first["id"] == "101"
        assert second["id"] == "202"
        assert methods.count("initialize") == (1 if era == LEGACY else 0)
        # The tool forwarded the caller's own token to Looker each time.
        forwarded = [call.request.headers["authorization"] for call in user_route.calls]
        assert "token token-a" in forwarded
        assert "token token-b" in forwarded

    @pytest.mark.asyncio
    @respx.mock
    async def test_credential_revoked_mid_connection_is_refused(self, era: str):
        """An established connection grants nothing: once the token stops
        verifying, the next request is challenged even though the session (in
        the handshake era) was opened with a valid one."""
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                valid = {"authorization": "Bearer token-a"}
                session = await open_session(raw, valid) if era == LEGACY else valid
                headers, body = era_message(
                    era, "tools/call", {"name": "whoami", "arguments": {}}, request_id=2
                )
                ok = await raw.post("/mcp", headers={**headers, **session}, json=body)
                revoked = await raw.post(
                    "/mcp",
                    headers={**headers, **session, "authorization": "Bearer revoked"},
                    json=body,
                )
                missing = {k: v for k, v in session.items() if k != "authorization"}
                anonymous = await raw.post("/mcp", headers={**headers, **missing}, json=body)
        finally:
            await looker.close()

        assert ok.status_code == 200, ok.text
        reply = jsonrpc_reply(ok)
        assert json.loads(reply["result"]["content"][0]["text"])["id"] == "101"
        _assert_challenged(revoked)
        _assert_challenged(anonymous)


# ── public posture: JWT gate ─────────────────────────────────────────


@pytest.fixture(scope="module")
def rsa_keypair() -> dict[str, Any]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    return {"private_pem": pem, "public_key": key.public_key()}


def _mock_jwks(public_key: Any) -> None:
    from jwt.algorithms import RSAAlgorithm

    jwk = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk.update({"kid": "k1", "use": "sig", "alg": "RS256"})
    respx.get(JWKS_URI).respond(200, json={"keys": [jwk]})


def _jwt(private_pem: str, *, ttl: int = 3600, audience: str = RESOURCE_URI) -> str:
    now = int(time.time())
    return pyjwt.encode(
        {"iss": ISSUER, "aud": audience, "sub": "user-1", "iat": now, "exp": now + ttl},
        private_pem,
        algorithm="RS256",
        headers={"kid": "k1"},
    )


@pytest.mark.parametrize("era", ERAS)
class TestPublicModeAcrossEras:
    @pytest.mark.asyncio
    @respx.mock
    async def test_valid_token_reaches_the_tool(self, era: str, rsa_keypair):
        _mock_jwks(rsa_keypair["public_key"])
        _mock_api_key_session(user_id="55")
        app, looker = _app(_public_config())
        try:
            async with serving(app):
                token = _jwt(rsa_keypair["private_pem"])
                async with http_client(app, mode=era, auth=token) as client:
                    assert client.protocol_version == NEGOTIATED_VERSION[era]
                    assert (await _whoami(client))["id"] == "55"
        finally:
            await looker.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("problem", ["missing", "expired", "wrong_audience"])
    @respx.mock
    async def test_missing_or_invalid_token_is_challenged(
        self, era: str, problem: str, rsa_keypair
    ):
        _mock_jwks(rsa_keypair["public_key"])
        app, looker = _app(_public_config())
        headers, body = _first_request(era)
        if problem == "expired":
            headers["authorization"] = f"Bearer {_jwt(rsa_keypair['private_pem'], ttl=-60)}"
        elif problem == "wrong_audience":
            token = _jwt(rsa_keypair["private_pem"], audience="https://other.example.com")
            headers["authorization"] = f"Bearer {token}"
        try:
            async with serving(app) as raw:
                resp = await raw.post("/mcp", headers=headers, json=body)
        finally:
            await looker.close()
        _assert_challenged(resp)


# ── static bearer (LOOKER_MCP_AUTH_TOKEN, FastMCP's own auth provider) ─


@pytest.mark.parametrize("era", ERAS)
class TestStaticBearerAcrossEras:
    @pytest.mark.asyncio
    @respx.mock
    async def test_configured_token_reaches_the_tool(self, era: str):
        _mock_api_key_session(user_id="9")
        app, looker = _app(_static_bearer_config())
        try:
            async with serving(app):
                async with http_client(app, mode=era, auth=STATIC_TOKEN) as client:
                    assert client.protocol_version == NEGOTIATED_VERSION[era]
                    assert (await _whoami(client))["id"] == "9"
        finally:
            await looker.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("authorization", [None, "Bearer wrong-token"])
    async def test_missing_or_wrong_token_is_rejected(self, era: str, authorization: str | None):
        app, looker = _app(_static_bearer_config())
        headers, body = _first_request(era)
        if authorization is not None:
            headers["authorization"] = authorization
        try:
            async with serving(app) as raw:
                resp = await raw.post("/mcp", headers=headers, json=body)
        finally:
            await looker.close()
        assert resp.status_code == 401, resp.text
        assert resp.headers.get("www-authenticate", "").lower().startswith("bearer")


# ── Non-routable traffic through the gate and header capture ─────────


#: Client-to-server notifications, including ones a server does not act on.
#: A 2026-07-28 server acknowledges and drops every notification POST.
NOTIFICATIONS = [
    ("notifications/cancelled", {"requestId": 999, "reason": "client gave up"}),
    ("notifications/progress", {"progressToken": "p-1", "progress": 0.5}),
    ("notifications/roots/list_changed", {}),
    ("notifications/not_a_real_notification", {}),
]

#: Requests no tool handler can route: an unknown method, and ``tools/call``
#: with params that fail validation.
UNROUTABLE_REQUESTS = [
    ("not/a/method", {}, -32601),
    ("tools/call", {"name": "whoami", "arguments": "not-an-object"}, -32602),
    ("tools/call", {"arguments": {}}, -32602),
]


@pytest.mark.parametrize("era", ERAS)
class TestNonRoutableTraffic:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("method", "params"), NOTIFICATIONS)
    @respx.mock
    async def test_notification_is_acknowledged_and_the_session_keeps_working(
        self, era: str, method: str, params: dict[str, Any]
    ):
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                auth = {"authorization": "Bearer token-a"}
                session = await open_session(raw, auth) if era == LEGACY else auth
                headers, body = era_message(era, method, params, request_id=None)
                ack = await raw.post("/mcp", headers={**headers, **session}, json=body)
                headers, body = era_message(
                    era, "tools/call", {"name": "whoami", "arguments": {}}, request_id=5
                )
                after = await raw.post("/mcp", headers={**headers, **session}, json=body)
        finally:
            await looker.close()

        assert ack.status_code == 202, ack.text
        assert after.status_code == 200, after.text
        assert json.loads(jsonrpc_reply(after)["result"]["content"][0]["text"])["id"] == "101"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("method", "params", "code"), UNROUTABLE_REQUESTS)
    @respx.mock
    async def test_unroutable_request_gets_a_jsonrpc_error_not_a_crash(
        self, era: str, method: str, params: dict[str, Any], code: int
    ):
        respx.get(f"{API_URL}/user").mock(side_effect=_looker_user_by_token)
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                auth = {"authorization": "Bearer token-a"}
                session = await open_session(raw, auth) if era == LEGACY else auth
                headers, body = era_message(era, method, params, request_id=7)
                resp = await raw.post("/mcp", headers={**headers, **session}, json=body)
        finally:
            await looker.close()

        assert resp.status_code < 500, resp.text
        reply = jsonrpc_reply(resp)
        assert reply["id"] == 7
        assert reply["error"]["code"] == code

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method", "params", "request_id"),
        [(method, params, None) for method, params in NOTIFICATIONS]
        + [(method, params, 7) for method, params, _ in UNROUTABLE_REQUESTS],
    )
    async def test_unauthenticated_traffic_is_challenged_whatever_the_message(
        self, era: str, method: str, params: dict[str, Any], request_id: int | None
    ):
        """The gate decides on the HTTP request alone, so a notification or a
        malformed call without a credential is challenged like any other."""
        app, looker = _app(_looker_oauth_config())
        try:
            async with serving(app) as raw:
                headers, body = era_message(era, method, params, request_id=request_id)
                resp = await raw.post("/mcp", headers=headers, json=body)
        finally:
            await looker.close()
        _assert_challenged(resp)
