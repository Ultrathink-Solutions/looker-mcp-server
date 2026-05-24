"""Tests for the unauthenticated ``/_introspect`` discovery endpoint.

The endpoint serves the MCP discovery slice (``initialize`` +
``notifications/initialized`` + ``tools/list``) so a gateway aggregator
can populate its tool catalog without holding service-account
credentials at the backend. ``tools/call`` is deliberately rejected
here — execution must traverse the authenticated ``/mcp`` route.

Coverage groups:

- ``TestIntrospectHandshake`` — JSON-RPC ``initialize`` /
  ``notifications/initialized`` / ``tools/list`` round-trips with the
  expected response shapes and ``Mcp-Session-Id`` echo behavior.
- ``TestIntrospectMethodGuard`` — non-discovery methods (``tools/call``
  etc.) return JSON-RPC ``-32601`` with HTTP 405.
- ``TestIntrospectMalformedInput`` — parse errors return ``-32700``,
  non-object bodies return ``-32600``.
- ``TestIntrospectTransportSurface`` — ``GET`` returns 204 (empty
  server-push stream) and ``DELETE`` returns 200 (session teardown
  ack). Without these, a well-behaved MCP client logs 405 on every
  discovery cycle.
- ``TestIntrospectBearerGuard`` — when
  ``LOOKER_MCP_INTROSPECT_BEARER`` is set, unauthenticated requests
  401; the configured bearer succeeds.
- ``TestIntrospectMiddlewareBypass`` — the ``LOOKER_MCP_MODE=public``
  OAuth 2.1 middleware lets ``/_introspect`` through without a JWT.
- ``TestIntrospectFactoryWiring`` — ``create_server`` only mounts
  the route on HTTP transport; ``stdio`` deployments don't get it.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.testclient import TestClient

from looker_mcp_server.config import LookerConfig, LookerMcpMode
from looker_mcp_server.introspect import (
    INTROSPECT_BEARER_ENV,
    MCP_PROTOCOL_VERSION,
    register_introspect_endpoint,
)
from looker_mcp_server.server import create_server


def _build_minimal_server(server_name: str = "test-server") -> FastMCP:
    """Construct a FastMCP server with two tools and the introspect
    route mounted, without going through the looker factory. Keeps
    these tests independent of the rest of the looker codebase.
    """
    mcp = FastMCP(server_name)

    @mcp.tool()
    def add(x: int, y: int) -> int:
        """Add two integers."""
        return x + y

    @mcp.tool()
    def echo(message: str) -> str:
        """Echo a message back unchanged."""
        return message

    register_introspect_endpoint(mcp, server_name=server_name, server_version="9.9.9")
    return mcp


def _http_app(mcp: FastMCP) -> Starlette:
    app = mcp.http_app()
    assert isinstance(app, Starlette)
    return app


class TestIntrospectHandshake:
    def test_initialize_returns_protocol_and_server_info(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == 1
        assert body["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        assert body["result"]["serverInfo"] == {"name": "test-server", "version": "9.9.9"}
        assert body["result"]["capabilities"]["tools"] == {"listChanged": False}
        # Session id is always present on a discovery handshake.
        assert resp.headers.get("mcp-session-id")

    def test_initialize_echoes_supplied_session_id(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 2, "method": "initialize"},
                headers={"mcp-session-id": "client-supplied-id"},
            )
        assert resp.headers["mcp-session-id"] == "client-supplied-id"

    def test_notifications_initialized_acks_with_202(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            )
        assert resp.status_code == 202
        assert resp.headers.get("mcp-session-id")

    def test_tools_list_returns_registered_tools(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
            )
        assert resp.status_code == 200
        body = resp.json()
        tools = {t["name"]: t for t in body["result"]["tools"]}
        assert {"add", "echo"} <= set(tools)
        # Tool wire-format must carry a non-empty description and an
        # ``inputSchema`` — the two fields the aggregator needs to
        # route calls and prompt model tool selection.
        assert tools["add"]["description"]
        assert "inputSchema" in tools["add"]


class TestIntrospectMethodGuard:
    def test_tools_call_is_rejected_with_405(self):
        """Execution must go through authenticated ``/mcp``. Discovery
        is read-only."""
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {"name": "add", "arguments": {"x": 1, "y": 2}},
                },
            )
        assert resp.status_code == 405
        body = resp.json()
        assert body["error"]["code"] == -32601

    @pytest.mark.parametrize(
        "method",
        ["resources/list", "prompts/list", "ping", "totally/made/up"],
    )
    def test_unknown_methods_are_rejected_with_405(self, method: str):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 99, "method": method},
            )
        assert resp.status_code == 405
        assert resp.json()["error"]["code"] == -32601


class TestIntrospectMalformedInput:
    def test_parse_error_returns_minus_32700(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                content=b"not-valid-json",
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"]["code"] == -32700
        # JSON-RPC §5.1 — id MUST be ``null`` when not extractable.
        assert body["id"] is None

    @pytest.mark.parametrize("payload", [[], "string", 42, None])
    def test_non_object_body_returns_minus_32600(self, payload: Any):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.post(
                "/_introspect",
                content=json.dumps(payload).encode(),
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"]["code"] == -32600


class TestIntrospectTransportSurface:
    """The MCP Streamable HTTP transport opens a GET on connect and
    sends a DELETE on close. Without handlers, a well-behaved client
    logs 405 on every discovery cycle. The discovery endpoint has no
    state to manage either way, so these are minimal stubs.
    """

    def test_get_returns_204(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.get("/_introspect")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_delete_returns_200(self):
        with TestClient(_http_app(_build_minimal_server())) as http:
            resp = http.delete("/_introspect")
        assert resp.status_code == 200


class TestIntrospectBearerGuard:
    """When :data:`INTROSPECT_BEARER_ENV` is unset (the module default)
    the endpoint is open and the operator implicitly relies on network
    isolation. Setting the variable layers an application-layer guard:
    requests must carry a matching ``Authorization: Bearer ...`` header.
    """

    @pytest.fixture
    def app_with_bearer(self, monkeypatch):
        monkeypatch.setenv(INTROSPECT_BEARER_ENV, "secret-token")
        return _http_app(_build_minimal_server())

    def test_request_without_bearer_returns_401(self, app_with_bearer):
        with TestClient(app_with_bearer) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
            )
        assert resp.status_code == 401
        assert resp.headers.get("www-authenticate", "").lower().startswith("bearer")

    def test_request_with_wrong_bearer_returns_401(self, app_with_bearer):
        with TestClient(app_with_bearer) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                headers={"authorization": "Bearer wrong-token"},
            )
        assert resp.status_code == 401

    def test_request_with_correct_bearer_succeeds(self, app_with_bearer):
        with TestClient(app_with_bearer) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                headers={"authorization": "Bearer secret-token"},
            )
        assert resp.status_code == 200

    def test_request_with_non_bearer_scheme_returns_401(self, app_with_bearer):
        with TestClient(app_with_bearer) as http:
            resp = http.post(
                "/_introspect",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                headers={"authorization": "Basic some-base64-thing"},
            )
        assert resp.status_code == 401

    def test_get_is_also_bearer_guarded(self, app_with_bearer):
        """The GET handler is part of the discovery surface and must
        not leak existence (or its 204 ack) when the bearer guard is
        configured. Without this an unauth caller could enumerate the
        path via HEAD-equivalent probing."""
        with TestClient(app_with_bearer) as http:
            unauth = http.get("/_introspect")
            authed = http.get("/_introspect", headers={"authorization": "Bearer secret-token"})
        assert unauth.status_code == 401
        assert authed.status_code == 204

    def test_delete_is_also_bearer_guarded(self, app_with_bearer):
        with TestClient(app_with_bearer) as http:
            unauth = http.delete("/_introspect")
            authed = http.delete("/_introspect", headers={"authorization": "Bearer secret-token"})
        assert unauth.status_code == 401
        assert authed.status_code == 200


class TestIntrospectMiddlewareBypass:
    """The OAuth 2.1 resource-server middleware in
    :mod:`looker_mcp_server.oidc.middleware` exempts ``/_introspect``
    from its token check so the gateway-aggregator discovery contract
    survives in ``LOOKER_MCP_MODE=public`` deployments. The endpoint
    enforces its own optional bearer instead.
    """

    def test_public_mode_lets_introspect_through(self):
        from looker_mcp_server.oidc.middleware import _BYPASS_PREFIXES

        # The constant is the authoritative bypass list — guarding
        # against a future edit that drops the introspect entry.
        assert "/_introspect" in _BYPASS_PREFIXES

    @pytest.mark.asyncio
    async def test_public_mode_drives_introspect_without_auth(self):
        """End-to-end: install ``PublicModeAuthMiddleware`` and confirm
        an unauthenticated ``POST /_introspect initialize`` lands on
        the handler (responds 200) rather than being short-circuited
        by the middleware's 401.
        """
        from looker_mcp_server.config import LookerConfig, LookerMcpMode
        from looker_mcp_server.server import (
            build_public_mode_middleware,
            create_server,
        )

        config = LookerConfig(
            base_url="https://test.looker.com",
            client_id="test-id",
            client_secret="test-secret",
            sudo_as_user=False,
            transport="streamable-http",
            mcp_mode=LookerMcpMode.PUBLIC,
            mcp_jwks_uri="https://as.example.com/.well-known/jwks.json",
            mcp_issuer_url="https://as.example.com",
            mcp_resource_uri="https://looker.example.com/mcp",
            _env_file=None,  # type: ignore[call-arg]
        )
        mcp, client = create_server(config)
        try:
            mw = build_public_mode_middleware(config)
            assert mw is not None
            app = mcp.http_app(middleware=[mw])
            assert isinstance(app, Starlette)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
                resp = await http.post(
                    "/_introspect",
                    json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                )
            assert resp.status_code == 200
            assert resp.json()["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
        finally:
            await client.close()


class TestIntrospectFactoryWiring:
    """``create_server`` mounts the introspect route only for HTTP
    transport — stdio servers don't sit behind a gateway and don't
    need it.
    """

    @pytest.mark.asyncio
    async def test_http_transport_mounts_introspect(self):
        config = LookerConfig(
            base_url="https://test.looker.com",
            client_id="id",
            client_secret="secret",
            sudo_as_user=False,
            transport="streamable-http",
            _env_file=None,  # type: ignore[call-arg]
        )
        mcp, client = create_server(config)
        try:
            app = mcp.http_app()
            assert isinstance(app, Starlette)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
                resp = await http.post(
                    "/_introspect",
                    json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                )
            assert resp.status_code == 200
            assert resp.json()["result"]["serverInfo"]["name"] == "looker-mcp-server"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_stdio_transport_omits_introspect(self):
        config = LookerConfig(
            base_url="https://test.looker.com",
            client_id="id",
            client_secret="secret",
            sudo_as_user=False,
            transport="stdio",
            _env_file=None,  # type: ignore[call-arg]
        )
        mcp, client = create_server(config)
        try:
            app = mcp.http_app()
            assert isinstance(app, Starlette)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
                resp = await http.post(
                    "/_introspect",
                    json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                )
            # No route registered, so the FastMCP app surfaces a 404
            # (Starlette default for unknown paths).
            assert resp.status_code == 404
        finally:
            await client.close()


# Suppress unused-import warnings when an import is legitimately
# load-bearing for a fixture or class attribute but not directly
# referenced in test bodies.
_ = LookerConfig, LookerMcpMode
