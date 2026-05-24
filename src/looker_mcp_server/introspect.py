"""Unauthenticated tool-discovery endpoint at host-root ``/_introspect``.

Mounts an MCP JSON-RPC handler at ``/_introspect`` that serves the
**discovery slice** of the MCP protocol — ``initialize``,
``notifications/initialized``, and ``tools/list`` — without requiring
a user JWT. ``tools/call`` is deliberately rejected; execution must go
through the authenticated ``/mcp`` route where token validation happens.

Why a separate, unauthenticated route
-------------------------------------
A gateway aggregator that fronts multiple MCP servers needs to
populate its tool catalog at registration time, before any user JWT
exists. The aggregator has no bootstrap identity at each backend's
authorization server. ``/_introspect`` resolves the chicken-and-egg:
the aggregator can list a backend's tools without credentials, then
forward the *user's* JWT on every subsequent ``tools/call`` it
proxies through the backend's authenticated route.

This pattern is canonical in federated/aggregated query architectures
— Strawberry GraphQL Federation makes the same recommendation
verbatim:

    "If your federated service is reachable by untrusted clients, use
    authentication, authorization, or network controls to restrict
    access to federation fields and make sure entity resolvers enforce
    their own access checks."
    — https://strawberry.rocks/docs/guides/federation

The "or network controls" clause is load-bearing here. Apollo
Federation v1 has run gateway-aggregates-subgraph-schemas in
production since 2019. Envoy AI Gateway uses the same two-tier
pattern (service-tier credentials for transport, header forwarding
for user identity).

Trust model
-----------
The route is mounted at host root, NOT under ``/mcp`` where the auth
middleware lives. It is reachable without an ``Authorization``
header **by design**. Operators are expected to compose at least one
of the following defenses:

1. **Network controls** (recommended for cluster deployments): a
   network policy or ingress configuration that only routes
   ``/_introspect`` from the trusted aggregator's pod or subnet.
   External ingress should expose only ``/mcp/*`` and the
   well-known PRM document.

2. **Application-layer shared bearer** (recommended when network
   isolation is not available): set the ``LOOKER_MCP_INTROSPECT_BEARER``
   environment variable. When set, the endpoint requires
   ``Authorization: Bearer <value>`` and 401s otherwise. The
   aggregator is configured with the same token out-of-band.

3. **Both, for defense-in-depth.**

When ``LOOKER_MCP_INTROSPECT_BEARER`` is unset (the default) the
endpoint is unauthenticated and the operator is implicitly relying on
network-layer isolation. That default is appropriate for cluster
deployments where the network boundary is already drawn at the
ingress layer; it is a footgun on a public network and operators
should set the env var in that case.

Transport surface
-----------------
The MCP Streamable HTTP transport opens a GET stream on connect (for
server-initiated notifications) and sends a DELETE on session
teardown. The discovery endpoint does not implement server-push and
holds no cross-request session state, but it must accept GET and
DELETE so a well-behaved MCP client doesn't log 405 errors on every
discovery cycle:

- ``GET /_introspect``    → 204 No Content (no notifications stream).
- ``DELETE /_introspect`` → 200 OK (no session state to release).

Each ``POST /_introspect`` is logically standalone; ``Mcp-Session-Id``
is echoed when supplied (per the MCP convention) or freshly minted.
"""

from __future__ import annotations

import os
from typing import Any, Final
from uuid import uuid4

import structlog

logger = structlog.get_logger()


# The MCP protocol revision this discovery endpoint advertises. Held
# as a module constant so the handshake reply and any future references
# agree on a single value. This SHOULD track whatever revision the
# authenticated ``/mcp`` handler negotiates so a client sees consistent
# capability semantics regardless of which endpoint it speaks to.
MCP_PROTOCOL_VERSION: Final[str] = "2025-06-18"


# Environment variable name for the optional shared-bearer guard. Kept
# as a module-level constant so tests and operator docs reference the
# same string.
INTROSPECT_BEARER_ENV: Final[str] = "LOOKER_MCP_INTROSPECT_BEARER"


def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _jsonrpc_result(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _bearer_guard_passes(authorization_header: str | None, configured_token: str | None) -> bool:
    """Constant-time comparison of the request bearer against the
    configured token. Returns ``True`` when no token is configured
    (unauthenticated mode — operator relies on network isolation).
    """
    if not configured_token:
        return True
    if not authorization_header:
        return False
    scheme, _, presented = authorization_header.partition(" ")
    if scheme.lower() != "bearer" or not presented:
        return False
    # ``secrets.compare_digest`` is the appropriate primitive for
    # comparing a presented credential against a configured one — it
    # is timing-attack resistant and the right answer even when both
    # operands fit comfortably in cache.
    import secrets

    return secrets.compare_digest(presented.strip(), configured_token)


def register_introspect_endpoint(
    mcp: Any,
    *,
    server_name: str,
    server_version: str,
) -> None:
    """Mount the ``/_introspect`` route on the FastMCP server's ASGI app.

    Parameters
    ----------
    mcp:
        A ``FastMCP`` server instance. Must expose the
        ``custom_route`` decorator, the async ``list_tools`` coroutine,
        and per-tool ``to_mcp_tool`` rendering — public surfaces on
        FastMCP >=3.2.
    server_name:
        Identity returned in the ``initialize`` response's
        ``serverInfo.name`` field. Should match the FastMCP
        construction-time name so discovery clients see one consistent
        identity across ``/mcp`` and ``/_introspect``.
    server_version:
        ``serverInfo.version`` value. The caller passes the package
        version explicitly so this module avoids an ``importlib.metadata``
        round trip and the test surface stays self-contained.

    Trust composition is the caller's responsibility — see the module
    docstring for the operator-facing guidance. This function reads
    ``LOOKER_MCP_INTROSPECT_BEARER`` from the process environment at
    *registration* time, not per-request, so the configured token is
    pinned for the life of the server process.
    """
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response

    server_info = {"name": server_name, "version": server_version}
    configured_bearer = os.environ.get(INTROSPECT_BEARER_ENV) or None

    @mcp.custom_route("/_introspect", methods=["POST"])
    async def _introspect(request: Request) -> Response:
        authz = request.headers.get("authorization")
        if not _bearer_guard_passes(authz, configured_bearer):
            return JSONResponse(
                {"error": "unauthorized"},
                status_code=401,
                headers={"www-authenticate": 'Bearer realm="introspect"'},
            )

        try:
            body = await request.json()
        except Exception:
            # JSON-RPC 2.0 §5.1 — ``-32700 Parse error`` on
            # unparseable body. ``id`` is null per spec (we can't
            # extract it from the payload we couldn't parse).
            return JSONResponse(
                _jsonrpc_error(None, -32700, "Parse error"),
                status_code=400,
            )

        if not isinstance(body, dict):
            return JSONResponse(
                _jsonrpc_error(None, -32600, "Invalid Request"),
                status_code=400,
            )

        method = body.get("method", "")
        req_id = body.get("id")

        # ``Mcp-Session-Id`` is echoed when supplied (case-insensitive
        # via Starlette's header dict) or freshly minted. The endpoint
        # tracks no cross-request session state — each call is
        # logically standalone, and a discovery loop opens a fresh
        # "session" per registration cycle.
        incoming_session = request.headers.get("mcp-session-id")
        session_id = incoming_session or str(uuid4())
        response_headers = {"mcp-session-id": session_id}

        if method == "initialize":
            result = {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "serverInfo": server_info,
                "capabilities": {"tools": {"listChanged": False}},
            }
            return JSONResponse(
                _jsonrpc_result(req_id, result),
                headers=response_headers,
            )

        if method == "notifications/initialized":
            # JSON-RPC notification — no response body. ``202``
            # mirrors what FastMCP returns for the same notification
            # on ``/mcp`` so a generic MCP client sees identical
            # behavior across the two endpoints.
            return Response(status_code=202, headers=response_headers)

        if method == "tools/list":
            try:
                tools = await mcp.list_tools()
                tools_payload = [t.to_mcp_tool().model_dump(exclude_none=True) for t in tools]
            except Exception:
                logger.exception("introspect.tools_list_failed")
                return JSONResponse(
                    _jsonrpc_error(req_id, -32603, "Internal error"),
                    status_code=500,
                    headers=response_headers,
                )
            return JSONResponse(
                _jsonrpc_result(req_id, {"tools": tools_payload}),
                headers=response_headers,
            )

        # Every other method — ``tools/call``, ``resources/*``,
        # ``prompts/*``, etc. — is rejected. Discovery is read-only;
        # execution must traverse the authenticated ``/mcp`` route
        # where the user JWT validates and per-tool scope is enforced.
        return JSONResponse(
            _jsonrpc_error(req_id, -32601, "Method not found"),
            status_code=405,
            headers=response_headers,
        )

    @mcp.custom_route("/_introspect", methods=["GET"])
    async def _introspect_stream(request: Request) -> Response:
        """Server-push stream placeholder.

        The MCP Streamable HTTP client opens a GET on the same URL on
        connect to receive server-initiated notifications. The
        discovery endpoint has none, so 204 No Content is the correct
        empty response — the client sees the stream "complete" and
        moves on. Without this handler the client logs a 405 on every
        discovery cycle.
        """
        # The bearer guard applies to GET too — an attacker who can
        # reach the endpoint shouldn't be able to enumerate it via
        # GET even though no body is returned.
        authz = request.headers.get("authorization")
        if not _bearer_guard_passes(authz, configured_bearer):
            return JSONResponse(
                {"error": "unauthorized"},
                status_code=401,
                headers={"www-authenticate": 'Bearer realm="introspect"'},
            )
        return Response(status_code=204)

    @mcp.custom_route("/_introspect", methods=["DELETE"])
    async def _introspect_teardown(request: Request) -> Response:
        """Session teardown ack.

        MCP clients send DELETE on session close. The endpoint holds
        no session state to release, so 200 OK is the correct ack.
        Without this handler the client logs a 405 on every clean
        teardown.
        """
        authz = request.headers.get("authorization")
        if not _bearer_guard_passes(authz, configured_bearer):
            return JSONResponse(
                {"error": "unauthorized"},
                status_code=401,
                headers={"www-authenticate": 'Bearer realm="introspect"'},
            )
        return Response(status_code=200)

    logger.info(
        "introspect.endpoint_registered",
        server_name=server_name,
        bearer_guard=configured_bearer is not None,
    )
