"""Unauthenticated tool-discovery endpoint at host-root ``/_introspect``.

Mounts an MCP JSON-RPC handler at ``/_introspect`` that serves the
**discovery slice** of the MCP protocol without requiring a user JWT, in
both protocol eras:

- Handshake era (2025-11-25 and earlier): ``initialize``,
  ``notifications/initialized`` and ``tools/list``.
- 2026-07-28 (no ``initialize``, no session): ``server/discover`` and
  ``tools/list``, each request naming its protocol revision in the
  ``MCP-Protocol-Version`` header and in ``params._meta``.

``tools/call`` is deliberately rejected; execution must go through the
authenticated ``/mcp`` route where token validation happens. FastMCP
serves both eras on ``/mcp`` by itself; this route is hand-rolled
JSON-RPC, so it shapes each reply for the era of the request through the
MCP SDK's per-revision result schemas.

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

Each ``POST /_introspect`` is logically standalone. On a handshake-era
request ``Mcp-Session-Id`` is echoed when supplied (per the MCP
convention) or freshly minted; a 2026-07-28 request gets none, because
that era has no sessions. Every notification is acknowledged with 202
and dropped: JSON-RPC notifications get no reply, and none changes
what discovery returns.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Final
from uuid import uuid4

import structlog
from mcp.types import (
    PROTOCOL_VERSION_META_KEY,
    SERVER_INFO_META_KEY,
    DiscoverResult,
    ListToolsResult,
    Result,
    ServerCapabilities,
    ToolsCapability,
)
from mcp.types.methods import serialize_server_result
from mcp.types.version import HANDSHAKE_PROTOCOL_VERSIONS, MODERN_PROTOCOL_VERSIONS

logger = structlog.get_logger()


# The MCP protocol revision this discovery endpoint answers ``initialize``
# with. Held as a module constant so the handshake reply and the shaping of
# handshake-era results agree on a single value.
MCP_PROTOCOL_VERSION: Final[str] = "2025-06-18"

# The HTTP header a client names its protocol revision in. Starlette's
# header mapping is case-insensitive.
_PROTOCOL_VERSION_HEADER: Final[str] = "mcp-protocol-version"

# Advertised by both ``initialize`` and ``server/discover``: the tool list
# is fixed for the life of the process.
_CAPABILITIES: Final = ServerCapabilities(tools=ToolsCapability(list_changed=False))


# Environment variable name for the optional shared-bearer guard. Kept
# as a module-level constant so tests and operator docs reference the
# same string.
INTROSPECT_BEARER_ENV: Final[str] = "LOOKER_MCP_INTROSPECT_BEARER"


def _jsonrpc_error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _jsonrpc_result(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _request_protocol_version(headers: Mapping[str, str], body: dict[str, Any]) -> str:
    """Return the protocol revision a discovery request is sent under.

    A 2026-07-28 request names its revision in the ``MCP-Protocol-Version``
    header and in ``params._meta``. A handshake-era request names it in the
    header once the handshake is done, and ``initialize`` names it in
    neither. Anything that does not name a modern revision is answered in
    the handshake era.
    """
    header = headers.get(_PROTOCOL_VERSION_HEADER)
    params = body.get("params")
    meta = params.get("_meta") if isinstance(params, dict) else None
    envelope = meta.get(PROTOCOL_VERSION_META_KEY) if isinstance(meta, dict) else None
    for candidate in (header, envelope):
        if isinstance(candidate, str) and candidate in MODERN_PROTOCOL_VERSIONS:
            return candidate
    if isinstance(header, str) and header in HANDSHAKE_PROTOCOL_VERSIONS:
        return header
    return MCP_PROTOCOL_VERSION


def _shape_result(
    method: str, version: str, result: Result, server_info: dict[str, str]
) -> dict[str, Any]:
    """Dump ``result`` as the wire JSON ``method`` returns in revision ``version``.

    SDK models name their fields in snake_case, so the dump uses the wire
    aliases (``inputSchema``, ``_meta``). The SDK's per-revision schema then
    keeps exactly the fields that revision defines: ``resultType``, ``ttlMs``
    and ``cacheScope`` appear only in a 2026-07-28 reply. A 2026-07-28 reply
    also identifies the server in ``_meta``, as ``/mcp`` does.
    """
    dumped = result.model_dump(by_alias=True, mode="json", exclude_none=True)
    if version in MODERN_PROTOCOL_VERSIONS:
        dumped["_meta"] = {**(dumped.get("_meta") or {}), SERVER_INFO_META_KEY: server_info}
    return serialize_server_result(method, version, dumped)


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
        FastMCP >=4.1.
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
        protocol_version = _request_protocol_version(request.headers, body)
        modern = protocol_version in MODERN_PROTOCOL_VERSIONS

        # On a handshake-era request ``Mcp-Session-Id`` is echoed when
        # supplied (case-insensitive via Starlette's header dict) or freshly
        # minted. The endpoint tracks no cross-request session state — each
        # call is logically standalone, and a discovery loop opens a fresh
        # "session" per registration cycle. The 2026-07-28 era has no
        # sessions, so its replies carry no session id.
        response_headers: dict[str, str] = {}
        if not modern:
            incoming_session = request.headers.get("mcp-session-id")
            response_headers["mcp-session-id"] = incoming_session or str(uuid4())

        if isinstance(method, str) and method.startswith("notifications/"):
            # JSON-RPC notification — no response body. ``202`` mirrors
            # what ``/mcp`` returns for a notification in either era, so a
            # generic MCP client sees identical behavior across the two
            # endpoints. Discovery acts on none of them (``initialized``
            # included), so each is acknowledged and dropped rather than
            # answered with an error the client has no request to match.
            return Response(status_code=202, headers=response_headers)

        if method == "initialize":
            result = {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "serverInfo": server_info,
                "capabilities": _CAPABILITIES.model_dump(
                    by_alias=True, mode="json", exclude_none=True
                ),
            }
            return JSONResponse(
                _jsonrpc_result(req_id, result),
                headers=response_headers,
            )

        if method == "server/discover":
            # The 2026-07-28 counterpart of ``initialize``: the revisions the
            # endpoint speaks without a handshake, and its capabilities.
            # Clients send it first when they auto-negotiate the era.
            discover = DiscoverResult(
                supported_versions=list(MODERN_PROTOCOL_VERSIONS),
                capabilities=_CAPABILITIES,
            )
            version = protocol_version if modern else MODERN_PROTOCOL_VERSIONS[-1]
            return JSONResponse(
                _jsonrpc_result(
                    req_id, _shape_result("server/discover", version, discover, server_info)
                ),
                headers=response_headers,
            )

        if method == "tools/list":
            try:
                tools = await mcp.list_tools()
                listing = ListToolsResult(tools=[t.to_mcp_tool() for t in tools])
                payload = _shape_result("tools/list", protocol_version, listing, server_info)
            except Exception:
                logger.exception("introspect.tools_list_failed")
                return JSONResponse(
                    _jsonrpc_error(req_id, -32603, "Internal error"),
                    status_code=500,
                    headers=response_headers,
                )
            return JSONResponse(
                _jsonrpc_result(req_id, payload),
                headers=response_headers,
            )

        # Every other method — ``tools/call``, ``resources/*``,
        # ``prompts/*``, etc. — is rejected. Discovery is read-only;
        # execution must traverse the authenticated ``/mcp`` route
        # where the user JWT validates and per-tool scope is enforced.
        # The 2026-07-28 HTTP binding maps method-not-found to 404; the
        # handshake era keeps this endpoint's established 405.
        return JSONResponse(
            _jsonrpc_error(req_id, -32601, "Method not found"),
            status_code=404 if modern else 405,
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
