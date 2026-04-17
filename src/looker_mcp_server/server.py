"""Server factory — creates the FastMCP server with configured tool groups."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

import structlog
from fastmcp import FastMCP

from .client import LookerClient
from .config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig, LookerMcpMode
from .identity import (
    ApiKeyIdentityProvider,
    DualModeIdentityProvider,
    IdentityProvider,
)

logger = structlog.get_logger()

#: Base well-known path for Protected Resource Metadata (RFC 9728 §3).
#: For an origin-only resource identifier (``https://host`` or
#: ``https://host/``), this IS the full PRM path. For a path-qualified
#: resource identifier (``https://host/mcp``), RFC 9728 §3 requires the
#: resource's path to be appended as a suffix — see
#: :func:`_well_known_prm_path`.
PRM_PATH = "/.well-known/oauth-protected-resource"


def _well_known_prm_path(resource_uri: str) -> str:
    """Return the PRM mount path per RFC 9728 §3.

    RFC 9728 §3 ("Protected Resource Metadata URL Construction")
    builds the well-known URL by inserting
    ``/.well-known/oauth-protected-resource`` between the authority and
    the path component of the resource identifier. Concretely:

    - ``https://host``          → ``/.well-known/oauth-protected-resource``
    - ``https://host/``         → ``/.well-known/oauth-protected-resource``
    - ``https://host/mcp``      → ``/.well-known/oauth-protected-resource/mcp``
    - ``https://host/a/b``      → ``/.well-known/oauth-protected-resource/a/b``

    The returned value doubles as the Starlette mount path (FastMCP
    registers ``custom_route`` at the app root) and as the path
    component of the advertised ``resource_metadata=`` URL, so the
    challenge hint and the served route agree by construction.
    """
    suffix = urlsplit(resource_uri).path.rstrip("/")
    return f"{PRM_PATH}{suffix}" if suffix else PRM_PATH


def _well_known_prm_url(resource_uri: str) -> str:
    """Return the full RFC 9728 §3 PRM URL for a resource identifier.

    Used as the ``resource_metadata=`` value in 401 ``WWW-Authenticate``
    challenges and as the canonical document-serving URL the server
    itself mounts the route at. Must agree with
    :func:`_well_known_prm_path` — do not derive the challenge hint and
    the route path independently.
    """
    parts = urlsplit(resource_uri)
    return urlunsplit((parts.scheme, parts.netloc, _well_known_prm_path(resource_uri), "", ""))


def create_server(
    config: LookerConfig,
    *,
    identity_provider: IdentityProvider | None = None,
    enabled_groups: set[str] | None = None,
    auth: Any = None,
) -> tuple[FastMCP, LookerClient]:
    """Create a configured Looker MCP server.

    Parameters
    ----------
    config:
        Server configuration (from env vars or explicit construction).
    identity_provider:
        Pluggable identity resolution.  Defaults to ``DualModeIdentityProvider``
        (uses sudo on self-hosted, OAuth on GC core) when ``sudo_as_user`` is
        enabled, otherwise ``ApiKeyIdentityProvider``.
    enabled_groups:
        Set of tool group names to enable.  Defaults to ``DEFAULT_GROUPS``.
    auth:
        FastMCP-compatible auth callback (e.g. bearer token verifier).

    Returns
    -------
    tuple[FastMCP, LookerClient]
        The configured server and the client (caller manages client lifecycle).
    """
    # ── Identity provider ────────────────────────────────────────────
    if identity_provider is None:
        if config.sudo_as_user and config.client_id and config.client_secret:
            provider = DualModeIdentityProvider(
                client_id=config.client_id,
                client_secret=config.client_secret,
                deployment_type=config.deployment_type,
                user_email_header=config.user_email_header,
                user_token_header=config.user_token_header,
            )
            identity_provider = provider
        else:
            identity_provider = ApiKeyIdentityProvider(
                config.client_id,
                config.client_secret,
            )

    client = LookerClient(config, identity_provider)

    # Wire the user-lookup function for sudo after the client is ready.
    if isinstance(identity_provider, DualModeIdentityProvider):
        identity_provider.set_user_lookup(client.lookup_user_by_email)

    # ── FastMCP server ───────────────────────────────────────────────
    mcp = FastMCP(
        name="looker-mcp-server",
        auth=auth,
    )

    # ── Register tool groups ─────────────────────────────────────────
    groups = enabled_groups or DEFAULT_GROUPS

    from .tools.admin import register_admin_tools
    from .tools.audit import register_audit_tools
    from .tools.board import register_board_tools
    from .tools.connection import register_connection_tools
    from .tools.content import register_content_tools
    from .tools.credentials import register_credentials_tools
    from .tools.explore import register_explore_tools
    from .tools.folder import register_folder_tools
    from .tools.git import register_git_tools
    from .tools.health import register_health_tools
    from .tools.modeling import register_modeling_tools
    from .tools.query import register_query_tools
    from .tools.schema import register_schema_tools
    from .tools.user_attributes import register_user_attribute_tools
    from .tools.workflows import register_workflow_tools

    _group_registry: dict[str, Any] = {
        "explore": register_explore_tools,
        "query": register_query_tools,
        "schema": register_schema_tools,
        "content": register_content_tools,
        "board": register_board_tools,
        "folder": register_folder_tools,
        "modeling": register_modeling_tools,
        "git": register_git_tools,
        "admin": register_admin_tools,
        "connection": register_connection_tools,
        "user_attributes": register_user_attribute_tools,
        "credentials": register_credentials_tools,
        "audit": register_audit_tools,
        "workflows": register_workflow_tools,
        "health": register_health_tools,
    }

    registered = []
    for name, register_fn in _group_registry.items():
        if name in groups:
            register_fn(mcp, client)
            registered.append(name)

    logger.info("looker.server.created", groups=registered)

    # ── OIDC public-mode PRM route ──────────────────────────────────
    # In ``LOOKER_MCP_MODE=public`` we serve the RFC 9728 Protected
    # Resource Metadata document so MCP clients can auto-discover the
    # authorization server after a 401. The token gate itself lives in
    # ``PublicModeAuthMiddleware`` (wired into the HTTP transport by
    # ``main.py`` via :func:`build_public_mode_middleware`); these
    # routes are deliberately bypassed by that middleware's
    # ``/.well-known/`` allowlist so discovery stays anonymous.
    #
    # RFC 9728 §3 constructs the PRM URL by inserting
    # ``/.well-known/oauth-protected-resource`` between the authority
    # and the path component of the resource identifier. For an
    # origin-only resource URI the canonical path is ``PRM_PATH``; for
    # a path-qualified resource URI (e.g. ``https://host/mcp``) it is
    # ``PRM_PATH + "/mcp"``. We register BOTH when they differ:
    #
    # - The suffix-variant path is the spec-canonical location that the
    #   ``WWW-Authenticate: resource_metadata=`` hint points at.
    # - The root ``PRM_PATH`` stays available as a defensive fallback
    #   for clients that probe the origin well-known location before
    #   following the challenge hint.
    if config.mcp_mode == LookerMcpMode.PUBLIC:
        from .oidc import build_prm_document

        def _prm_response() -> Any:
            from starlette.responses import JSONResponse

            doc = build_prm_document(
                resource_uri=config.mcp_resource_uri,
                authorization_server_issuer_url=config.mcp_issuer_url,
            )
            return JSONResponse(
                doc,
                headers={
                    # PRM rarely changes and is safe to cache by intermediaries.
                    # One hour is a reasonable default — matches the JWKS TTL.
                    "Cache-Control": "public, max-age=3600",
                },
            )

        @mcp.custom_route(PRM_PATH, methods=["GET"])
        async def prm_root(request: Any) -> Any:
            return _prm_response()

        suffix_path = _well_known_prm_path(config.mcp_resource_uri)
        if suffix_path != PRM_PATH:

            @mcp.custom_route(suffix_path, methods=["GET"])
            async def prm_suffix(request: Any) -> Any:
                return _prm_response()

    # ── Health-check routes ──────────────────────────────────────────
    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request: Any) -> Any:
        from starlette.responses import JSONResponse

        return JSONResponse({"status": "healthy", "server": "looker-mcp-server"})

    @mcp.custom_route("/readyz", methods=["GET"])
    async def readyz(request: Any) -> Any:
        from starlette.responses import JSONResponse

        if not config.base_url:
            return JSONResponse(
                {"status": "not_ready", "reason": "LOOKER_BASE_URL not configured"},
                status_code=503,
            )
        if not config.client_id or not config.client_secret:
            return JSONResponse(
                {"status": "not_ready", "reason": "LOOKER_CLIENT_ID/SECRET not configured"},
                status_code=503,
            )

        ok = await client.check_connectivity()
        if not ok:
            return JSONResponse(
                {"status": "not_ready", "reason": "Cannot connect to Looker"},
                status_code=503,
            )
        return JSONResponse({"status": "ready"})

    return mcp, client


def parse_groups(groups_str: str) -> set[str]:
    """Parse a comma-separated group string (or ``all``) into a set."""
    if groups_str.strip().lower() == "all":
        return set(ALL_GROUPS)
    parsed = {g.strip() for g in groups_str.split(",") if g.strip()}
    unknown = parsed - ALL_GROUPS
    if unknown:
        logger.warning("looker.groups.unknown", unknown=sorted(unknown), valid=sorted(ALL_GROUPS))
    return parsed & ALL_GROUPS


def build_public_mode_middleware(config: LookerConfig) -> Any | None:
    """Build the OAuth 2.1 resource-server middleware for HTTP transport.

    Returns a :class:`starlette.middleware.Middleware` wrapper ready to
    hand to FastMCP's ``run_async(middleware=[...])`` kwarg when
    ``config.mcp_mode == LookerMcpMode.PUBLIC``, or ``None`` in dev mode
    (so the HTTP transport stays permissive for local iteration).

    The middleware enforces:

    - 400 on URL-query bearer tokens (``?access_token=`` /
      ``?authorization=``) per OAuth 2.1 §5.1.1 — URL-bound tokens leak
      into referrer + proxy logs regardless of path.
    - 401 (+ ``WWW-Authenticate: Bearer realm="..." resource_metadata=
      "..."``) on missing / malformed / invalid tokens.
    - Pass-through on ``/.well-known/*``, ``/healthz``, ``/readyz`` so
      the PRM + health probes remain anonymous.

    This is deliberately a pure helper (not a side effect of
    ``create_server``) so ``main.py`` can thread the returned value
    into the starlette middleware chain alongside
    :class:`HeaderCaptureMiddleware`. Composition order matters — the
    auth gate must run FIRST (outermost) so unauthenticated requests
    never reach the header-capture layer or the tool handlers.
    """
    if config.mcp_mode != LookerMcpMode.PUBLIC:
        return None

    from starlette.middleware import Middleware

    from .oidc import JWKSCache, OAuth21ResourceServer, PublicModeAuthMiddleware

    jwks = JWKSCache(config.mcp_jwks_uri)
    resource_server = OAuth21ResourceServer(
        jwks,
        issuer=config.mcp_issuer_url,
        audience=config.mcp_resource_uri,
    )
    # Point the challenge hint at the spec-canonical PRM URL (RFC 9728
    # §3) — matches where :func:`create_server` mounts the suffix-variant
    # route when the resource identifier has a path, and reduces to the
    # origin-rooted ``PRM_PATH`` otherwise.
    prm_url = _well_known_prm_url(config.mcp_resource_uri)

    return Middleware(
        PublicModeAuthMiddleware,
        resource_server=resource_server,
        realm=config.mcp_resource_uri,
        prm_url=prm_url,
    )
