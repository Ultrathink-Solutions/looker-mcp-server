"""Server factory — creates the FastMCP server with configured tool groups."""

from __future__ import annotations

from typing import Any

import structlog
from fastmcp import FastMCP

from .client import LookerClient
from .config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig
from .identity import (
    ApiKeyIdentityProvider,
    DualModeIdentityProvider,
    IdentityProvider,
)

logger = structlog.get_logger()


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
    from .tools.board import register_board_tools
    from .tools.connection import register_connection_tools
    from .tools.content import register_content_tools
    from .tools.explore import register_explore_tools
    from .tools.folder import register_folder_tools
    from .tools.git import register_git_tools
    from .tools.health import register_health_tools
    from .tools.modeling import register_modeling_tools
    from .tools.query import register_query_tools
    from .tools.schema import register_schema_tools
    from .tools.user_attributes import register_user_attribute_tools

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
        "health": register_health_tools,
    }

    registered = []
    for name, register_fn in _group_registry.items():
        if name in groups:
            register_fn(mcp, client)
            registered.append(name)

    logger.info("looker.server.created", groups=registered)

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
