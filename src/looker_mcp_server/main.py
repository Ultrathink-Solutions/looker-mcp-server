"""Entry point for the Looker MCP server.

Supports two transport modes:

    # Local development / Claude Code (stdio)
    looker-mcp-server --groups explore,query

    # Production deployment (HTTP)
    LOOKER_TRANSPORT=streamable-http looker-mcp-server --groups all
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from typing import TYPE_CHECKING, Any

import structlog
from starlette.middleware import Middleware

from .config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig, LookerMcpMode
from .middleware import HeaderCaptureMiddleware
from .server import (
    build_looker_oauth_mode_middleware,
    build_public_mode_middleware,
    create_server,
    parse_groups,
)

if TYPE_CHECKING:
    from fastmcp.server.auth import StaticTokenVerifier

logger = structlog.get_logger()


def cli() -> None:
    """CLI entry point (registered as ``looker-mcp-server`` console script)."""
    parser = argparse.ArgumentParser(
        prog="looker-mcp-server",
        description="MCP server for the Looker API",
    )
    parser.add_argument(
        "--groups",
        type=str,
        default=None,
        help=(
            f"Comma-separated tool groups to enable, or 'all'. "
            f"Default: {','.join(sorted(DEFAULT_GROUPS))}. "
            f"Available: {','.join(sorted(ALL_GROUPS))}"
        ),
    )
    parser.add_argument(
        "--transport",
        type=str,
        default=None,
        choices=["stdio", "streamable-http"],
        help="Transport mode (overrides LOOKER_TRANSPORT env var)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP port (overrides LOOKER_PORT env var)",
    )
    args = parser.parse_args()

    # Load config from env, then apply CLI overrides.
    config = LookerConfig()
    if args.transport:
        config = config.model_copy(update={"transport": args.transport})
    if args.port:
        config = config.model_copy(update={"port": args.port})

    # Resolve groups.
    if args.groups:
        groups = parse_groups(args.groups)
    else:
        groups = set(DEFAULT_GROUPS)

    asyncio.run(run(config, groups))


def build_server_auth(config: LookerConfig) -> StaticTokenVerifier | None:
    """Return the FastMCP auth provider for ``config``, or ``None``.

    ``LOOKER_MCP_AUTH_TOKEN`` installs FastMCP's static bearer verifier, which
    checks the ``Authorization`` header on every HTTP request to ``/mcp``.
    """
    if not config.mcp_auth_token:
        return None
    from fastmcp.server.auth import StaticTokenVerifier

    return StaticTokenVerifier(
        tokens={config.mcp_auth_token: {"client_id": "mcp-client", "scopes": []}}
    )


def build_http_middleware(config: LookerConfig) -> list[Middleware]:
    """Return the ASGI middleware chain for the streamable-http transport.

    Composition order matters: the mode-specific auth gate (when present)
    runs FIRST so unauthenticated requests are rejected before
    :class:`HeaderCaptureMiddleware` copies anything into the request-scoped
    ``ContextVar``. ``public`` mode validates a JWKS-backed JWT;
    ``looker_oauth`` mode introspects an opaque Looker token via
    ``GET /user``. At most one is non-None for a given config. In dev mode
    both return None and the chain is just ``[HeaderCaptureMiddleware]``.

    Every layer works on the HTTP request alone, never on MCP session state,
    so it applies identically to a 2025-11-25 handshake-era request (which
    belongs to a session opened by ``initialize``) and to a self-contained
    2026-07-28 request (which has no ``initialize`` and no session).
    """
    middleware: list[Middleware] = []
    auth_gate = build_public_mode_middleware(config) or build_looker_oauth_mode_middleware(config)
    if auth_gate is not None:
        middleware.append(auth_gate)
    middleware.append(Middleware(HeaderCaptureMiddleware))
    return middleware


async def run(config: LookerConfig, groups: set[str]) -> None:
    """Start the server with graceful shutdown handling."""
    # ``public`` and ``looker_oauth`` are HTTP-only auth postures: their
    # authentication gate is mounted only on the streamable-http transport
    # (below). Pairing either with stdio would silently start the server with
    # NO MCP-layer authentication, so fail fast at startup rather than ship
    # that misconfiguration and surface it as unauthenticated access.
    if (
        config.mcp_mode in (LookerMcpMode.PUBLIC, LookerMcpMode.LOOKER_OAUTH)
        and not config.is_http()
    ):
        raise SystemExit(
            f"LOOKER_MCP_MODE={config.mcp_mode.value} requires "
            "LOOKER_TRANSPORT=streamable-http — its authentication gate is "
            "HTTP-only; stdio would run unauthenticated."
        )

    # ── MCP-level auth (optional bearer token) ───────────────────────
    mcp, client = create_server(config, enabled_groups=groups, auth=build_server_auth(config))

    # ── Shutdown handling ────────────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _signal_handler(signum: int, frame: Any) -> None:
        logger.info("looker.shutdown.requested", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # ── Transport kwargs ─────────────────────────────────────────────
    kwargs: dict[str, Any] = {}
    if config.is_http():
        kwargs["transport"] = "streamable-http"
        kwargs["host"] = config.host
        kwargs["port"] = config.port
        kwargs["middleware"] = build_http_middleware(config)
    else:
        kwargs["transport"] = "stdio"

    logger.info(
        "looker.server.starting",
        transport=config.transport,
        groups=sorted(groups),
        base_url=config.base_url or "(not configured)",
    )

    try:
        server_task = asyncio.create_task(mcp.run_async(**kwargs))
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            {server_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
    finally:
        await client.close()
        logger.info("looker.server.stopped")


# Allow ``python -m looker_mcp_server``
if __name__ == "__main__":
    cli()
