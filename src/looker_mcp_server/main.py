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
from typing import Any

import structlog

from .config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig
from .middleware import HeaderCaptureMiddleware
from .server import build_public_mode_middleware, create_server, parse_groups

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


async def run(config: LookerConfig, groups: set[str]) -> None:
    """Start the server with graceful shutdown handling."""
    # ── MCP-level auth (optional bearer token) ───────────────────────
    auth: Any = None
    if config.mcp_auth_token:
        from fastmcp.server.auth import StaticTokenVerifier

        auth = StaticTokenVerifier(
            tokens={config.mcp_auth_token: {"client_id": "mcp-client", "scopes": []}}
        )

    mcp, client = create_server(config, enabled_groups=groups, auth=auth)

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
        from starlette.middleware import Middleware

        kwargs["transport"] = "streamable-http"
        kwargs["host"] = config.host
        kwargs["port"] = config.port
        # Composition order matters: PublicModeAuthMiddleware (when
        # present) runs FIRST so unauthenticated requests are rejected
        # before HeaderCaptureMiddleware copies anything into the
        # request-scoped ContextVar. In dev mode the returned value is
        # None and the chain is just [HeaderCaptureMiddleware] as before.
        middleware: list[Middleware] = []
        public_auth = build_public_mode_middleware(config)
        if public_auth is not None:
            middleware.append(public_auth)
        middleware.append(Middleware(HeaderCaptureMiddleware))
        kwargs["middleware"] = middleware
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
