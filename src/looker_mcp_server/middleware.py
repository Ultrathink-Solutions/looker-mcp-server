"""Lightweight ASGI middleware for capturing request headers.

Stores per-request HTTP headers in a ``ContextVar`` so that tool handlers
can access identity headers (e.g. ``X-User-Email``, ``X-User-Token``)
without threading parameters through the FastMCP tool signature.

Custom deployments can replace this middleware with richer variants that
extract structured user context objects.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

_request_headers: ContextVar[dict[str, str]] = ContextVar("looker_mcp_request_headers")


def get_request_headers() -> dict[str, str]:
    """Return the HTTP headers for the current request.

    Returns an empty dict in stdio mode or outside a request scope.
    """
    return _request_headers.get({})


class HeaderCaptureMiddleware:
    """ASGI middleware that stores request headers in a ``ContextVar``.

    This enables tool handlers to read per-request headers (e.g.
    ``X-User-Email``, ``X-User-Token``) without threading parameters
    through the FastMCP tool signature.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] in ("http", "websocket"):
            headers = {
                k.decode("latin-1"): v.decode("latin-1") for k, v in scope.get("headers", [])
            }
            token = _request_headers.set(headers)
            try:
                await self.app(scope, receive, send)
            finally:
                _request_headers.reset(token)
        else:
            await self.app(scope, receive, send)
