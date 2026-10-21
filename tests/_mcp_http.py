"""In-process HTTP harness for driving the server's ASGI app with a real MCP client.

The FastMCP client and the MCP SDK speak HTTP through ``httpx2``. Routing that
client through ``httpx2.ASGITransport`` exercises the full Starlette stack the
production ``streamable-http`` transport serves (auth gate, header capture,
the SDK's session manager and its protocol-era routing) without opening a
socket. The package's own Looker API client still uses ``httpx``, so ``respx``
mocks of Looker endpoints never intercept this traffic.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Generator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from starlette.applications import Starlette

BASE_URL = "http://testserver"

#: The two protocol eras a FastMCP 4 server serves, as ``Client(mode=...)``
#: values. ``legacy`` runs the 2025-11-25 initialize handshake over a
#: stateful session; ``2026-07-28`` sends self-contained requests with no
#: ``initialize`` and no ``Mcp-Session-Id``.
LEGACY = "legacy"
MODERN = "2026-07-28"
ERAS = (LEGACY, MODERN)

#: The protocol version a FastMCP 4 client negotiates in each era.
NEGOTIATED_VERSION = {LEGACY: "2025-11-25", MODERN: "2026-07-28"}


RequestHook = Callable[[httpx2.Request], Awaitable[None]]


class SwitchableBearer(httpx2.Auth):
    """Bearer auth whose token can change between requests on one connection.

    Lets a test prove identity is resolved per HTTP request rather than
    pinned at connect time (``initialize``) or to a session.
    """

    def __init__(self, token: str | None) -> None:
        self.token = token

    def auth_flow(
        self, request: httpx2.Request
    ) -> Generator[httpx2.Request, httpx2.Response, None]:
        if self.token is not None:
            request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


def asgi_client_factory(app: Starlette, *, on_request: RequestHook | None = None):
    """Return an ``httpx_client_factory`` that routes the MCP client into ``app``."""

    def factory(
        headers: dict[str, str] | None = None,
        timeout: httpx2.Timeout | None = None,
        auth: httpx2.Auth | None = None,
        **kwargs: Any,
    ) -> httpx2.AsyncClient:
        hooks: dict[str, list[Any]] = {"request": [on_request]} if on_request else {}
        return httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app),
            base_url=BASE_URL,
            headers=headers,
            auth=auth,
            timeout=timeout if timeout is not None else httpx2.Timeout(30.0),
            follow_redirects=True,
            event_hooks=hooks,
        )

    return factory


def http_client(
    app: Starlette,
    *,
    mode: str,
    auth: httpx2.Auth | str | None = None,
    headers: dict[str, str] | None = None,
    path: str = "/mcp",
    on_request: RequestHook | None = None,
) -> Client:
    """Build a FastMCP client that talks to ``app`` over streamable HTTP."""
    transport = StreamableHttpTransport(
        f"{BASE_URL}{path}",
        headers=headers,
        auth=auth,
        httpx_client_factory=asgi_client_factory(app, on_request=on_request),
    )
    return Client(transport, mode=mode)


@asynccontextmanager
async def serving(app: Starlette) -> AsyncIterator[httpx2.AsyncClient]:
    """Run ``app``'s lifespan (the SDK session manager) and yield a raw HTTP client."""
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url=BASE_URL
        ) as raw:
            yield raw


def recorded_methods() -> tuple[list[str], RequestHook]:
    """Return a list and a request hook that appends each JSON-RPC method to it."""
    methods: list[str] = []

    async def hook(request: httpx2.Request) -> None:
        if request.method != "POST" or not request.content:
            return
        try:
            body = json.loads(request.content)
        except ValueError:
            return
        if isinstance(body, dict) and isinstance(body.get("method"), str):
            methods.append(body["method"])

    return methods, hook


def era_message(
    era: str,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    request_id: int | None = 1,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Return ``(headers, body)`` for a raw JSON-RPC message sent in ``era``.

    A 2026-07-28 message carries its protocol version and client capabilities
    in ``params._meta`` (there is no ``initialize`` to negotiate them) and
    repeats the version, the method and, for ``tools/call``, the tool name in
    routing headers that the SDK checks against the body. ``request_id=None``
    makes the message a notification. A handshake-era message after
    ``initialize`` also needs the session headers from :func:`open_session`.
    """
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
    }
    body: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        body["id"] = request_id
    params = dict(params or {})
    if era == MODERN:
        params["_meta"] = {
            "io.modelcontextprotocol/protocolVersion": MODERN,
            "io.modelcontextprotocol/clientInfo": {"name": "test-client", "version": "1.0"},
            "io.modelcontextprotocol/clientCapabilities": {},
        }
        headers["mcp-protocol-version"] = MODERN
        headers["mcp-method"] = method
        if method == "tools/call" and isinstance(params.get("name"), str):
            headers["mcp-name"] = params["name"]
    elif method == "initialize":
        params.setdefault("protocolVersion", NEGOTIATED_VERSION[LEGACY])
        params.setdefault("capabilities", {})
        params.setdefault("clientInfo", {"name": "test-client", "version": "1.0"})
    if params:
        body["params"] = params
    return headers, body


async def open_session(raw: httpx2.AsyncClient, headers: dict[str, str]) -> dict[str, str]:
    """Run the 2025-11-25 handshake over raw HTTP and return the session headers.

    ``headers`` carries whatever the caller authenticates with; the returned
    headers add the ``Mcp-Session-Id`` and negotiated ``MCP-Protocol-Version``
    every later request in the session must send.
    """
    init_headers, init_body = era_message(LEGACY, "initialize")
    resp = await raw.post("/mcp", headers={**init_headers, **headers}, json=init_body)
    assert resp.status_code == 200, resp.text
    session = {
        **headers,
        "mcp-session-id": resp.headers["mcp-session-id"],
        "mcp-protocol-version": NEGOTIATED_VERSION[LEGACY],
    }
    ack_headers, ack_body = era_message(LEGACY, "notifications/initialized", request_id=None)
    ack = await raw.post("/mcp", headers={**ack_headers, **session}, json=ack_body)
    assert ack.status_code == 202, ack.text
    return session


def jsonrpc_reply(resp: httpx2.Response) -> dict[str, Any]:
    """Decode the single JSON-RPC reply in ``resp``, whether JSON or one SSE event."""
    if resp.headers.get("content-type", "").startswith("text/event-stream"):
        for line in resp.text.splitlines():
            if line.startswith("data:") and line[5:].strip():
                return json.loads(line[5:])
        raise AssertionError(f"no data event in SSE response: {resp.text!r}")
    return resp.json()
