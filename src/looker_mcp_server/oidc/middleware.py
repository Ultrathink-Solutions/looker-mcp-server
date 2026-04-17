"""ASGI middleware enforcing OAuth 2.1 resource-server semantics.

Sits in front of the MCP app and the PRM route. In ``LOOKER_MCP_MODE=
public``:

- Requests to unauthenticated paths (``/.well-known/*``, ``/healthz``,
  ``/readyz``) pass straight through.
- Requests carrying a bearer token in the URL query (``?access_token=``
  or ``?authorization=``) receive a 400 with an OAuth 2.1
  ``invalid_request`` body — OAuth 2.1 §5.1.1 bans URL-query-string
  bearer carriage. Checked before the ``Authorization`` header so the
  rejection is unconditional.
- Requests without an ``Authorization: Bearer ...`` header receive a 401
  with a ``WWW-Authenticate`` challenge pointing at the PRM document
  (RFC 9728 §5.1).
- Requests with a malformed or expired token receive the same 401 —
  the concrete reason is logged internally but collapsed to a generic
  message at the HTTP boundary per RFC 6750 §3.1.
- Successful validations attach the :class:`VerifiedClaims` to
  ``scope["state"]["verified_claims"]`` for downstream tool handlers to
  read.

Scope-level authorization (emitting the 403 ``insufficient_scope``
challenge) is left to per-tool checks the server factory can wire after
this middleware has authenticated the caller.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs

from .resource_server import OAuth21ResourceServer, TokenVerificationError
from .www_authenticate import invalid_token_challenge

# ASGI 3.0 type aliases, spelled out so the module is self-contained.
Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


# Paths that bypass token validation. These are either public by design
# (discovery / health probes) or have their own auth handling.
_BYPASS_PREFIXES: tuple[str, ...] = (
    "/.well-known/",
    "/healthz",
    "/readyz",
)


class PublicModeAuthMiddleware:
    """ASGI middleware that enforces OAuth 2.1 resource-server auth.

    Construct with a pre-configured :class:`OAuth21ResourceServer` and the
    ``resource`` identifier (used to build the ``WWW-Authenticate``
    ``realm``) plus the ``prm_url`` (used as the ``resource_metadata``
    parameter on 401 challenges).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        resource_server: OAuth21ResourceServer,
        realm: str,
        prm_url: str,
    ) -> None:
        if not realm:
            raise ValueError("realm must not be empty")
        if not prm_url:
            raise ValueError("prm_url must not be empty")
        self._app = app
        self._resource_server = resource_server
        self._realm = realm
        self._prm_url = prm_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            # Lifespan, websocket, etc. — pass through untouched.
            await self._app(scope, receive, send)
            return

        # Bearer-in-query is a protocol violation regardless of path —
        # URL tokens leak into referrer headers, proxy access logs, and
        # browser history. Checked before the bypass paths so that
        # /.well-known/* and /healthz can't be used to sneak a forbidden
        # query-string bearer past the gate.
        if await self._reject_bearer_in_query(scope, send):
            return

        path: str = scope.get("path") or ""
        if self._is_bypass_path(path):
            await self._app(scope, receive, send)
            return

        header_value = _get_header(scope, b"authorization")
        if header_value is None:
            await self._reply_401(send, body_message="missing authorization")
            return

        scheme, _, token = header_value.partition(" ")
        if scheme.lower() != "bearer" or not token:
            await self._reply_401(send, body_message="missing or malformed Bearer token")
            return

        try:
            verified = await self._resource_server.verify(token.strip())
        except TokenVerificationError:
            await self._reply_401(send, body_message="invalid token")
            return

        # Stash the verified claims on ``scope["state"]`` so downstream
        # handlers can read them without re-validating. The key matches
        # the Starlette ``request.state`` attribute path.
        state = scope.setdefault("state", {})
        state["verified_claims"] = verified

        await self._app(scope, receive, send)

    @staticmethod
    def _is_bypass_path(path: str) -> bool:
        """Return True if ``path`` exactly matches or is a proper sub-path of
        any bypass entry.

        A naive ``path.startswith(p)`` check over ``_BYPASS_PREFIXES`` would
        false-match ``/healthzfoo`` against ``/healthz``.  The fix: a match
        requires either strict equality to the prefix (with trailing slash
        stripped) or the prefix being followed by a ``/`` — a real path
        separator rather than an arbitrary continuation.
        """
        for raw in _BYPASS_PREFIXES:
            exact = raw.rstrip("/")
            with_sep = exact + "/"
            if path == exact or path.startswith(with_sep):
                return True
        return False

    async def _reject_bearer_in_query(self, scope: Scope, send: Send) -> bool:
        """OAuth 2.1 §5.1.1: bearer tokens in the URL query are forbidden.

        Returns True when a rejection was sent (caller should return).

        Uses ``urllib.parse.parse_qs`` to decode into parameter names —
        a substring match against the raw query string would false-positive
        on benign queries like ``?filter=access_token=foo`` where
        ``access_token`` appears in a *value* rather than as a key.
        """
        query_string: bytes = scope.get("query_string") or b""
        if not query_string:
            return False
        # Check both the spec-registered ``access_token`` (RFC 6750 §2.3
        # canonical name) and the defensive ``authorization`` parameter
        # (some clients misroute the header value into the query).
        params = parse_qs(query_string.decode("latin-1"), keep_blank_values=True)
        if "access_token" in params or "authorization" in params:
            body = json.dumps(
                {
                    "error": "invalid_request",
                    "error_description": (
                        "bearer tokens in the URL query string are forbidden "
                        "(OAuth 2.1 §5.1.1); use the Authorization header"
                    ),
                }
            ).encode("utf-8")
            await send(
                {
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return True
        return False

    async def _reply_401(self, send: Send, *, body_message: str) -> None:
        challenge = invalid_token_challenge(realm=self._realm, resource_metadata_url=self._prm_url)
        body = json.dumps({"error": "invalid_token", "error_description": body_message}).encode(
            "utf-8"
        )
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"www-authenticate", challenge.encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def _get_header(scope: Scope, name: bytes) -> str | None:
    """Return the first header matching ``name`` (lowercase-compared)."""
    for k, v in scope.get("headers") or []:
        if k.lower() == name:
            try:
                return v.decode("latin-1")
            except UnicodeDecodeError:  # pragma: no cover — HTTP headers are latin-1
                return None
    return None
