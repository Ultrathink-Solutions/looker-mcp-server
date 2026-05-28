"""Opaque-token introspection for ``LOOKER_MCP_MODE=looker_oauth``.

In the Looker-as-authorization-server posture the inbound bearer is an
**opaque** Looker access token, not a JWT — there is no signature to verify
against a JWKS, no ``iss``/``aud`` claims to bind. The token is validated the
way Looker itself validates it: by presenting it to the Looker API and asking
"who is this?" via ``GET /user`` (RFC 7662-style introspection against the
resource owner). Looker answers 200 with the user record when the token is
live and authorized, and 401/403 when it is expired, revoked, or malformed.

The two pieces here:

- :class:`LookerUserIntrospector` — the vendor-specific verifier. Calls
  ``GET {base_url}/api/{api_version}/user`` with the presented bearer and
  returns a :class:`LookerUser` on success, raising
  :class:`OpaqueTokenVerificationError` otherwise.
- :class:`LookerOAuthAuthMiddleware` — the ASGI gate. Mirrors
  :class:`~looker_mcp_server.oidc.middleware.PublicModeAuthMiddleware`'s
  contract (same bypass paths, same bearer-in-query 400, same 401 challenge
  shape) but swaps JWT validation for opaque-token introspection. On success
  it leaves the ``Authorization`` header untouched so the downstream
  identity provider can forward the same token to Looker as the session
  token — the user's own Looker permissions then govern every API call.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

import httpx

from .www_authenticate import invalid_token_challenge

logger = logging.getLogger(__name__)

# ASGI 3.0 type aliases, spelled out so the module is self-contained.
Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


# Paths that bypass token validation — parity with the public-mode gate.
# Public by design (discovery / health probes) or self-guarding
# (``/_introspect`` runs its own optional shared-bearer check).
_BYPASS_PREFIXES: tuple[str, ...] = (
    "/.well-known/",
    "/healthz",
    "/readyz",
    "/_introspect",
)


class OpaqueTokenVerificationError(Exception):
    """Raised when an opaque Looker token fails introspection.

    Deliberately coarse — the HTTP boundary collapses every failure mode
    (expired, revoked, malformed, Looker unreachable) into a single
    ``invalid_token`` 401 so an unauthenticated caller learns nothing about
    which specific check failed.
    """


@dataclass(frozen=True)
class LookerUser:
    """The minimal identity slice :class:`LookerUserIntrospector` extracts.

    Looker's ``/user`` response is large; we keep only the fields a caller
    plausibly needs for audit/logging. ``id`` is always present on a valid
    response; the rest are best-effort.
    """

    id: str
    email: str | None = None
    display_name: str | None = None


class LookerUserIntrospector:
    """Verify an opaque Looker access token by calling ``GET /user``.

    Args:
        base_url: Looker instance base URL (e.g. ``https://co.looker.com``).
            Combined with ``api_version`` to form the introspection URL.
        api_version: Looker API version path segment (default ``4.0``).
        verify_ssl: Verify TLS on the introspection call. Mirrors the
            client-wide ``LOOKER_VERIFY_SSL`` setting.
        timeout_seconds: Per-introspection HTTP timeout.
        http_client: Optional pre-built ``httpx.AsyncClient`` (tests inject
            one; production lets the introspector own its client lifecycle).
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_version: str = "4.0",
        verify_ssl: bool = True,
        timeout_seconds: float = 10.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url must not be empty")
        self._user_url = f"{base_url.rstrip('/')}/api/{api_version}/user"
        self._verify_ssl = verify_ssl
        self._timeout = timeout_seconds
        self._client = http_client

    async def verify(self, token: str) -> LookerUser:
        """Introspect ``token`` against Looker. Returns the :class:`LookerUser`.

        Raises:
            OpaqueTokenVerificationError: On any failure — empty token,
                Looker rejecting the token (401/403), Looker unreachable, or
                a 200 response that doesn't carry a usable user ``id``.
        """
        if not token:
            raise OpaqueTokenVerificationError("empty token")

        headers = {"Authorization": f"token {token}"}
        try:
            if self._client is not None:
                resp = await self._client.get(
                    self._user_url, headers=headers, timeout=self._timeout
                )
            else:
                async with httpx.AsyncClient(
                    timeout=self._timeout, verify=self._verify_ssl
                ) as client:
                    resp = await client.get(self._user_url, headers=headers)
        except httpx.HTTPError as exc:
            # Looker unreachable / TLS / timeout. Fail closed — never admit a
            # token we couldn't actually verify.
            logger.warning(
                "looker_oauth.introspect.unreachable",
                extra={"url": self._user_url, "error": str(exc)},
            )
            raise OpaqueTokenVerificationError("introspection request failed") from exc

        if resp.status_code in (401, 403):
            logger.info(
                "looker_oauth.introspect.rejected",
                extra={"status": resp.status_code},
            )
            raise OpaqueTokenVerificationError("token rejected by Looker")
        if resp.status_code != 200:
            logger.warning(
                "looker_oauth.introspect.unexpected_status",
                extra={"status": resp.status_code},
            )
            raise OpaqueTokenVerificationError(
                f"unexpected introspection status {resp.status_code}"
            )

        try:
            payload = resp.json()
        except ValueError as exc:
            raise OpaqueTokenVerificationError("introspection response not JSON") from exc

        if not isinstance(payload, dict) or payload.get("id") is None:
            raise OpaqueTokenVerificationError("introspection response missing user id")

        return LookerUser(
            id=str(payload["id"]),
            email=payload.get("email"),
            display_name=payload.get("display_name"),
        )


class LookerOAuthAuthMiddleware:
    """ASGI gate for ``looker_oauth`` mode — opaque-token introspection.

    Constructed with a :class:`LookerUserIntrospector` and the ``realm`` +
    ``prm_url`` used to build ``WWW-Authenticate`` challenges. Behavior
    mirrors :class:`~looker_mcp_server.oidc.middleware.PublicModeAuthMiddleware`
    so operators see one consistent HTTP contract across postures:

    - bearer-in-query → 400 ``invalid_request`` (OAuth 2.1 §5.1.1),
    - bypass paths pass through anonymously,
    - missing / malformed bearer → 401 with PRM-pointing challenge,
    - failed introspection → 401 ``invalid_token``,
    - success → request proceeds with the ``Authorization`` header intact so
      the identity provider can forward the verified opaque token to Looker.
      The resolved :class:`LookerUser` is stashed on
      ``scope["state"]["looker_user"]`` for downstream audit/logging.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        introspector: LookerUserIntrospector,
        realm: str,
        prm_url: str,
    ) -> None:
        if not realm:
            raise ValueError("realm must not be empty")
        if not prm_url:
            raise ValueError("prm_url must not be empty")
        self._app = app
        self._introspector = introspector
        self._realm = realm
        self._prm_url = prm_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        # Bearer-in-query is a protocol violation regardless of path —
        # checked before the bypass paths so /.well-known/* can't be used to
        # sneak a forbidden query-string bearer past the gate.
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
            user = await self._introspector.verify(token.strip())
        except OpaqueTokenVerificationError:
            await self._reply_401(send, body_message="invalid token")
            return

        state = scope.setdefault("state", {})
        state["looker_user"] = user

        await self._app(scope, receive, send)

    @staticmethod
    def _is_bypass_path(path: str) -> bool:
        """Match-or-proper-sub-path check against the bypass prefixes.

        A naive ``startswith`` would false-match ``/healthzfoo`` against
        ``/healthz``; require either exact equality (trailing slash stripped)
        or a ``/`` separator after the prefix.
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
        Parses parameter NAMES (not a substring match) so a benign value
        containing ``access_token`` doesn't false-positive.
        """
        query_string: bytes = scope.get("query_string") or b""
        if not query_string:
            return False
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
