"""Looker API client with per-request ephemeral sessions.

Each tool invocation gets its own authenticated session that is created
and torn down within the scope of the call.  This prevents token leakage
and supports concurrent requests with different identities.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import structlog

from .config import LookerConfig
from .identity import IdentityProvider, RequestContext
from .middleware import get_request_headers

logger = structlog.get_logger()


class LookerApiError(Exception):
    """Raised when a Looker API call fails."""

    def __init__(self, status_code: int, message: str, detail: str = "") -> None:
        self.status_code = status_code
        self.message = message
        self.detail = detail
        super().__init__(f"Looker API {status_code}: {message}")


class LookerSession:
    """Authenticated session for making Looker API calls.

    Instances are short-lived — created per tool invocation and closed
    immediately after.  All HTTP methods inject the session's bearer token.
    """

    def __init__(self, http: httpx.AsyncClient, token: str) -> None:
        self._http = http
        self._headers = {"Authorization": f"token {token}"}

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("GET", path, params=params)

    async def post(
        self,
        path: str,
        body: dict[str, Any] | list[Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("POST", path, params=params, json=body)

    async def patch(
        self,
        path: str,
        body: dict[str, Any] | list[Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("PATCH", path, params=params, json=body)

    async def put(
        self,
        path: str,
        body: dict[str, Any] | list[Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("PUT", path, params=params, json=body)

    async def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        await self._request("DELETE", path, params=params)

    async def get_text(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> str:
        """GET an endpoint that returns ``text/plain`` rather than JSON.

        Looker's git deploy-key endpoints return a raw SSH public key as
        plain text; calling ``.json()`` on the response would raise.
        Mirrors ``get()``'s error-handling so 4xx/5xx still raise
        ``LookerApiError`` with a usable detail body.
        """
        return await self._request_text("GET", path, params=params)

    async def post_text(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> str:
        """POST to an endpoint that returns ``text/plain`` (deploy-key rotation)."""
        return await self._request_text("POST", path, params=params)

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        """Raise ``LookerApiError`` with the best-available detail string.

        Looker error bodies are JSON ``{"message": ..., "errors": ...}`` in
        most cases but can be plain text for some endpoints (notably the
        text/plain ones). Try JSON first; fall back to a 500-char text
        truncation. Shared by both the JSON and text request paths so
        their error parsing can never drift.
        """
        if response.status_code < 400:
            return
        detail = ""
        try:
            body = response.json()
            detail = body.get("message", "") or body.get("error", "")
        except Exception:
            detail = response.text[:500]
        raise LookerApiError(response.status_code, response.reason_phrase, detail)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | list[Any] | None = None,
    ) -> Any:
        response = await self._http.request(
            method,
            path,
            headers=self._headers,
            params=params,
            json=json,
        )
        self._raise_for_status(response)

        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    async def _request_text(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> str:
        response = await self._http.request(
            method,
            path,
            headers=self._headers,
            params=params,
        )
        self._raise_for_status(response)
        return response.text


class LookerClient:
    """Manages per-request authenticated sessions to the Looker API.

    The client is long-lived (created once at server startup).  Individual
    sessions are ephemeral and scoped to a single tool invocation.

    Parameters
    ----------
    config:
        Server configuration.
    identity_provider:
        Resolves per-request identity (API key, sudo, or OAuth).
    """

    def __init__(self, config: LookerConfig, identity_provider: IdentityProvider) -> None:
        self._config = config
        self._identity_provider = identity_provider
        self._http = httpx.AsyncClient(
            base_url=config.api_url,
            timeout=httpx.Timeout(config.timeout),
            verify=config.verify_ssl,
        )

    # ── Public API ───────────────────────────────────────────────────

    @asynccontextmanager
    async def session(self, context: RequestContext) -> AsyncGenerator[LookerSession, None]:
        """Create an ephemeral authenticated session for a tool invocation.

        The session lifecycle depends on the resolved identity mode:

        - **api_key**: login with client credentials → yield → logout
        - **sudo**: admin login → login_user → yield → logout sudo → logout admin
        - **oauth**: use pre-obtained token directly → yield (no login/logout)
        """
        identity = await self._identity_provider.resolve(context)
        log = logger.bind(mode=identity.mode, tool=context.tool_name)

        match identity.mode:
            case "api_key":
                token = await self._login(identity.client_id, identity.client_secret)
                log.debug("looker.session.created")
                try:
                    yield LookerSession(self._http, token)
                finally:
                    await self._logout(token)

            case "sudo":
                admin_token = await self._login(identity.client_id, identity.client_secret)
                try:
                    sudo_token = await self._login_user(
                        admin_token,
                        identity.target_user_id,
                        associative=self._config.sudo_associative,
                    )
                    log.debug("looker.session.sudo", user_id=identity.target_user_id)
                    try:
                        yield LookerSession(self._http, sudo_token)
                    finally:
                        await self._logout(sudo_token)
                finally:
                    await self._logout(admin_token)

            case "oauth":
                if not identity.access_token:
                    raise ValueError("OAuth identity resolved without an access token.")
                log.debug("looker.session.oauth")
                yield LookerSession(self._http, identity.access_token)

            case _:
                raise ValueError(f"Unknown identity mode: {identity.mode!r}")

    def build_context(
        self,
        tool_name: str,
        tool_group: str,
        arguments: dict[str, Any] | None = None,
    ) -> RequestContext:
        """Build a ``RequestContext`` from the current request headers."""
        return RequestContext(
            headers=get_request_headers(),
            tool_name=tool_name,
            tool_group=tool_group,
            arguments=arguments or {},
        )

    async def check_connectivity(self) -> bool:
        """Test connectivity with a login/logout cycle (for readiness probes)."""
        if not self._config.client_id or not self._config.client_secret:
            return False
        try:
            token = await self._login(self._config.client_id, self._config.client_secret)
            await self._logout(token)
            return True
        except Exception:
            return False

    async def lookup_user_by_email(self, email: str) -> str | None:
        """Resolve a Looker user ID from an email address.

        Uses service-account credentials.  Returns ``None`` if no user is found.
        """
        token = await self._login(self._config.client_id, self._config.client_secret)
        try:
            session = LookerSession(self._http, token)
            users = await session.get("/users", params={"email": email, "limit": 1})
            if users and isinstance(users, list) and len(users) > 0:
                return str(users[0]["id"])
            return None
        finally:
            await self._logout(token)

    async def close(self) -> None:
        await self._http.aclose()

    # ── Internal helpers ─────────────────────────────────────────────

    async def _login(self, client_id: str | None, client_secret: str | None) -> str:
        """Authenticate with API3 credentials and return an access token."""
        response = await self._http.post(
            "/login",
            data={"client_id": client_id, "client_secret": client_secret},
        )
        if response.status_code >= 400:
            raise LookerApiError(
                response.status_code,
                "Authentication failed",
                "Check LOOKER_CLIENT_ID and LOOKER_CLIENT_SECRET.",
            )
        return response.json()["access_token"]

    async def _login_user(
        self, admin_token: str, user_id: str | None, *, associative: bool = False
    ) -> str:
        """Login as another user (sudo) using an admin token."""
        params: dict[str, Any] = {}
        if not associative:
            params["associative"] = "false"
        response = await self._http.post(
            f"/login/{user_id}",
            headers={"Authorization": f"token {admin_token}"},
            params=params,
        )
        if response.status_code >= 400:
            detail = ""
            try:
                detail = response.json().get("message", "")
            except Exception:
                pass
            raise LookerApiError(
                response.status_code,
                f"Sudo login failed for user {user_id}",
                detail,
            )
        return response.json()["access_token"]

    async def _logout(self, token: str) -> None:
        """Invalidate an access token.  Failures are silently ignored."""
        try:
            await self._http.delete(
                "/logout",
                headers={"Authorization": f"token {token}"},
            )
        except Exception:
            pass


def format_api_error(tool_name: str, error: Exception) -> str:
    """Format an error into a user-friendly JSON string for MCP responses."""
    if isinstance(error, LookerApiError):
        status = error.status_code
        match status:
            case 400:
                hint = "One or more parameters are invalid."
            case 401:
                hint = "Authentication failed — credentials may be expired or invalid."
            case 403:
                hint = "Permission denied — the current user lacks access."
            case 404:
                hint = "The requested resource was not found."
            case 429:
                hint = "Rate limited — too many requests. Retry after a brief wait."
            case s if s >= 500:
                hint = "Looker server error — the service may be temporarily unavailable."
            case _:
                hint = error.message
        result = {"error": hint, "status": status}
        if error.detail:
            result["detail"] = error.detail
    else:
        result = {"error": f"Unexpected error in {tool_name}: {error}"}
    return json.dumps(result, indent=2)
