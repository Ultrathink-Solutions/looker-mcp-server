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
from urllib.parse import quote

import httpx
import structlog

from .config import LookerConfig
from .identity import IdentityProvider, RequestContext
from .middleware import get_request_headers

logger = structlog.get_logger()


class LookerApiError(Exception):
    """Raised when a Looker API call fails."""

    def __init__(
        self,
        status_code: int,
        message: str,
        detail: str = "",
        body: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.detail = detail
        # Full decoded JSON error body when Looker returns a structured
        # response. Carries fields like ``sql`` (compiled SQL on query
        # failures), ``errors[]`` (LookML compile/evaluator diagnostics),
        # and ``applied_filters`` — the highest-signal debugging payload.
        # Left ``None`` for plain-text or unparseable bodies.
        self.body = body
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
        timeout: float | None = None,
    ) -> Any:
        return await self._request("GET", path, params=params, timeout=timeout)

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

    async def get_bytes(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, str, int, bool]:
        """GET an endpoint that returns binary content (image/png|jpeg, application/pdf).

        Returns ``(body_bytes, content_type, total_bytes, truncated)``:

        * ``body_bytes`` — accumulated content. Empty when the response
          is short-circuited by Content-Length, or contains the prefix
          read before the cap fired on a chunk boundary.
        * ``total_bytes`` — bytes seen on the wire (or Content-Length
          when the fast path triggered). Always set, even when truncated.
        * ``truncated`` — True when ``max_bytes`` was provided and the
          response exceeded it. Body is not safe to use in this case;
          callers must surface the size signal to the user.

        When ``max_bytes`` is set:

        * If the server returns a ``Content-Length`` header larger than
          ``max_bytes``, no body is downloaded — the helper returns
          immediately with ``body=b""`` and ``truncated=True``.
        * Otherwise the response is streamed; appending stops as soon
          as the running total exceeds the cap.

        Looker's render-task results endpoint (``/render_tasks/{id}/results``)
        is the canonical caller. Reuses :meth:`_raise_for_status` for
        4xx/5xx so the structured error body still reaches callers as a
        ``LookerApiError``.
        """
        return await self._request_bytes("GET", path, params=params, max_bytes=max_bytes)

    async def update_workspace(self, workspace_id: str) -> None:
        """Set the active workspace on this API session.

        Looker's session model treats workspace as a per-session property:
        ``PATCH /session {"workspace_id": "dev"}`` switches into the dev
        workspace; ``"production"`` switches back. The setting is bound to
        the bearer token and does not survive logout — every new login
        starts in production. Calling this method is idempotent (Looker
        returns the current state regardless of whether the value changed).

        Required prerequisite for any operation that needs the user's dev
        workspace: branch checkouts, dev-mode file edits, dev-LookML
        queries, dev-LookML data tests.
        """
        await self.patch("/session", body={"workspace_id": workspace_id})

    @asynccontextmanager
    async def use_branch(self, project_id: str, branch_name: str) -> AsyncGenerator[None, None]:
        """Atomically swap the project's branch for the duration of the block.

        Reads the dev workspace's currently-checked-out branch on the
        project, switches to ``branch_name`` for the body, and restores
        the saved branch in ``finally`` — even if the body raises. This
        is the safe default for one-shot operations against a feature
        branch (e.g. CI validating a PR).

        Caller must have already called ``update_workspace("dev")``;
        Looker rejects branch operations from the production workspace.
        If the dev workspace is already checked out to ``branch_name``,
        the swap and the restore are both no-ops.

        The restore is best-effort: if the second PUT fails (network
        flake, branch deleted concurrently), the failure is logged but
        does not mask the original exception.
        """
        path = f"/projects/{quote(project_id, safe='')}/git_branch"
        current = await self.get(path)
        saved = (current or {}).get("name")
        # Fail fast if Looker returns a malformed/empty payload without a
        # branch name. Without this guard, the swap would still PUT the
        # target branch but the restore would PUT ``{"name": None}``,
        # which Looker would reject — leaving the workspace stuck on the
        # caller-supplied branch and silently breaking the atomic swap.
        if not isinstance(saved, str) or not saved:
            raise LookerApiError(
                500,
                "Cannot swap branches",
                f"Project {project_id!r} returned no current branch name from "
                "GET /projects/{id}/git_branch — refusing to swap without a "
                "known restore target.",
            )
        if saved == branch_name:
            yield
            return
        await self.put(path, body={"name": branch_name})
        try:
            yield
        finally:
            try:
                await self.put(path, body={"name": saved})
            except Exception as restore_err:  # pragma: no cover — log-only
                logger.error(
                    "looker.branch.restore_failed",
                    project_id=project_id,
                    saved_branch=saved,
                    target_branch=branch_name,
                    error=str(restore_err),
                )

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        """Raise ``LookerApiError`` with the best-available detail string.

        Looker error bodies are JSON ``{"message": ..., "errors": ...}`` in
        most cases but can be plain text for some endpoints (notably the
        text/plain ones). Try JSON first; fall back to a 500-char text
        truncation. Shared by both the JSON and text request paths so
        their error parsing can never drift.

        When the body parses as a JSON object, it is captured verbatim on
        the raised ``LookerApiError`` so callers can surface high-signal
        fields like ``sql``, ``errors[]``, and ``applied_filters`` —
        critical for debugging query and LookML failures.
        """
        if response.status_code < 400:
            return
        detail = ""
        body: dict[str, Any] | None = None
        try:
            parsed = response.json()
        except Exception:
            detail = response.text[:500]
        else:
            if isinstance(parsed, dict):
                body = parsed
                # ``detail`` is part of the public ``LookerApiError`` /
                # ``format_api_error`` shape and consumers expect a string.
                # Looker normally returns ``message``/``error`` as strings,
                # but a non-conforming payload (nested object/array under
                # those keys) would otherwise leak a non-string and break
                # downstream string ops. Coerce defensively; the full
                # structured payload is still available via ``body``.
                hint = parsed.get("message") or parsed.get("error") or ""
                detail = hint if isinstance(hint, str) else json.dumps(hint)[:500]
            else:
                # Looker occasionally returns JSON arrays / scalars on error.
                # Stringify to keep ``detail`` populated; don't carry through
                # ``body`` since the contract is "dict or nothing".
                detail = json.dumps(parsed)[:500]
        raise LookerApiError(response.status_code, response.reason_phrase, detail, body=body)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | list[Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        # Per-call timeout overrides the connection default. Used for
        # long-running endpoints like ``/lookml_tests/run`` where data
        # tests can run for many minutes against the warehouse.
        request_kwargs: dict[str, Any] = {
            "headers": self._headers,
            "params": params,
            "json": json,
        }
        if timeout is not None:
            request_kwargs["timeout"] = httpx.Timeout(timeout)
        response = await self._http.request(method, path, **request_kwargs)
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

    async def _request_bytes(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, str, int, bool]:
        # Stream the response so a render result larger than ``max_bytes``
        # never fully materializes in memory. Error responses still go
        # through ``_raise_for_status`` (after one explicit ``aread``) so
        # the structured-body contract used by the JSON and text paths
        # carries over to binary callers.
        async with self._http.stream(
            method,
            path,
            headers=self._headers,
            params=params,
        ) as response:
            if response.status_code >= 400:
                await response.aread()
                self._raise_for_status(response)  # always raises

            # Strip parameters (charset, boundary) so callers can match
            # on the bare MIME type without case-sensitivity surprises.
            content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()

            # Trust ``Content-Length`` when the server provides it: if
            # the advertised size already exceeds the cap, skip the
            # download entirely. Looker's render-task results endpoint
            # always sets Content-Length, so this is the common path
            # for oversized renders.
            if max_bytes is not None:
                cl_header = response.headers.get("content-length")
                if cl_header is not None:
                    try:
                        cl = int(cl_header)
                    except ValueError:
                        cl = -1
                    if cl > max_bytes:
                        return b"", content_type, cl, True

            # No fast path: stream and accumulate, stopping as soon as
            # the running total passes the cap. We track ``total``
            # independently of ``len(body)`` so the truncated case can
            # still surface a size signal (lower bound = cap + 1).
            body = bytearray()
            total = 0
            truncated = False
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    truncated = True
                    break
                body.extend(chunk)
            return bytes(body), content_type, total, truncated


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
    async def session(
        self,
        context: RequestContext,
        *,
        dev_mode: bool = False,
    ) -> AsyncGenerator[LookerSession, None]:
        """Create an ephemeral authenticated session for a tool invocation.

        The session lifecycle depends on the resolved identity mode:

        - **api_key**: login with client credentials → yield → logout
        - **sudo**: admin login → login_user → yield → logout sudo → logout admin
        - **oauth**: use pre-obtained token directly → yield (no login/logout)

        When ``dev_mode=True``, the session is switched into the dev
        workspace via ``PATCH /session`` immediately after authentication.
        Looker scopes workspace selection to the bearer token, so this
        affects every subsequent call routed through the yielded
        ``LookerSession``. Required for branch checkouts, dev-mode file
        edits, dev-LookML queries, and dev-LookML data tests.
        """
        identity = await self._identity_provider.resolve(context)
        log = logger.bind(mode=identity.mode, tool=context.tool_name)

        match identity.mode:
            case "api_key":
                token = await self._login(identity.client_id, identity.client_secret)
                log.debug("looker.session.created")
                try:
                    session = LookerSession(self._http, token)
                    if dev_mode:
                        await session.update_workspace("dev")
                    yield session
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
                    log.debug(
                        "looker.session.sudo",
                        user_id=identity.target_user_id,
                        triggered_by=identity.triggered_by,
                    )
                    # Argument-driven sudo is admin impersonation requested
                    # explicitly by the caller (per-call ``act_as_user``
                    # parameter). It MUST be auditable independently of
                    # header-driven sudo (gateway pattern), so emit an
                    # INFO-level audit line for it. ``configured_user`` is
                    # the API3 client_id that performed the underlying
                    # ``login_user`` — i.e. the admin identity backing the
                    # impersonation.
                    if identity.triggered_by == "argument":
                        log.info(
                            "looker.audit.act_as_user",
                            tool=context.tool_name,
                            target_user_id=identity.target_user_id,
                            target_user_email=identity.user_email,
                            triggered_by=identity.triggered_by,
                            configured_user=identity.client_id,
                        )
                    try:
                        session = LookerSession(self._http, sudo_token)
                        if dev_mode:
                            await session.update_workspace("dev")
                        yield session
                    finally:
                        await self._logout(sudo_token)
                finally:
                    await self._logout(admin_token)

            case "oauth":
                if not identity.access_token:
                    raise ValueError("OAuth identity resolved without an access token.")
                log.debug("looker.session.oauth")
                session = LookerSession(self._http, identity.access_token)
                if dev_mode:
                    await session.update_workspace("dev")
                yield session

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

    async def check_reachability(self) -> bool:
        """Verify the configured ``base_url`` is network-reachable, no auth.

        Issues a single HEAD against the Looker instance's web root. Any
        HTTP response — including 401/403/404 — proves DNS resolution,
        TCP connect, and TLS handshake all succeeded, which is all a
        readiness probe needs to assert in deployments where the server
        does not hold API3 service-account credentials (e.g. OAuth
        pass-through, where per-request user tokens supply auth). Only
        transport-layer failures (DNS, connection refused, TLS error,
        timeout) return ``False``.

        Per-request auth is validated at tool-invoke time; readiness is a
        liveness-of-dependency check, not an auth check.
        """
        if not self._config.base_url:
            return False
        try:
            response = await self._http.head(
                self._config.base_url,
                follow_redirects=False,
                timeout=httpx.Timeout(5.0),
            )
            return 100 <= response.status_code < 600
        except httpx.HTTPError:
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
        result: dict[str, Any] = {"error": hint, "status": status}
        if error.detail:
            result["detail"] = error.detail
        # Surface the full Looker error body (sql, errors[], applied_filters,
        # fields.measures[].sql, …) so debuggers don't have to re-fetch via
        # raw REST. ``_raise_for_status`` only populates ``body`` when the
        # response decoded as a JSON object.
        if error.body is not None:
            result["body"] = error.body
    elif isinstance(error, ValueError):
        # Validation errors raised by tools or identity providers
        # (e.g. ``act_as_user`` rejecting a malformed value or an
        # unresolvable email) carry a self-describing message — surface
        # it directly instead of dressing it up as an "unexpected"
        # error, which would mislead callers about whether to retry.
        result = {"error": str(error)}
    else:
        result = {"error": f"Unexpected error in {tool_name}: {error}"}
    return json.dumps(result, indent=2)
