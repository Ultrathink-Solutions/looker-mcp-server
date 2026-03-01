"""Identity resolution for Looker API authentication.

The ``IdentityProvider`` protocol is the primary extension point.  The server
ships four built-in providers; custom wrappers can substitute their own
implementation to integrate with existing identity infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import structlog

logger = structlog.get_logger()


# ── Data types ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class RequestContext:
    """Context available to identity providers for every tool invocation."""

    headers: dict[str, str] = field(default_factory=dict)
    """HTTP request headers (empty in stdio mode)."""

    tool_name: str = ""
    """Name of the MCP tool being invoked."""

    tool_group: str = ""
    """Tool group the tool belongs to."""

    arguments: dict[str, Any] = field(default_factory=dict)
    """Arguments passed to the tool."""


@dataclass(frozen=True)
class LookerIdentity:
    """Resolved identity for a single Looker API session."""

    mode: str
    """``api_key`` | ``sudo`` | ``oauth``"""

    # api_key / sudo mode
    client_id: str | None = None
    client_secret: str | None = None

    # sudo mode
    target_user_id: str | None = None

    # oauth mode
    access_token: str | None = None

    # metadata (for logging / audit — not used for auth)
    user_email: str | None = None
    user_subject: str | None = None


# ── Protocol ─────────────────────────────────────────────────────────


@runtime_checkable
class IdentityProvider(Protocol):
    """Resolve per-request Looker identity.

    Implement this protocol to plug in custom authentication flows.
    The open-source server ships ``ApiKeyIdentityProvider``,
    ``SudoIdentityProvider``, and ``OAuthIdentityProvider``.
    """

    async def resolve(self, context: RequestContext) -> LookerIdentity: ...


# ── Built-in providers ───────────────────────────────────────────────


class ApiKeyIdentityProvider:
    """Default provider — uses configured API3 client credentials for every request."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        return LookerIdentity(
            mode="api_key",
            client_id=self._client_id,
            client_secret=self._client_secret,
        )


class SudoIdentityProvider:
    """Admin impersonation via ``login_user`` (sudo).

    Requires admin-level API3 credentials.  Resolves the target user by
    looking up their email address (from a configurable HTTP header) via the
    Looker API, then generates a sudo token.

    Falls back to service-account mode when no user header is present.

    .. note::

       On **Looker (Google Cloud core)** only Embed-type users can be
       impersonated via sudo.  Regular users require the
       ``OAuthIdentityProvider`` instead.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_email_header: str = "X-User-Email",
        user_lookup_fn: Any | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._header = user_email_header.lower()
        self._user_lookup_fn = user_lookup_fn
        self._email_to_id_cache: dict[str, str] = {}

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        email = context.headers.get(self._header)
        if not email:
            return LookerIdentity(
                mode="api_key",
                client_id=self._client_id,
                client_secret=self._client_secret,
            )

        user_id = self._email_to_id_cache.get(email)
        if user_id is None and self._user_lookup_fn is not None:
            user_id = await self._user_lookup_fn(email)
            if user_id is not None:
                self._email_to_id_cache[email] = user_id

        if user_id is None:
            logger.warning("looker.sudo.user_not_found", email=email)
            return LookerIdentity(
                mode="api_key",
                client_id=self._client_id,
                client_secret=self._client_secret,
            )

        return LookerIdentity(
            mode="sudo",
            client_id=self._client_id,
            client_secret=self._client_secret,
            target_user_id=user_id,
            user_email=email,
        )


class OAuthIdentityProvider:
    """OAuth pass-through — uses a pre-obtained access token from an HTTP header.

    Ideal for Looker (Google Cloud core) deployments where regular users
    cannot be impersonated via sudo.  A gateway or MCP OAuth flow supplies
    the token.

    Falls back to API-key mode when no token header is present.
    """

    def __init__(
        self,
        token_header: str = "X-User-Token",
        fallback_client_id: str | None = None,
        fallback_client_secret: str | None = None,
    ) -> None:
        self._header = token_header.lower()
        self._fallback_id = fallback_client_id
        self._fallback_secret = fallback_client_secret

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        token = context.headers.get(self._header)
        if token:
            return LookerIdentity(mode="oauth", access_token=token)

        if self._fallback_id and self._fallback_secret:
            return LookerIdentity(
                mode="api_key",
                client_id=self._fallback_id,
                client_secret=self._fallback_secret,
            )

        raise PermissionError(
            "No OAuth token in request headers and no fallback API credentials configured."
        )


class DualModeIdentityProvider:
    """Automatically selects sudo or OAuth based on deployment type and request context.

    This is the default provider when ``sudo_as_user=True`` in the config.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        deployment_type: str = "self_hosted",
        user_email_header: str = "X-User-Email",
        user_token_header: str = "X-User-Token",
    ) -> None:
        self._sudo = SudoIdentityProvider(
            client_id=client_id,
            client_secret=client_secret,
            user_email_header=user_email_header,
        )
        self._oauth = OAuthIdentityProvider(
            token_header=user_token_header,
            fallback_client_id=client_id,
            fallback_client_secret=client_secret,
        )
        self._deployment_type = deployment_type

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        if self._deployment_type == "google_cloud_core":
            return await self._oauth.resolve(context)
        return await self._sudo.resolve(context)

    def set_user_lookup(self, fn: Any) -> None:
        """Inject the user-lookup function after the client is ready."""
        self._sudo._user_lookup_fn = fn
