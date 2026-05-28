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

    # how the sudo target was selected — "argument" (per-call act_as_user
    # parameter) or "header" (gateway-injected request header). ``None``
    # for non-sudo identities. Surfaced in audit logs so operators can
    # tell admin-driven impersonation from gateway-driven impersonation.
    triggered_by: str | None = None


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
            triggered_by="header",
        )


class ArgumentSudoIdentityProvider:
    """Per-call admin impersonation via the ``act_as_user`` tool argument.

    Wraps an inner :class:`IdentityProvider`. When a tool invocation
    includes a non-empty ``act_as_user`` argument, this provider returns
    a sudo identity targeting that user — overriding whatever identity
    the inner provider would have resolved. When ``act_as_user`` is
    absent, the inner provider's resolution is returned unchanged.

    The argument value may be either:

    - a Looker user ID (e.g. ``"123"``)
    - an email address (containing ``@``), resolved to a user ID via
      ``user_lookup_fn``

    Sudo capability is enforced by Looker server-side: if the
    credentials backing the configured admin login cannot impersonate,
    ``login_user`` fails with HTTP 403. This provider only forwards
    capability — it does not gate it.

    Both invalid input formats and email-lookup misses raise
    ``ValueError`` rather than silently falling back to the inner
    identity. A silent fallback would make a mistyped email or stray
    string perform an action as the *configured* admin user instead of
    refusing — a footgun. Fail loudly so the caller can fix the input.

    Accepted forms: an email address (containing ``@``) or an
    all-digits Looker user ID. Anything else (e.g. a username
    fragment, a UUID, an empty-after-stripping string) is rejected up
    front with a clear validation error rather than being forwarded to
    Looker's ``/login/{value}`` endpoint where it would surface as an
    opaque HTTP 400/404.

    .. note::

       On **Looker (Google Cloud core)** only Embed-type users can be
       impersonated via sudo. For regular GCC users, configure
       ``OAuthIdentityProvider`` and pass tokens via the configured
       header instead.
    """

    def __init__(
        self,
        inner: IdentityProvider,
        client_id: str,
        client_secret: str,
        user_lookup_fn: Any | None = None,
        argument_name: str = "act_as_user",
        sudo_enabled: bool = True,
    ) -> None:
        self._inner = inner
        self._client_id = client_id
        self._client_secret = client_secret
        self._user_lookup_fn = user_lookup_fn
        self._argument_name = argument_name
        self._sudo_enabled = sudo_enabled
        self._email_to_id_cache: dict[str, str] = {}

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        raw = context.arguments.get(self._argument_name)
        if not raw:
            return await self._inner.resolve(context)

        act_as = str(raw).strip()
        if not act_as:
            return await self._inner.resolve(context)

        # Honor the deployment's sudo kill switch. We could skip
        # installing the wrapper entirely when ``sudo_enabled=False``,
        # but then ``act_as_user`` would silently route the call under
        # the configured identity — making an operator who disabled
        # sudo see calls succeed as the wrong user. Failing loudly is
        # the safer carve-out (cf. "no half-baked PRs": silent-disable
        # on opt-in is the failure mode to avoid).
        if not self._sudo_enabled:
            logger.warning(
                "looker.act_as_user.sudo_disabled",
                tool=context.tool_name,
            )
            raise ValueError(
                "act_as_user requires LOOKER_SUDO_AS_USER=true. "
                "Either enable sudo on the server or remove the "
                "act_as_user argument from the call."
            )

        if "@" in act_as:
            email = act_as
            user_id = self._email_to_id_cache.get(email)
            if user_id is None and self._user_lookup_fn is not None:
                user_id = await self._user_lookup_fn(email)
                if user_id is not None:
                    self._email_to_id_cache[email] = user_id
            if user_id is None:
                logger.warning(
                    "looker.act_as_user.lookup_miss",
                    email=email,
                    tool=context.tool_name,
                )
                raise ValueError(
                    f"act_as_user: no Looker user found for email {email!r}. "
                    "Verify the email or pass a numeric user ID instead."
                )
        elif act_as.isdigit():
            email = None
            user_id = act_as
        else:
            # Reject up front rather than forwarding to ``/login/{value}``
            # where Looker would respond with an opaque 400/404. Same
            # bad-input failure mode as a lookup miss.
            logger.warning(
                "looker.act_as_user.invalid_format",
                value=act_as,
                tool=context.tool_name,
            )
            raise ValueError(
                f"act_as_user: {act_as!r} is not a valid Looker user reference. "
                "Pass either a numeric Looker user ID (e.g. '42') or an email "
                "address (e.g. 'user@example.com')."
            )

        return LookerIdentity(
            mode="sudo",
            client_id=self._client_id,
            client_secret=self._client_secret,
            target_user_id=user_id,
            user_email=email,
            triggered_by="argument",
        )

    def set_user_lookup(self, fn: Any) -> None:
        """Inject the user-lookup function after the client is ready.

        Propagates the same function to the wrapped inner provider when
        it supports lookup injection, so a single call wires the whole
        chain (e.g. ``ArgumentSudo`` wrapping ``DualMode``).
        """
        self._user_lookup_fn = fn
        if hasattr(self._inner, "set_user_lookup"):
            self._inner.set_user_lookup(fn)  # type: ignore[attr-defined]


class OAuthIdentityProvider:
    """OAuth pass-through — uses a pre-obtained access token from an HTTP header.

    Ideal for Looker (Google Cloud core) deployments where regular users
    cannot be impersonated via sudo.  A gateway or MCP OAuth flow supplies
    the token.

    Two carriage shapes are supported, selected by ``strip_bearer_scheme``:

    - **Bare token** (default) — the header value IS the token. This is the
      ``X-User-Token`` shape a gateway uses after exchanging the user's
      identity for a Looker token out-of-band.
    - **``Authorization: Bearer <token>``** (``strip_bearer_scheme=True``) —
      the header carries the standard ``Bearer`` scheme prefix, which is
      stripped before use. This is the ``LOOKER_MCP_MODE=looker_oauth``
      shape, where the client presents its opaque Looker access token
      directly in the ``Authorization`` header (no gateway exchange).

    When ``fallback_client_id`` / ``fallback_client_secret`` are configured
    and no token is present, the provider falls back to API-key mode.
    Otherwise a missing token raises ``PermissionError`` — the no-credential
    ``looker_oauth`` posture deliberately omits the fallback so an
    unauthenticated request can never silently borrow a shared identity.
    """

    def __init__(
        self,
        token_header: str = "X-User-Token",
        fallback_client_id: str | None = None,
        fallback_client_secret: str | None = None,
        *,
        strip_bearer_scheme: bool = False,
    ) -> None:
        self._header = token_header.lower()
        self._fallback_id = fallback_client_id
        self._fallback_secret = fallback_client_secret
        self._strip_bearer_scheme = strip_bearer_scheme

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        token = context.headers.get(self._header)
        if token and self._strip_bearer_scheme:
            scheme, _, rest = token.partition(" ")
            # Only strip when the prefix is actually the Bearer scheme;
            # a bare opaque token without a space passes through unchanged.
            if scheme.lower() == "bearer":
                token = rest.strip()
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
