"""Configuration for the Looker MCP server."""

from __future__ import annotations

from enum import Enum
from typing import Self
from urllib.parse import urlparse

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class LookerMcpMode(str, Enum):
    """Deployment posture for the MCP endpoint.

    See ``LookerConfig.mcp_mode`` for a description of each mode's behavior.
    """

    DEV = "dev"
    PUBLIC = "public"


class PostureErrorKind(str, Enum):
    """Discriminator for :class:`DeploymentPostureError` subclasses.

    Kinds round-trip cleanly into log records and JSON; callers switch on
    ``error.kind`` instead of parsing the human-readable message.
    """

    PUBLIC_MISSING_JWKS_URI = "public_missing_jwks_uri"
    PUBLIC_MISSING_ISSUER_URL = "public_missing_issuer_url"
    PUBLIC_MISSING_RESOURCE_URI = "public_missing_resource_uri"
    PUBLIC_RESOURCE_URI_NOT_HTTPS = "public_resource_uri_not_https"
    PUBLIC_RESOURCE_URI_MALFORMED = "public_resource_uri_malformed"
    PUBLIC_STATIC_BEARER_FORBIDDEN = "public_static_bearer_forbidden"


class DeploymentPostureError(ValueError):
    """Raised at startup when the configured deployment posture is
    self-inconsistent.

    Mirrors the ``ValueError`` contract of Pydantic validators so existing
    ``pydantic_settings`` callers observe no API break, while carrying
    a structured :attr:`kind` for operators / orchestration tooling.
    """

    def __init__(self, kind: PostureErrorKind, message: str) -> None:
        self.kind = kind
        super().__init__(f"[{kind.value}] {message}")


# Tool groups that are enabled by default (read-oriented, safe for most deployments).
DEFAULT_GROUPS = frozenset({"explore", "query", "schema", "content", "health"})

# All available tool groups.
ALL_GROUPS = frozenset(
    {
        "explore",
        "query",
        "schema",
        "content",
        "board",
        "folder",
        "modeling",
        "git",
        "admin",
        "connection",
        "user_attributes",
        "credentials",
        "audit",
        "workflows",
        "health",
    }
)


class LookerConfig(BaseSettings):
    """Looker MCP server configuration.

    All values can be set via environment variables with the ``LOOKER_`` prefix
    (e.g. ``LOOKER_BASE_URL``) or via a ``.env`` file.
    """

    model_config = SettingsConfigDict(
        env_prefix="LOOKER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Looker instance ──────────────────────────────────────────────
    base_url: str = ""
    """Base URL of the Looker instance (e.g. ``https://mycompany.looker.com``)."""

    api_version: str = "4.0"
    """Looker API version."""

    verify_ssl: bool = True
    """Verify TLS certificates when connecting to Looker."""

    # ── Service-account credentials (API3 key pair) ──────────────────
    client_id: str = ""
    """Looker API3 client ID for the service account."""

    client_secret: str = ""
    """Looker API3 client secret for the service account."""

    # ── Deployment type ──────────────────────────────────────────────
    deployment_type: str = "self_hosted"
    """``self_hosted`` or ``google_cloud_core``.

    Determines the impersonation strategy:
    - ``self_hosted``: admin login_user (sudo) — works for all user types.
    - ``google_cloud_core``: OAuth pass-through — required for regular users.
    """

    # ── Impersonation ────────────────────────────────────────────────
    sudo_as_user: bool = True
    """When ``True`` (and headers carry user identity), impersonate the user
    via ``login_user`` or OAuth token depending on ``deployment_type``.
    When ``False``, always use the service-account credentials."""

    sudo_associative: bool = False
    """Looker ``login_user`` ``associative`` parameter.
    ``False`` = activity attributed to the impersonated user (default).
    ``True`` = activity attributed to the admin."""

    user_email_header: str = "X-User-Email"
    """HTTP header that carries the user's email address for impersonation."""

    user_token_header: str = "X-User-Token"
    """HTTP header that carries a pre-exchanged OAuth token for the user."""

    # ── Transport ────────────────────────────────────────────────────
    transport: str = "stdio"
    """``stdio`` for local/CLI usage, ``streamable-http`` for production."""

    host: str = "0.0.0.0"
    port: int = 8080

    # ── K8s service-discovery env var collision guards ────────────────
    # When deployed as a K8s Service named "looker", K8s auto-injects
    # LOOKER_PORT=tcp://10.x.x.x:8080 and LOOKER_HOST=10.x.x.x which
    # collide with the LOOKER_ env prefix.  Discard those values.
    @field_validator("port", mode="before")
    @classmethod
    def _ignore_k8s_port(cls, v: object) -> object:
        if isinstance(v, str) and v.startswith("tcp://"):
            return 8080  # default
        return v

    @field_validator("host", mode="before")
    @classmethod
    def _ignore_k8s_host(cls, v: object) -> object:
        if isinstance(v, str) and v.startswith("tcp://"):
            return "0.0.0.0"  # default
        return v

    # ── Server behaviour ─────────────────────────────────────────────
    timeout: float = 60.0
    """HTTP request timeout in seconds for Looker API calls."""

    max_rows: int = 5000
    """Default maximum rows returned by query tools."""

    log_level: str = "INFO"

    # ── MCP-level auth (who can talk to *this* server) ───────────────
    mcp_mode: "LookerMcpMode" = None  # type: ignore[assignment]  # defaulted in validator
    """Deployment posture for the MCP endpoint.

    - ``dev`` (default): permissive. Local development and trusted-network
      deployments. ``LOOKER_MCP_AUTH_TOKEN`` (static bearer) is accepted for
      backwards compatibility with existing OSS users.
    - ``public``: internet-exposed deployment. Conforms to MCP 2025-11-25
      authorization requirements — OAuth 2.1 resource server with RS256/ES256
      JWKS-based token validation. Static-bearer mode is rejected at
      startup; RFC 9068 §2.1 forbids symmetric signing for access tokens.
    """

    mcp_auth_token: str = ""
    """Static bearer token for MCP-level authentication.

    **Deprecated in ``public`` mode** — RFC 9068 §2.1 forbids symmetric
    signing for OAuth 2.1 access tokens. In ``dev`` mode this remains
    accepted for local iteration; in ``public`` mode the server fails
    startup with a pointer to the OIDC mode documentation.
    """

    # ── OIDC resource-server configuration (used in public mode) ─────
    mcp_jwks_uri: str = ""
    """URL of the JWK Set document (RFC 7517) for the authorization server
    that issues access tokens bound to this resource. Required when
    ``mcp_mode=public``."""

    mcp_issuer_url: str = ""
    """Expected ``iss`` claim — the authorization server's issuer URL
    (RFC 8414). Required when ``mcp_mode=public``."""

    mcp_resource_uri: str = ""
    """Canonical URI of this resource server, used for audience binding
    (RFC 8707 §2) and as the ``resource`` field in the Protected Resource
    Metadata document (RFC 9728 §2). Required when ``mcp_mode=public``."""

    # ── Validators ───────────────────────────────────────────────────
    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @field_validator("deployment_type")
    @classmethod
    def _validate_deployment_type(cls, v: str) -> str:
        allowed = {"self_hosted", "google_cloud_core"}
        if v not in allowed:
            msg = f"deployment_type must be one of {allowed}, got {v!r}"
            raise ValueError(msg)
        return v

    @field_validator("transport")
    @classmethod
    def _validate_transport(cls, v: str) -> str:
        allowed = {"stdio", "streamable-http"}
        if v not in allowed:
            msg = f"transport must be one of {allowed}, got {v!r}"
            raise ValueError(msg)
        return v

    @field_validator("mcp_mode", mode="before")
    @classmethod
    def _default_mcp_mode(cls, v: object) -> object:
        """Default ``mcp_mode`` to :attr:`LookerMcpMode.DEV` when unset.

        Kept separate from the main model default because pydantic-settings'
        env-var binding treats ``None`` as "use the declared default," and we
        want the declared default to resolve to the enum value (not ``None``).
        """
        if v is None or v == "":
            return LookerMcpMode.DEV
        return v

    @field_validator("mcp_resource_uri")
    @classmethod
    def _strip_resource_uri_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @model_validator(mode="after")
    def _deprecation_warn_static_bearer_in_dev(self) -> Self:
        """Log a deprecation notice when ``LOOKER_MCP_AUTH_TOKEN`` is used
        in ``dev`` mode — the static-bearer mode is scheduled for removal
        in a future major.

        Emitted at config-resolution time rather than request time so
        operators see it once per process start, not per request.
        """
        if self.mcp_mode == LookerMcpMode.DEV and self.mcp_auth_token:
            import warnings

            warnings.warn(
                "LOOKER_MCP_AUTH_TOKEN (static-bearer MCP auth) is deprecated. "
                "It will be removed in a future release. Migrate to OIDC mode: "
                "set LOOKER_MCP_MODE=public and configure LOOKER_MCP_JWKS_URI, "
                "LOOKER_MCP_ISSUER_URL, and LOOKER_MCP_RESOURCE_URI. "
                "See the README for details.",
                DeprecationWarning,
                stacklevel=2,
            )
        return self

    @model_validator(mode="after")
    def _validate_public_mode_posture(self) -> Self:
        """Enforce the MCP 2025-11-25 MUSTs that can be statically checked.

        Only runs in :attr:`LookerMcpMode.PUBLIC`. In ``dev`` mode the server
        stays permissive so existing OSS users' local workflows keep working.
        """
        if self.mcp_mode != LookerMcpMode.PUBLIC:
            return self

        # Static-bearer mode is incompatible with OAuth 2.1 access-token
        # semantics (RFC 9068 §2.1). Fail closed rather than silently accept
        # a spec-incompliant credential.
        if self.mcp_auth_token:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_STATIC_BEARER_FORBIDDEN,
                "LOOKER_MCP_AUTH_TOKEN is set but LOOKER_MCP_MODE=public forbids "
                "symmetric static bearers (RFC 9068 §2.1). Configure OIDC mode "
                "via LOOKER_MCP_JWKS_URI / LOOKER_MCP_ISSUER_URL / LOOKER_MCP_RESOURCE_URI, "
                "or switch to LOOKER_MCP_MODE=dev for local iteration.",
            )

        if not self.mcp_jwks_uri:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_MISSING_JWKS_URI,
                "LOOKER_MCP_MODE=public requires LOOKER_MCP_JWKS_URI — "
                "the JWK Set document (RFC 7517) of the authorization server "
                "that issues access tokens for this resource.",
            )

        if not self.mcp_issuer_url:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_MISSING_ISSUER_URL,
                "LOOKER_MCP_MODE=public requires LOOKER_MCP_ISSUER_URL — "
                "the expected JWT `iss` claim (RFC 8414).",
            )

        if not self.mcp_resource_uri:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_MISSING_RESOURCE_URI,
                "LOOKER_MCP_MODE=public requires LOOKER_MCP_RESOURCE_URI — "
                "this server's canonical URI for audience binding (RFC 8707 §2) "
                "and for the Protected Resource Metadata `resource` field (RFC 9728 §2).",
            )

        parsed = urlparse(self.mcp_resource_uri)
        if not parsed.scheme or not parsed.netloc:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_RESOURCE_URI_MALFORMED,
                f"LOOKER_MCP_RESOURCE_URI={self.mcp_resource_uri!r} is not a "
                "valid absolute URI.",
            )
        if parsed.fragment:
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_RESOURCE_URI_MALFORMED,
                f"LOOKER_MCP_RESOURCE_URI={self.mcp_resource_uri!r} has a "
                "fragment component; RFC 9728 §3 forbids fragments in the "
                "resource identifier.",
            )
        if parsed.scheme != "https":
            raise DeploymentPostureError(
                PostureErrorKind.PUBLIC_RESOURCE_URI_NOT_HTTPS,
                f"LOOKER_MCP_RESOURCE_URI={self.mcp_resource_uri!r} must use "
                "the https scheme in public mode (OAuth 2.1 §5.1.1).",
            )
        return self

    def is_http(self) -> bool:
        return self.transport == "streamable-http"

    @property
    def api_url(self) -> str:
        """Full base URL for API requests (e.g. ``https://co.looker.com/api/4.0``)."""
        return f"{self.base_url}/api/{self.api_version}"
