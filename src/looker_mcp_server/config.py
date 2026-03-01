"""Configuration for the Looker MCP server."""

from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Tool groups that are enabled by default (read-oriented, safe for most deployments).
DEFAULT_GROUPS = frozenset({"explore", "query", "schema", "content", "health"})

# All available tool groups.
ALL_GROUPS = frozenset(
    {
        "explore",
        "query",
        "schema",
        "content",
        "modeling",
        "git",
        "admin",
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

    # ── Server behaviour ─────────────────────────────────────────────
    timeout: float = 60.0
    """HTTP request timeout in seconds for Looker API calls."""

    max_rows: int = 5000
    """Default maximum rows returned by query tools."""

    log_level: str = "INFO"

    # ── MCP-level auth (who can talk to *this* server) ───────────────
    mcp_auth_token: str = ""
    """Static bearer token for MCP-level authentication (optional)."""

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

    def is_http(self) -> bool:
        return self.transport == "streamable-http"

    @property
    def api_url(self) -> str:
        """Full base URL for API requests (e.g. ``https://co.looker.com/api/4.0``)."""
        return f"{self.base_url}/api/{self.api_version}"
