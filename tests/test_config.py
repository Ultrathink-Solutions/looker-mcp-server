"""Tests for LookerConfig."""

import os
from unittest.mock import patch

import pytest

from looker_mcp_server.config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig


class TestLookerConfig:
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            config = LookerConfig(
                _env_file=None,  # type: ignore[call-arg]
            )
        assert config.base_url == ""
        assert config.api_version == "4.0"
        assert config.transport == "stdio"
        assert config.port == 8080
        assert config.deployment_type == "self_hosted"
        assert config.sudo_as_user is True
        assert config.max_rows == 5000
        assert config.verify_ssl is True

    def test_env_vars(self):
        env = {
            "LOOKER_BASE_URL": "https://myco.looker.com/",
            "LOOKER_CLIENT_ID": "test-id",
            "LOOKER_CLIENT_SECRET": "test-secret",
            "LOOKER_DEPLOYMENT_TYPE": "google_cloud_core",
            "LOOKER_TRANSPORT": "streamable-http",
            "LOOKER_PORT": "9090",
        }
        with patch.dict(os.environ, env, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.base_url == "https://myco.looker.com"  # trailing slash stripped
        assert config.client_id == "test-id"
        assert config.client_secret == "test-secret"
        assert config.deployment_type == "google_cloud_core"
        assert config.transport == "streamable-http"
        assert config.port == 9090

    def test_api_url(self):
        with patch.dict(os.environ, {"LOOKER_BASE_URL": "https://co.looker.com"}, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.api_url == "https://co.looker.com/api/4.0"

    def test_is_http(self):
        with patch.dict(os.environ, {"LOOKER_TRANSPORT": "streamable-http"}, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.is_http() is True

        with patch.dict(os.environ, {"LOOKER_TRANSPORT": "stdio"}, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.is_http() is False

    def test_k8s_port_env_collision(self):
        """K8s injects LOOKER_PORT=tcp://10.x.x.x:8080 — should fall back to default."""
        env = {"LOOKER_PORT": "tcp://10.0.0.1:8080"}
        with patch.dict(os.environ, env, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.port == 8080

    def test_k8s_host_env_collision(self):
        """K8s injects LOOKER_HOST=tcp://... — should fall back to default."""
        env = {"LOOKER_HOST": "tcp://10.0.0.1:8080"}
        with patch.dict(os.environ, env, clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.host == "0.0.0.0"

    def test_invalid_deployment_type(self):
        with patch.dict(os.environ, {"LOOKER_DEPLOYMENT_TYPE": "invalid"}, clear=True):
            with pytest.raises(ValueError):
                LookerConfig(_env_file=None)  # type: ignore[call-arg]

    def test_invalid_transport(self):
        with patch.dict(os.environ, {"LOOKER_TRANSPORT": "grpc"}, clear=True):
            with pytest.raises(ValueError):
                LookerConfig(_env_file=None)  # type: ignore[call-arg]


class TestGroupConstants:
    def test_default_groups_are_subset_of_all(self):
        assert DEFAULT_GROUPS.issubset(ALL_GROUPS)

    def test_all_groups_includes_expected(self):
        expected = {
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
        assert ALL_GROUPS == expected

    def test_admin_and_git_not_in_defaults(self):
        assert "admin" not in DEFAULT_GROUPS
        assert "git" not in DEFAULT_GROUPS
        assert "modeling" not in DEFAULT_GROUPS


class TestMcpModePosture:
    """Tests for the MCP deployment-posture system — ``LOOKER_MCP_MODE`` enum
    plus ``public``-mode validation that enforces MCP 2025-11-25 MUSTs.

    ``DeploymentPostureError`` is a ``ValueError`` subclass raised inside a
    ``model_validator(mode="after")``, which Pydantic wraps in a
    ``ValidationError``.  We assert on the wrapped error's message (carries
    the posture-kind tag) and on ``ctx["error"]`` (which preserves the
    original typed exception object — useful for operator tooling).
    """

    def _env(self, **overrides: str) -> dict[str, str]:
        base = {"LOOKER_BASE_URL": "https://example.looker.com"}
        base.update(overrides)
        return base

    @staticmethod
    def _extract_posture_kind(exc_info):
        """Pull the :class:`PostureErrorKind` out of a Pydantic ValidationError.

        Pydantic wraps a ``ValueError`` raised inside a ``@model_validator``
        such that the original exception is accessible via
        ``exc.errors()[0]["ctx"]["error"]``.
        """
        from pydantic import ValidationError

        from looker_mcp_server.config import DeploymentPostureError

        err = exc_info.value
        assert isinstance(err, ValidationError), f"expected ValidationError, got {type(err)}"
        details = err.errors()
        assert len(details) == 1
        ctx = details[0].get("ctx", {})
        inner = ctx.get("error")
        assert isinstance(inner, DeploymentPostureError), (
            f"expected DeploymentPostureError inside ctx, got {type(inner)}"
        )
        return inner.kind

    def test_default_mode_is_dev(self):
        from looker_mcp_server.config import LookerMcpMode

        with patch.dict(os.environ, self._env(), clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_mode == LookerMcpMode.DEV

    def test_dev_mode_permissive_without_oidc(self):
        """``dev`` mode does not require OIDC fields — local iteration works."""
        with patch.dict(os.environ, self._env(LOOKER_MCP_MODE="dev"), clear=True):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_jwks_uri == ""
        assert config.mcp_issuer_url == ""

    def test_dev_mode_accepts_static_bearer_with_deprecation_warning(self):
        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="dev",
                LOOKER_MCP_AUTH_TOKEN="dev-bearer-secret",
            ),
            clear=True,
        ):
            with pytest.warns(DeprecationWarning, match="LOOKER_MCP_AUTH_TOKEN"):
                config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_auth_token == "dev-bearer-secret"

    def test_public_mode_rejects_static_bearer(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_AUTH_TOKEN="any-value",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI="https://looker.example.com/mcp",
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_STATIC_BEARER_FORBIDDEN

    def test_public_mode_requires_jwks_uri(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(LOOKER_MCP_MODE="public"),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_MISSING_JWKS_URI

    def test_public_mode_requires_issuer_url(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_MISSING_ISSUER_URL

    @pytest.mark.parametrize(
        "bad_jwks",
        [
            "http://as.example.com/.well-known/jwks.json",  # plaintext
            "as.example.com/.well-known/jwks.json",  # no scheme
            "https://",  # no host
            "   ",  # whitespace only
        ],
    )
    def test_public_mode_rejects_malformed_jwks_uri(self, bad_jwks: str):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(LOOKER_MCP_MODE="public", LOOKER_MCP_JWKS_URI=bad_jwks),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_MISSING_JWKS_URI

    @pytest.mark.parametrize(
        "bad_issuer",
        [
            "http://as.example.com",
            "as.example.com",
            "https://",
            "   ",
        ],
    )
    def test_public_mode_rejects_malformed_issuer_url(self, bad_issuer: str):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL=bad_issuer,
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_MISSING_ISSUER_URL

    def test_public_mode_trims_whitespace_on_jwks_and_issuer(self):
        """Surrounding whitespace on LOOKER_MCP_JWKS_URI / LOOKER_MCP_ISSUER_URL
        must be stripped BOTH during validation AND on the stored value, or
        downstream consumers (JWKSCache, PyJWT's exact-match iss check) break
        in subtle ways."""
        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="  https://as.example.com/.well-known/jwks.json  ",
                LOOKER_MCP_ISSUER_URL="\thttps://as.example.com\n",
                LOOKER_MCP_RESOURCE_URI="https://looker.example.com/mcp",
            ),
            clear=True,
        ):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_jwks_uri == "https://as.example.com/.well-known/jwks.json"
        assert config.mcp_issuer_url == "https://as.example.com"

    def test_resource_uri_trims_whitespace_and_trailing_slash(self):
        """Resource URI normalization applies in every mode (not just
        public), since dev-mode deployments also compare the audience
        claim downstream. Field validator runs before the mode-specific
        posture check, so whitespace + trailing slash both disappear."""
        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI=" https://looker.example.com/mcp/ ",
            ),
            clear=True,
        ):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_resource_uri == "https://looker.example.com/mcp"

    def test_public_mode_requires_resource_uri(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_MISSING_RESOURCE_URI

    def test_public_mode_rejects_http_resource_uri(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI="http://looker.example.com/mcp",
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_RESOURCE_URI_NOT_HTTPS

    def test_public_mode_rejects_resource_uri_with_fragment(self):
        from pydantic import ValidationError

        from looker_mcp_server.config import PostureErrorKind

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI="https://looker.example.com/mcp#frag",
            ),
            clear=True,
        ):
            with pytest.raises(ValidationError) as exc:
                LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert self._extract_posture_kind(exc) == PostureErrorKind.PUBLIC_RESOURCE_URI_MALFORMED

    def test_public_mode_strips_resource_uri_trailing_slash(self):
        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI="https://looker.example.com/mcp/",
            ),
            clear=True,
        ):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_resource_uri == "https://looker.example.com/mcp"

    def test_public_mode_happy_path(self):
        """All required env set + https resource URI → config resolves cleanly."""
        from looker_mcp_server.config import LookerMcpMode

        with patch.dict(
            os.environ,
            self._env(
                LOOKER_MCP_MODE="public",
                LOOKER_MCP_JWKS_URI="https://as.example.com/.well-known/jwks.json",
                LOOKER_MCP_ISSUER_URL="https://as.example.com",
                LOOKER_MCP_RESOURCE_URI="https://looker.example.com/mcp",
            ),
            clear=True,
        ):
            config = LookerConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.mcp_mode == LookerMcpMode.PUBLIC
        assert config.mcp_jwks_uri.startswith("https://")
        assert config.mcp_resource_uri == "https://looker.example.com/mcp"

    def test_posture_error_carries_kind_and_message(self):
        from looker_mcp_server.config import DeploymentPostureError, PostureErrorKind

        err = DeploymentPostureError(PostureErrorKind.PUBLIC_MISSING_JWKS_URI, "detail")
        assert err.kind == PostureErrorKind.PUBLIC_MISSING_JWKS_URI
        assert "public_missing_jwks_uri" in str(err)
        assert "detail" in str(err)
        # Backwards-compat with Pydantic validators expecting ValueError.
        assert isinstance(err, ValueError)
