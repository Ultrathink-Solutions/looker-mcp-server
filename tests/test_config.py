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
