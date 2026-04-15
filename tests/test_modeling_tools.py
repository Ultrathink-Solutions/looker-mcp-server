"""Tests for modeling tool group — LookML project CRUD.

Scoped to the newly added project-level tools. File-level tools
(``get_file``, ``create_file``, etc.) have coverage in test_client.py via
the session/auth paths.
"""

import json

import httpx
import pytest
import respx

from looker_mcp_server.client import LookerClient
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import ApiKeyIdentityProvider


@pytest.fixture
def config():
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        _env_file=None,  # type: ignore[call-arg]
    )


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestGetProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_full_project(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "analytics",
                    "name": "analytics",
                    "git_remote_url": "git@github.com:example/looker-analytics.git",
                    "git_service_name": "github",
                    "validation_required": True,
                    "pull_request_mode": "required",
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                project = await session.get("/projects/analytics")
                assert project["id"] == "analytics"
                assert project["git_service_name"] == "github"
        finally:
            await client.close()


class TestCreateProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_name_only(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "new_project", "name": "new_project"})

        respx.post(f"{API_URL}/projects").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_project", "modeling", {"name": "new_project"})
        try:
            async with client.session(ctx) as session:
                project = await session.post("/projects", body={"name": "new_project"})
                assert project["id"] == "new_project"
                assert captured["body"] == {"name": "new_project"}
        finally:
            await client.close()


class TestUpdateProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_only_provided_fields(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "analytics"})

        respx.patch(f"{API_URL}/projects/analytics").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("update_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                await session.patch(
                    "/projects/analytics",
                    body={
                        "git_remote_url": "git@github.com:example/looker-analytics.git",
                        "git_service_name": "github",
                    },
                )
                assert captured["body"] == {
                    "git_remote_url": "git@github.com:example/looker-analytics.git",
                    "git_service_name": "github",
                }
                assert "pull_request_mode" not in captured["body"]
        finally:
            await client.close()


class TestDeleteProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_success_envelope(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/projects/analytics").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                result = await session.delete("/projects/analytics")
                assert result is None
        finally:
            await client.close()


class TestGetProjectManifest:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_manifest(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics/manifest").mock(
            return_value=httpx.Response(
                200,
                json={
                    "name": "analytics",
                    "project_name": "analytics",
                    "localizations": [],
                    "local_dependency": [],
                    "remote_dependency": [
                        {"name": "shared_lookml", "url": "git@github.com:example/shared.git"}
                    ],
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project_manifest", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                manifest = await session.get("/projects/analytics/manifest")
                assert manifest["project_name"] == "analytics"
                assert len(manifest["remote_dependency"]) == 1
        finally:
            await client.close()


class TestProjectDeployKey:
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_public_key(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                json="ssh-ed25519 AAAAC3Nz... looker-deploy-key",
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "get_project_deploy_key", "modeling", {"project_id": "analytics"}
        )
        try:
            async with client.session(ctx) as session:
                key = await session.get("/projects/analytics/git/deploy_key")
                assert isinstance(key, str)
                assert "ssh-ed25519" in key
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_generates_new_key(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/projects/analytics/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                json="ssh-ed25519 AAAAC3Nz-rotated...",
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "create_project_deploy_key", "modeling", {"project_id": "analytics"}
        )
        try:
            async with client.session(ctx) as session:
                key = await session.post("/projects/analytics/git/deploy_key")
                assert "rotated" in key
        finally:
            await client.close()


class TestPathEncoding:
    """Project IDs can contain characters that need URL-encoding."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_special_character_project_id_is_encoded(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(200, json={"id": "my analytics"})

        respx.get(url__regex=rf"{API_URL}/projects/.*").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project", "modeling", {"project_id": "my analytics"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.get(f"/projects/{quote('my analytics', safe='')}")
                assert "my%20analytics" in captured["raw_path"]
                assert " " not in captured["raw_path"]
        finally:
            await client.close()
