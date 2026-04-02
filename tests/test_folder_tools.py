"""Tests for folder tool group — folder navigation and management."""

import httpx
import pytest
import respx

from looker_mcp_server.client import LookerClient
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import ApiKeyIdentityProvider
from looker_mcp_server.server import create_server


@pytest.fixture
def config():
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        _env_file=None,  # type: ignore[call-arg]
    )


@pytest.fixture
def server_and_client(config):
    mcp, client = create_server(config, enabled_groups={"folder"})
    return mcp, client


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    """Set up login/logout mocks for API-key auth sessions."""
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestListFolders:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_folders(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/folders/search").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "1",
                        "name": "Shared",
                        "parent_id": None,
                        "child_count": 3,
                        "dashboards": [{"id": "d1"}],
                        "looks": [],
                    },
                    {
                        "id": "2",
                        "name": "Users",
                        "parent_id": None,
                        "child_count": 10,
                        "dashboards": [],
                        "looks": [{"id": "l1"}, {"id": "l2"}],
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_folders", "folder")

        try:
            async with client.session(ctx) as session:
                folders = await session.get("/folders/search", params={"limit": 50})
                assert len(folders) == 2
                assert folders[0]["name"] == "Shared"
                assert folders[0]["child_count"] == 3
        finally:
            await client.close()


class TestCreateFolder:
    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_folder(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/folders").mock(
            return_value=httpx.Response(
                200, json={"id": "5", "name": "New Folder", "parent_id": "1"}
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_folder", "folder")

        try:
            async with client.session(ctx) as session:
                result = await session.post(
                    "/folders", body={"name": "New Folder", "parent_id": "1"}
                )
                assert result["id"] == "5"
                assert result["parent_id"] == "1"
        finally:
            await client.close()


class TestDeleteFolder:
    @pytest.mark.asyncio
    @respx.mock
    async def test_deletes_folder(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/folders/5").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_folder", "folder", {"folder_id": "5"})

        try:
            async with client.session(ctx) as session:
                result = await session.delete("/folders/5")
                assert result is None
        finally:
            await client.close()


class TestFolderChildren:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_children(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/folders/1/children").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": "10", "name": "Sub A", "parent_id": "1", "child_count": 0},
                    {"id": "11", "name": "Sub B", "parent_id": "1", "child_count": 2},
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_folder_children", "folder", {"folder_id": "1"})

        try:
            async with client.session(ctx) as session:
                children = await session.get("/folders/1/children", params={"limit": 50})
                assert len(children) == 2
                assert children[1]["name"] == "Sub B"
        finally:
            await client.close()


class TestFolderAncestors:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_ancestors(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/folders/10/ancestors").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": "0", "name": "Root", "parent_id": None},
                    {"id": "1", "name": "Shared", "parent_id": "0"},
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_folder_ancestors", "folder", {"folder_id": "10"})

        try:
            async with client.session(ctx) as session:
                ancestors = await session.get("/folders/10/ancestors")
                assert len(ancestors) == 2
                assert ancestors[0]["name"] == "Root"
        finally:
            await client.close()


class TestFolderContent:
    @pytest.mark.asyncio
    @respx.mock
    async def test_folder_looks(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/folders/1/looks").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "100", "title": "Sales Look", "description": "Monthly sales"}],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_folder_looks", "folder", {"folder_id": "1"})

        try:
            async with client.session(ctx) as session:
                looks = await session.get("/folders/1/looks")
                assert len(looks) == 1
                assert looks[0]["title"] == "Sales Look"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_folder_dashboards(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/folders/1/dashboards").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "200", "title": "Exec Dashboard", "description": None}],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_folder_dashboards", "folder", {"folder_id": "1"})

        try:
            async with client.session(ctx) as session:
                dashboards = await session.get("/folders/1/dashboards")
                assert len(dashboards) == 1
                assert dashboards[0]["title"] == "Exec Dashboard"
        finally:
            await client.close()


class TestFolderToolRegistration:
    """Verify that folder tools are registered when the group is enabled."""

    async def test_folder_tools_registered(self, server_and_client):
        mcp, _client = server_and_client
        tool_names = {t.name for t in await mcp.list_tools()}
        expected = {
            "list_folders",
            "get_folder",
            "create_folder",
            "update_folder",
            "delete_folder",
            "get_folder_children",
            "get_folder_ancestors",
            "get_folder_looks",
            "get_folder_dashboards",
        }
        assert expected.issubset(tool_names), f"Missing tools: {expected - tool_names}"
