"""Tests for board tool group — boards, sections, and items."""

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
    mcp, client = create_server(config, enabled_groups={"board"})
    return mcp, client


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    """Set up login/logout mocks for API-key auth sessions."""
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestListBoards:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_boards(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/boards/search").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "1",
                        "title": "My Board",
                        "description": "desc",
                        "created_at": "2025-01-01",
                    },
                    {
                        "id": "2",
                        "title": "Board 2",
                        "description": None,
                        "created_at": "2025-02-01",
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_boards", "board")

        try:
            async with client.session(ctx) as session:
                boards = await session.get("/boards/search", params={"limit": 50})
                assert len(boards) == 2
                assert boards[0]["title"] == "My Board"
        finally:
            await client.close()


class TestCreateBoard:
    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_board(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/boards").mock(
            return_value=httpx.Response(200, json={"id": "10", "title": "New Board"})
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_board", "board")

        try:
            async with client.session(ctx) as session:
                result = await session.post("/boards", body={"title": "New Board"})
                assert result["id"] == "10"
                assert result["title"] == "New Board"
        finally:
            await client.close()


class TestDeleteBoard:
    @pytest.mark.asyncio
    @respx.mock
    async def test_deletes_board(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/boards/5").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_board", "board", {"board_id": "5"})

        try:
            async with client.session(ctx) as session:
                result = await session.delete("/boards/5")
                assert result is None  # DELETE returns None
        finally:
            await client.close()


class TestBoardSections:
    @pytest.mark.asyncio
    @respx.mock
    async def test_create_section(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/board_sections").mock(
            return_value=httpx.Response(
                200, json={"id": "20", "title": "Section 1", "board_id": "1"}
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_board_section", "board", {"board_id": "1"})

        try:
            async with client.session(ctx) as session:
                result = await session.post(
                    "/board_sections", body={"board_id": "1", "title": "Section 1"}
                )
                assert result["id"] == "20"
                assert result["board_id"] == "1"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_section(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/board_sections/20").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_board_section", "board", {"board_section_id": "20"})

        try:
            async with client.session(ctx) as session:
                result = await session.delete("/board_sections/20")
                assert result is None
        finally:
            await client.close()


class TestBoardItems:
    @pytest.mark.asyncio
    @respx.mock
    async def test_create_item_with_dashboard(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/board_items").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "30",
                    "title": "Sales Dashboard",
                    "board_section_id": "20",
                    "dashboard_id": 42,
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_board_item", "board", {"board_section_id": "20"})

        try:
            async with client.session(ctx) as session:
                result = await session.post(
                    "/board_items",
                    body={"board_section_id": "20", "dashboard_id": 42, "title": "Sales Dashboard"},
                )
                assert result["id"] == "30"
                assert result["dashboard_id"] == 42
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_item(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/board_items/30").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_board_item", "board", {"board_item_id": "30"})

        try:
            async with client.session(ctx) as session:
                result = await session.delete("/board_items/30")
                assert result is None
        finally:
            await client.close()


class TestBoardToolRegistration:
    """Verify that board tools are registered when the group is enabled."""

    def test_board_tools_registered(self, server_and_client):
        mcp, _client = server_and_client
        tool_names = {t.name for t in mcp._tool_manager._tools.values()}
        expected = {
            "list_boards",
            "get_board",
            "create_board",
            "update_board",
            "delete_board",
            "get_board_section",
            "create_board_section",
            "update_board_section",
            "delete_board_section",
            "get_board_item",
            "create_board_item",
            "update_board_item",
            "delete_board_item",
        }
        assert expected.issubset(tool_names), f"Missing tools: {expected - tool_names}"
