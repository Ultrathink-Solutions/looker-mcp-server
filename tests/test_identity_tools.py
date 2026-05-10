"""Tests for the identity tool group — current-user introspection."""

import json

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.config import LookerConfig
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


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout() -> None:
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestWhoami:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_current_user_with_field_allow_list(self, config):
        _mock_login_logout()
        # Looker's response carries many fields; the tool's allow-list
        # surfaces a stable subset and ignores the rest. ``home_folder_id``
        # is in the response but should NOT appear in the tool result.
        respx.get(f"{API_URL}/user").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "42",
                    "display_name": "Test User",
                    "email": "test@example.com",
                    "first_name": "Test",
                    "last_name": "User",
                    "role_ids": ["1", "2"],
                    "group_ids": ["10"],
                    "verified_looker_employee": False,
                    "is_disabled": False,
                    "home_folder_id": "secret-internal-id",  # NOT in allow-list
                },
            )
        )

        mcp, client = create_server(config, enabled_groups={"identity"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("whoami", {})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["id"] == "42"
                assert payload["email"] == "test@example.com"
                assert payload["role_ids"] == ["1", "2"]
                # Field allow-list keeps the surface stable across Looker
                # API additions — new server-side fields don't auto-leak
                # into tool output until a maintainer adds them.
                assert "home_folder_id" not in payload
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_surfaces_api_error_via_format_api_error(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/user").mock(
            return_value=httpx.Response(401, json={"message": "Unauthorized"})
        )

        mcp, client = create_server(config, enabled_groups={"identity"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("whoami", {})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["status"] == 401
        finally:
            await client.close()
