"""Tests for the schema tool group — the query parameters sent to Looker."""

import httpx
import pytest
import respx
from fastmcp import Client

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


async def _list_columns_params(config, args: dict) -> dict[str, str]:
    route = respx.get(f"{API_URL}/connections/warehouse/columns").mock(
        return_value=httpx.Response(200, json=[])
    )
    mcp, looker_client = create_server(config, enabled_groups={"schema"})
    try:
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool("list_columns", {"connection_name": "warehouse", **args})
    finally:
        await looker_client.close()
    assert route.called
    return dict(route.calls.last.request.url.params)


class TestListColumnsParameters:
    @pytest.mark.asyncio
    @respx.mock
    async def test_table_name_is_sent_as_looker_table_names(self, config):
        _mock_login_logout()
        params = await _list_columns_params(
            config, {"schema_name": "public", "table_name": "orders"}
        )
        assert params == {"schema_name": "public", "table_names": "orders"}

    @pytest.mark.asyncio
    @respx.mock
    async def test_without_table_name_no_table_filter_is_sent(self, config):
        _mock_login_logout()
        params = await _list_columns_params(config, {"schema_name": "public"})
        assert params == {"schema_name": "public"}
