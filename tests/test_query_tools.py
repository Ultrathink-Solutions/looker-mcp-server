"""Tests for query tool group — semantic-layer queries and content search."""

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


class TestQuerySql:
    """Looker's ``/queries/{id}/run/sql`` endpoint returns ``text/plain`` —
    the compiled SQL as a raw string. Routing it through ``session.get``
    (which calls ``response.json()``) raises ``Expecting value: line 1
    column 1`` before the SQL can ever be returned. This test stops the
    regression by mocking the text/plain response and asserting the tool
    returns the SQL string under ``{"sql": ...}``."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_compiled_sql_from_text_plain_response(self, config):
        _mock_login_logout()

        compiled_sql = "SELECT region, SUM(total) FROM orders GROUP BY 1"
        respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(201, json={"id": "abc123"})
        )
        respx.get(f"{API_URL}/queries/abc123/run/sql").mock(
            return_value=httpx.Response(
                200,
                text=compiled_sql,
                headers={"content-type": "text/plain"},
            )
        )

        mcp, client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "query_sql",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region", "orders.total"],
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["sql"] == compiled_sql
        finally:
            await client.close()
