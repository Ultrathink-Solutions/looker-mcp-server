"""Tests for explore tool group — hidden-content filtering.

LookML ``hidden: yes`` is a curation signal: hidden explores and fields
are excluded from discovery tools by default, with a per-call
``include_hidden`` escape hatch.
"""

import json
from typing import Any

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


async def _call(config, tool: str, args: dict) -> Any:
    mcp, looker_client = create_server(config, enabled_groups={"explore"})
    try:
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(tool, args)
            content = result.content[0]
            assert isinstance(content, TextContent)
            return json.loads(content.text)
    finally:
        await looker_client.close()


MODELS_PAYLOAD = [
    {
        "name": "ecommerce",
        "label": "Ecommerce",
        "project_name": "proj1",
        "has_content": True,
        "explores": [
            {"name": "orders", "label": "Orders", "hidden": False},
            {"name": "staging_orders", "label": "Staging Orders", "hidden": True},
        ],
    }
]

EXPLORE_PAYLOAD = {
    "name": "orders",
    "label": "Orders",
    "description": "Order facts",
    "fields": {
        "dimensions": [
            {"name": "orders.region", "label": "Region", "type": "string", "hidden": False},
            {"name": "orders.pk", "label": "PK", "type": "number", "hidden": True},
        ],
        "measures": [
            {"name": "orders.count", "label": "Count", "type": "count", "hidden": False},
            {"name": "orders.raw_sum", "label": "Raw Sum", "type": "sum", "hidden": True},
        ],
        "filters": [
            {"name": "orders.date_filter", "label": "Date Filter", "type": "date"},
            {"name": "orders.secret_filter", "label": "Secret", "type": "string", "hidden": True},
        ],
        "parameters": [
            {"name": "orders.tz", "label": "Timezone", "type": "string"},
            {"name": "orders.debug", "label": "Debug", "type": "string", "hidden": True},
        ],
    },
}


class TestListModelsHiddenFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_hidden_explores_excluded_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models").mock(
            return_value=httpx.Response(200, json=MODELS_PAYLOAD)
        )
        payload = await _call(config, "list_models", {})
        explores = payload[0]["explores"]
        assert [e["name"] for e in explores] == ["orders"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_include_hidden_returns_all_with_flag(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models").mock(
            return_value=httpx.Response(200, json=MODELS_PAYLOAD)
        )
        payload = await _call(config, "list_models", {"include_hidden": True})
        explores = payload[0]["explores"]
        assert [e["name"] for e in explores] == ["orders", "staging_orders"]
        assert explores[1]["hidden"] is True


class TestGetModelHiddenFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_hidden_explores_excluded_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce").mock(
            return_value=httpx.Response(200, json=MODELS_PAYLOAD[0])
        )
        payload = await _call(config, "get_model", {"model_name": "ecommerce"})
        assert [e["name"] for e in payload["explores"]] == ["orders"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_include_hidden_returns_all(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce").mock(
            return_value=httpx.Response(200, json=MODELS_PAYLOAD[0])
        )
        payload = await _call(
            config, "get_model", {"model_name": "ecommerce", "include_hidden": True}
        )
        assert [e["name"] for e in payload["explores"]] == ["orders", "staging_orders"]


class TestGetExploreHiddenFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_hidden_fields_excluded_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config, "get_explore", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        assert [d["name"] for d in payload["dimensions"]] == ["orders.region"]
        assert [m["name"] for m in payload["measures"]] == ["orders.count"]
        assert [f["name"] for f in payload["filters"]] == ["orders.date_filter"]
        assert [p["name"] for p in payload["parameters"]] == ["orders.tz"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_include_hidden_returns_all_fields(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config,
            "get_explore",
            {"model_name": "ecommerce", "explore_name": "orders", "include_hidden": True},
        )
        assert [d["name"] for d in payload["dimensions"]] == ["orders.region", "orders.pk"]
        assert [m["name"] for m in payload["measures"]] == ["orders.count", "orders.raw_sum"]
        assert [f["name"] for f in payload["filters"]] == [
            "orders.date_filter",
            "orders.secret_filter",
        ]
        assert [f["hidden"] for f in payload["filters"]] == [None, True]
        assert [p["name"] for p in payload["parameters"]] == ["orders.tz", "orders.debug"]
        assert [p["hidden"] for p in payload["parameters"]] == [None, True]


class TestListDimensionsHiddenFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_hidden_dimensions_excluded_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config, "list_dimensions", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        assert [d["name"] for d in payload] == ["orders.region"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_include_hidden_returns_all(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config,
            "list_dimensions",
            {"model_name": "ecommerce", "explore_name": "orders", "include_hidden": True},
        )
        assert [d["name"] for d in payload] == ["orders.region", "orders.pk"]
        assert [d["hidden"] for d in payload] == [False, True]


class TestListMeasuresHiddenFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_hidden_measures_excluded_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config, "list_measures", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        assert [m["name"] for m in payload] == ["orders.count"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_include_hidden_returns_all(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config,
            "list_measures",
            {"model_name": "ecommerce", "explore_name": "orders", "include_hidden": True},
        )
        assert [m["name"] for m in payload] == ["orders.count", "orders.raw_sum"]
        assert [m["hidden"] for m in payload] == [False, True]
