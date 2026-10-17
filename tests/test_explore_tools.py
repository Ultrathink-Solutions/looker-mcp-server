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
        # A field with no ``hidden`` key is not hidden; the flag is a boolean.
        assert [f["hidden"] for f in payload["filters"]] == [False, True]
        assert [p["name"] for p in payload["parameters"]] == ["orders.tz", "orders.debug"]
        assert [p["hidden"] for p in payload["parameters"]] == [False, True]


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


# An explore that joins a second view, with the SQL and descriptions Looker
# returns for every field. ``customers.count`` carries no ``view`` key, so the
# view filter has to fall back to the field-name prefix.
JOINED_EXPLORE_PAYLOAD = {
    "name": "orders",
    "label": "Orders",
    "description": "Order facts",
    "fields": {
        "dimensions": [
            {
                "name": "orders.region",
                "label": "Region",
                "type": "string",
                "description": "Sales region",
                "sql": "${TABLE}.region",
                "hidden": False,
                "view": "orders",
            },
            {
                "name": "customers.tier",
                "label": "Tier",
                "type": "string",
                "description": "Loyalty tier",
                "sql": "${TABLE}.tier",
                "hidden": False,
                "view": "customers",
            },
        ],
        "measures": [
            {
                "name": "orders.total_revenue",
                "label": "Total Revenue",
                "type": "sum",
                "description": "Net revenue",
                "sql": "${TABLE}.amount",
                "hidden": False,
                "view": "orders",
            },
            {
                "name": "customers.count",
                "label": "Customer Count",
                "type": "count",
                "description": "Distinct customers",
                "sql": None,
                "hidden": False,
            },
        ],
        "filters": [
            {
                "name": "customers.signup_window",
                "label": "Signup Window",
                "type": "date",
                "sql": "${TABLE}.signed_up_at",
                "view": "customers",
            },
        ],
        "parameters": [
            {
                "name": "orders.currency",
                "label": "Currency",
                "type": "string",
                "description": "Reporting currency",
                "view": "orders",
            },
        ],
    },
}


async def _call_raw(config, tool: str, args: dict) -> str:
    mcp, looker_client = create_server(config, enabled_groups={"explore"})
    try:
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(tool, args)
            content = result.content[0]
            assert isinstance(content, TextContent)
            return content.text
    finally:
        await looker_client.close()


def _all_fields(payload: dict) -> list[dict]:
    return [
        field
        for kind in ("dimensions", "measures", "filters", "parameters")
        for field in payload[kind]
    ]


class TestGetExploreConciseView:
    """``get_explore`` returns a compact, filterable view by default: no SQL,
    no pretty-printing, and ``fields`` / ``views`` narrow what comes back."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_explore_omits_sql_by_default(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=JOINED_EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config, "get_explore", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        fields = _all_fields(payload)
        assert {f["name"] for f in fields} == {
            "orders.region",
            "customers.tier",
            "orders.total_revenue",
            "customers.count",
            "customers.signup_window",
            "orders.currency",
        }
        assert all("sql" not in f for f in fields)
        # What a caller needs to build a query survives the concise view.
        assert all({"name", "label", "type"} <= f.keys() for f in fields)

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_explore_includes_sql_on_request(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=JOINED_EXPLORE_PAYLOAD)
        )
        payload = await _call(
            config,
            "get_explore",
            {"model_name": "ecommerce", "explore_name": "orders", "include_sql": True},
        )
        sql_by_name = {d["name"]: d.get("sql") for d in payload["dimensions"]}
        assert sql_by_name == {
            "orders.region": "${TABLE}.region",
            "customers.tier": "${TABLE}.tier",
        }

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_explore_filters_fields_and_views(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=JOINED_EXPLORE_PAYLOAD)
        )

        by_field = await _call(
            config,
            "get_explore",
            {
                "model_name": "ecommerce",
                "explore_name": "orders",
                "fields": ["orders.region", "customers.count", "orders.currency"],
            },
        )
        assert [d["name"] for d in by_field["dimensions"]] == ["orders.region"]
        assert [m["name"] for m in by_field["measures"]] == ["customers.count"]
        assert by_field["filters"] == []
        assert [p["name"] for p in by_field["parameters"]] == ["orders.currency"]

        by_view = await _call(
            config,
            "get_explore",
            {"model_name": "ecommerce", "explore_name": "orders", "views": ["customers"]},
        )
        assert [d["name"] for d in by_view["dimensions"]] == ["customers.tier"]
        assert [m["name"] for m in by_view["measures"]] == ["customers.count"]
        assert [f["name"] for f in by_view["filters"]] == ["customers.signup_window"]
        assert by_view["parameters"] == []

        both = await _call(
            config,
            "get_explore",
            {
                "model_name": "ecommerce",
                "explore_name": "orders",
                "fields": ["orders.region", "customers.tier"],
                "views": ["customers"],
            },
        )
        assert [d["name"] for d in both["dimensions"]] == ["customers.tier"]
        assert both["measures"] == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_explore_keeps_descriptions_unless_turned_off(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=JOINED_EXPLORE_PAYLOAD)
        )
        with_descriptions = await _call(
            config, "get_explore", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        assert {d["name"]: d.get("description") for d in with_descriptions["dimensions"]} == {
            "orders.region": "Sales region",
            "customers.tier": "Loyalty tier",
        }

        without = await _call(
            config,
            "get_explore",
            {"model_name": "ecommerce", "explore_name": "orders", "include_descriptions": False},
        )
        assert all("description" not in f for f in _all_fields(without))

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_explore_returns_compact_json(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/lookml_models/ecommerce/explores/orders").mock(
            return_value=httpx.Response(200, json=JOINED_EXPLORE_PAYLOAD)
        )
        text = await _call_raw(
            config, "get_explore", {"model_name": "ecommerce", "explore_name": "orders"}
        )
        assert "\n" not in text
        assert json.loads(text)["name"] == "orders"
