"""Tests for the content tool group — Look and dashboard CRUD.

The load-bearing assertion in this file is CALL ORDERING. ``create_look``
must materialize a Query (POST /queries) BEFORE saving the Look (POST
/looks), and the Look body must carry ``query_id`` rather than an inline
``query``. A test that only asserts a 200 would not catch the bug this
file exists to prevent.
"""

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


def _text(result) -> str:
    content = result.content[0]
    assert isinstance(content, TextContent)
    return content.text


class TestCreateLookMaterializesQuery:
    """Looker's POST /looks requires a query_id; an inline query 422s with
    {"field": "query_id", "code": "missing"}. These tests pin the two-step."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_query_before_look_and_sends_query_id(self, config):
        _mock_login_logout()
        create_query_route = respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(200, json={"id": "q42"})
        )
        create_look_route = respx.post(f"{API_URL}/looks").mock(
            return_value=httpx.Response(
                200,
                json={"id": "17", "title": "Revenue by Region", "url": "/looks/17"},
            )
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(
                "create_look",
                {
                    "title": "Revenue by Region",
                    "model": "ecommerce",
                    "view": "orders",
                    "fields": ["orders.region", "orders.total_revenue"],
                    "folder_id": "42",
                },
            )

        assert create_query_route.called
        assert create_look_route.called

        # Ordering: the Query must exist before the Look references it.
        paths = [c.request.url.path for c in respx.calls]
        assert paths.index("/api/4.0/queries") < paths.index("/api/4.0/looks")

        look_body = json.loads(create_look_route.calls[0].request.content)
        assert look_body["query_id"] == "q42"
        assert "query" not in look_body, "inline query is exactly what 422s"
        assert look_body["title"] == "Revenue by Region"
        assert look_body["folder_id"] == "42"

        payload = json.loads(_text(result))
        assert payload["id"] == "17"
        assert payload["query_id"] == "q42"

    @pytest.mark.asyncio
    @respx.mock
    async def test_query_body_carries_the_tool_inputs(self, config):
        _mock_login_logout()
        create_query_route = respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(200, json={"id": "q1"})
        )
        respx.post(f"{API_URL}/looks").mock(
            return_value=httpx.Response(200, json={"id": "1", "title": "t"})
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool(
                "create_look",
                {
                    "title": "t",
                    "model": "ecommerce",
                    "view": "orders",
                    "fields": ["orders.region"],
                    "folder_id": "1",
                    "filters": {"orders.created_date": "90 days"},
                    "sorts": ["orders.region desc"],
                    "limit": 100,
                },
            )

        query_body = json.loads(create_query_route.calls[0].request.content)
        assert query_body["model"] == "ecommerce"
        assert query_body["view"] == "orders"
        assert query_body["fields"] == ["orders.region"]
        assert query_body["filters"] == {"orders.created_date": "90 days"}
        assert query_body["sorts"] == ["orders.region desc"]
        assert query_body["limit"] == "100"

    @pytest.mark.asyncio
    @respx.mock
    async def test_description_reaches_the_look_body(self, config):
        """Reported as a missing parameter; it was always in the signature —
        the call just 422'd before the body ever shipped."""
        _mock_login_logout()
        respx.post(f"{API_URL}/queries").mock(return_value=httpx.Response(200, json={"id": "q1"}))
        create_look_route = respx.post(f"{API_URL}/looks").mock(
            return_value=httpx.Response(200, json={"id": "1", "title": "t"})
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool(
                "create_look",
                {
                    "title": "t",
                    "model": "m",
                    "view": "v",
                    "fields": ["f"],
                    "folder_id": "1",
                    "description": "Weekly revenue tracker",
                },
            )

        look_body = json.loads(create_look_route.calls[0].request.content)
        assert look_body["description"] == "Weekly revenue tracker"

    @pytest.mark.asyncio
    @respx.mock
    async def test_vis_config_and_pivots_reach_the_query_body(self, config):
        """Anything beyond a flat table used to require the raw SDK."""
        _mock_login_logout()
        create_query_route = respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(200, json={"id": "q1"})
        )
        respx.post(f"{API_URL}/looks").mock(
            return_value=httpx.Response(200, json={"id": "1", "title": "t"})
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool(
                "create_look",
                {
                    "title": "t",
                    "model": "m",
                    "view": "v",
                    "fields": ["f"],
                    "folder_id": "1",
                    "pivots": ["v.status"],
                    "vis_config": {"type": "looker_grid"},
                    "query_timezone": "America/Chicago",
                    "row_total": "right",
                    "total": True,
                },
            )

        query_body = json.loads(create_query_route.calls[0].request.content)
        assert query_body["pivots"] == ["v.status"]
        assert query_body["vis_config"] == {"type": "looker_grid"}
        assert query_body["query_timezone"] == "America/Chicago"
        assert query_body["row_total"] == "right"
        assert query_body["total"] is True

    @pytest.mark.asyncio
    @respx.mock
    async def test_look_is_not_created_when_query_creation_fails(self, config):
        """A failed materialization must abort, not fall through to a
        POST /looks that would 422 with a misleading message."""
        _mock_login_logout()
        respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(400, json={"message": "invalid field"})
        )
        create_look_route = respx.post(f"{API_URL}/looks").mock(
            return_value=httpx.Response(200, json={"id": "1"})
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(
                "create_look",
                {
                    "title": "t",
                    "model": "m",
                    "view": "v",
                    "fields": ["bad.field"],
                    "folder_id": "1",
                },
            )

        assert not create_look_route.called
        payload = json.loads(_text(result))
        assert payload["status"] == 400


class TestLookmlDashboardDiscovery:
    """/dashboards/search covers user-defined dashboards only — the Looker
    spec says so explicitly. LookML dashboards need their own endpoint, and
    without it the only way to find a model::slug id is grepping the LookML
    repo."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_uses_the_lookml_search_endpoint(self, config):
        _mock_login_logout()
        route = respx.get(f"{API_URL}/dashboards/lookml/search").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "analytics::executive_overview",
                        "title": "Executive Overview",
                        "folder_id": "9",
                    }
                ],
            )
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_lookml_dashboards", {"title": "%Executive%"})

        assert route.called
        assert route.calls[0].request.url.params["title"] == "%Executive%"

        payload = json.loads(_text(result))
        assert payload[0]["id"] == "analytics::executive_overview"
        assert payload[0]["title"] == "Executive Overview"

    @pytest.mark.asyncio
    @respx.mock
    async def test_handles_a_dict_wrapped_response(self, config):
        """The generated SDK types this endpoint's response as a singular
        DashboardLookml even though it returns a collection. Tolerate both
        rather than trusting the annotation."""
        _mock_login_logout()
        respx.get(f"{API_URL}/dashboards/lookml/search").mock(
            return_value=httpx.Response(200, json={"id": "m::only_one", "title": "Only One"})
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_lookml_dashboards", {})

        payload = json.loads(_text(result))
        assert payload[0]["id"] == "m::only_one"

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_result_is_an_empty_list(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/dashboards/lookml/search").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, _ = create_server(config, enabled_groups={"content"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_lookml_dashboards", {})

        assert json.loads(_text(result)) == []


class TestAddDashboardFilter:
    """``add_dashboard_filter`` sends one of Looker's filter types to
    ``POST /dashboard_filters``, with the model and explore a field filter needs."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_field_filter_posts_type_model_explore_and_dimension(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f1", "title": "Region"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "add_dashboard_filter",
                    {
                        "dashboard_id": "17",
                        "title": "Region",
                        "dimension": "orders.region",
                        "model": "ecommerce",
                        "explore": "orders",
                    },
                )
                payload = json.loads(_text(result))
        finally:
            await looker_client.close()

        assert payload == {"id": "f1", "name": "region", "title": "Region"}
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "dashboard_id": "17",
            "name": "region",
            "title": "Region",
            "dimension": "orders.region",
            "type": "field",
            "model": "ecommerce",
            "explore": "orders",
        }

    @pytest.mark.asyncio
    @respx.mock
    async def test_field_filter_without_a_dimension_fails_before_any_request(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f1"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "add_dashboard_filter",
                    {
                        "dashboard_id": "17",
                        "title": "Region",
                        "model": "ecommerce",
                        "explore": "orders",
                    },
                )
                payload = json.loads(_text(result))
        finally:
            await looker_client.close()

        assert "dimension" in payload["error"]
        assert not route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_field_filter_without_model_and_explore_fails_before_any_request(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f1"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "add_dashboard_filter",
                    {"dashboard_id": "17", "title": "Region", "dimension": "orders.region"},
                )
                payload = json.loads(_text(result))
        finally:
            await looker_client.close()

        assert "model" in payload["error"]
        assert not route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_date_filter_needs_no_dimension_model_or_explore(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f2", "title": "Date"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "add_dashboard_filter",
                    {
                        "dashboard_id": "17",
                        "title": "Date",
                        "type": "date",
                        "default_value": "90 days",
                    },
                )
        finally:
            await looker_client.close()

        body = json.loads(route.calls.last.request.content)
        assert body == {
            "dashboard_id": "17",
            "name": "date",
            "title": "Date",
            "type": "date",
            "default_value": "90 days",
        }

    @pytest.mark.asyncio
    @respx.mock
    async def test_filter_name_is_sent_as_given_or_derived_from_the_title(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f3", "title": "Order Date (UTC)"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                derived = json.loads(
                    _text(
                        await mcp_client.call_tool(
                            "add_dashboard_filter",
                            {"dashboard_id": "17", "title": "Order Date (UTC)", "type": "date"},
                        )
                    )
                )
                derived_body = json.loads(route.calls.last.request.content)
                given = json.loads(
                    _text(
                        await mcp_client.call_tool(
                            "add_dashboard_filter",
                            {
                                "dashboard_id": "17",
                                "title": "Order Date (UTC)",
                                "name": "order_window",
                                "type": "date",
                            },
                        )
                    )
                )
                given_body = json.loads(route.calls.last.request.content)
        finally:
            await looker_client.close()

        assert derived_body["name"] == "order_date_utc"
        assert derived["name"] == "order_date_utc"
        assert given_body["name"] == "order_window"
        assert given["name"] == "order_window"

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_filter_type_is_refused(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_filters").mock(
            return_value=httpx.Response(200, json={"id": "f1"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "add_dashboard_filter",
                    {
                        "dashboard_id": "17",
                        "title": "Region",
                        "dimension": "orders.region",
                        "type": "field_filter",
                    },
                    raise_on_error=False,
                )
        finally:
            await looker_client.close()

        assert result.is_error
        assert not route.called


class TestAddDashboardElement:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_to_dashboard_elements_with_the_dashboard_in_the_body(self, config):
        _mock_login_logout()
        route = respx.post(f"{API_URL}/dashboard_elements").mock(
            return_value=httpx.Response(200, json={"id": "e1", "title": "Revenue"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "add_dashboard_element",
                    {"dashboard_id": "17", "title": "Revenue", "look_id": "42"},
                )
                payload = json.loads(_text(result))
        finally:
            await looker_client.close()

        assert payload == {"id": "e1", "title": "Revenue"}
        request = route.calls.last.request
        assert request.method == "POST"
        assert request.url.path == "/api/4.0/dashboard_elements"
        body = json.loads(request.content)
        assert body["dashboard_id"] == "17"
        assert body["look_id"] == "42"
