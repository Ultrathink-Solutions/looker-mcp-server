"""Tests for the admin tool group — user search.

list_users previously hit GET /users, which has no email parameter.
Looker silently ignores unknown query params, so the filter vanished and
the caller got every user back with no way to tell "no match" from
"filter ignored".
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


class TestListUsersFiltering:
    @pytest.mark.asyncio
    @respx.mock
    async def test_email_filter_hits_search_endpoint(self, config):
        _mock_login_logout()
        search_route = respx.get(f"{API_URL}/users/search").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "2", "email": "dana@example.com", "first_name": "Dana"}],
            )
        )
        all_users_route = respx.get(f"{API_URL}/users").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_users", {"email": "%dana%"})

        assert search_route.called
        assert not all_users_route.called, "GET /users ignores the email filter"
        assert search_route.calls[0].request.url.params["email"] == "%dana%"

        payload = json.loads(_text(result))
        assert payload[0]["email"] == "dana@example.com"

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_match_returns_empty_list_not_every_user(self, config):
        """The reported failure mode: a non-matching filter returned the
        full user list, so the caller could not distinguish "no match" from
        "filter ignored"."""
        _mock_login_logout()
        respx.get(f"{API_URL}/users/search").mock(return_value=httpx.Response(200, json=[]))

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_users", {"email": "%nobody%"})

        assert json.loads(_text(result)) == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_name_filter_maps_to_full_name(self, config):
        _mock_login_logout()
        search_route = respx.get(f"{API_URL}/users/search").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool("list_users", {"name": "%dana%"})

        assert search_route.calls[0].request.url.params["full_name"] == "%dana%"

    @pytest.mark.asyncio
    @respx.mock
    async def test_unfiltered_call_still_uses_search_endpoint(self, config):
        """One code path, filtered or not — /users/search with no criteria
        returns all users, so there is no reason to keep a second branch."""
        _mock_login_logout()
        search_route = respx.get(f"{API_URL}/users/search").mock(
            return_value=httpx.Response(200, json=[{"id": "1", "email": "a@b.com"}])
        )

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool("list_users", {})

        assert search_route.called
        assert "email" not in search_route.calls[0].request.url.params
        assert len(json.loads(_text(result))) == 1

    @pytest.mark.asyncio
    @respx.mock
    async def test_limit_is_forwarded(self, config):
        _mock_login_logout()
        search_route = respx.get(f"{API_URL}/users/search").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool("list_users", {"limit": 25})

        assert search_route.calls[0].request.url.params["limit"] == "25"

    @pytest.mark.asyncio
    @respx.mock
    async def test_unwrapped_value_is_passed_through_verbatim(self, config):
        """Filter values are never auto-wrapped in '%'. A bare 'dana' must
        reach Looker as exactly 'dana', not '%dana%' — wrapping is the
        caller's decision, since Looker's LIKE semantics also give '_' a
        wildcard meaning that would silently over-match on a literal
        underscore if we tried to be "helpful" here."""
        _mock_login_logout()
        search_route = respx.get(f"{API_URL}/users/search").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, _ = create_server(config, enabled_groups={"admin"})
        async with Client(mcp) as mcp_client:
            await mcp_client.call_tool("list_users", {"email": "dana"})

        assert search_route.calls[0].request.url.params["email"] == "dana"
