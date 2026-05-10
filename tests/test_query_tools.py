"""Tests for query tool group — semantic-layer queries with dev_mode + branch."""

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


class TestQueryWithBranch:
    """``query(branch=…)`` is the canonical one-shot CI primitive: switch
    the dev workspace to the PR branch, run the query, restore the saved
    branch — all atomic. These tests pin the call sequence."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_arg_drives_save_swap_run_restore(self, config):
        _mock_login_logout()
        # dev_mode is auto-implied by branch=
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        # Save current branch ("main")
        get_branch = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        # PUT — captures both the swap and the restore
        put_branch = respx.put(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )
        respx.post(f"{API_URL}/queries").mock(return_value=httpx.Response(201, json={"id": "q1"}))
        respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[{"orders.region": "west"}])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "query",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region"],
                        "branch": "feature-x",
                        "project_id": "proj1",
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["row_count"] == 1
        finally:
            await looker_client.close()

        assert patch_session.called, "branch= should imply dev_mode and PATCH /session"
        assert get_branch.called, "must read current branch before swap"
        assert put_branch.call_count == 2, "swap + restore"
        bodies = [json.loads(c.request.content.decode()) for c in put_branch.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_without_project_id_returns_clean_validation_error(self, config):
        _mock_login_logout()

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "query",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region"],
                        "branch": "feature-x",
                        # No project_id — validation should fail-fast.
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                # ValueError is surfaced by format_api_error as a self-
                # describing error (no "Unexpected error" prefix).
                assert "branch=" in payload["error"]
                assert "project_id" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_dev_mode_without_branch_skips_swap_logic(self, config):
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        get_branch = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        respx.post(f"{API_URL}/queries").mock(return_value=httpx.Response(201, json={"id": "q1"}))
        respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[{"orders.region": "west"}])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "query",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region"],
                        "dev_mode": True,
                    },
                )
        finally:
            await looker_client.close()

        # PATCH /session yes (dev_mode=True), but no GET branch / no PUT —
        # the user's dev workspace's currently-checked-out branch is used
        # as-is. This is the "iterative human debug" flow.
        assert patch_session.called
        assert not get_branch.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_compiled_sql_from_text_plain_response(self, config):
        """``/queries/{id}/run/sql`` returns text/plain — must use get_text."""
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

        mcp, looker_client = create_server(config, enabled_groups={"query"})
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
            await looker_client.close()
