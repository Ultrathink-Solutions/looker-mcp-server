"""Tests for PR #13 admin-surface-completion additions.

Covers:
- admin group: update_schedule, run_schedule_once, get_role_groups, get_role_users
- modeling group: list_datagroups, reset_datagroup
- content group: validate_content
"""

import json

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent

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


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


# ══ Admin: schedules ═════════════════════════════════════════════════


class TestUpdateSchedule:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_only_provided_fields(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "42"})

        respx.patch(f"{API_URL}/scheduled_plans/42").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("update_schedule", "admin", {"schedule_id": "42"})
        try:
            async with client.session(ctx) as session:
                await session.patch(
                    "/scheduled_plans/42", body={"enabled": False, "crontab": "0 9 * * *"}
                )
                assert captured["body"] == {"enabled": False, "crontab": "0 9 * * *"}
                assert "name" not in captured["body"]
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_fields_returns_error(self, config):
        _mock_login_logout()
        # No PATCH mock — any outbound PATCH would raise.

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("update_schedule", {"schedule_id": "42"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["error"] == "No fields provided to update."
                patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
                assert patch_calls == []
        finally:
            await looker_client.close()


class TestRunScheduleOnce:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_run_once(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/scheduled_plans/42/run_once").mock(
            return_value=httpx.Response(200, json={"id": "run-99"})
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("run_schedule_once", "admin", {"schedule_id": "42"})
        try:
            async with client.session(ctx) as session:
                result = await session.post("/scheduled_plans/42/run_once")
                assert result["id"] == "run-99"
        finally:
            await client.close()


# ══ Admin: role membership readers ═══════════════════════════════════


class TestGetRoleGroups:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_trimmed_group_list(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/roles/5/groups").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": "1", "name": "data-team", "description": "..."},
                    {"id": "2", "name": "analysts", "description": "..."},
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_role_groups", "admin", {"role_id": "5"})
        try:
            async with client.session(ctx) as session:
                groups_list = await session.get("/roles/5/groups")
                assert len(groups_list) == 2
                assert groups_list[0]["name"] == "data-team"
        finally:
            await client.close()


class TestGetRoleUsers:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_trimmed_user_list(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/roles/5/users").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "42",
                        "email": "a@example.com",
                        "first_name": "Alice",
                        "last_name": "Example",
                        "is_disabled": False,
                    }
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_role_users", "admin", {"role_id": "5"})
        try:
            async with client.session(ctx) as session:
                users_list = await session.get("/roles/5/users")
                assert users_list[0]["email"] == "a@example.com"
        finally:
            await client.close()


# ══ Modeling: datagroups ═════════════════════════════════════════════


class TestDatagroups:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_summary(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/datagroups").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "ecommerce::daily",
                        "model_name": "ecommerce",
                        "name": "daily",
                        "trigger_check_at": 1700000000,
                        "triggered_at": 1700000000,
                        "stale_before": 0,
                    }
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_datagroups", "modeling")
        try:
            async with client.session(ctx) as session:
                datagroups = await session.get("/datagroups")
                assert datagroups[0]["id"] == "ecommerce::daily"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_reset_sends_current_timestamp(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "ecommerce::daily"})

        respx.patch(f"{API_URL}/datagroups/ecommerce::daily").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "reset_datagroup", {"datagroup_id": "ecommerce::daily"}
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["reset"] is True
                # stale_before must be a plausible unix timestamp (seconds
                # since epoch for "around now" — millisecond timestamps would
                # be 1000x larger and we don't want those).
                assert 1_700_000_000 < captured["body"]["stale_before"] < 10_000_000_000
        finally:
            await looker_client.close()


# ══ Content: validate_content ════════════════════════════════════════


class TestValidateContent:
    @pytest.mark.asyncio
    @respx.mock
    async def test_summarizes_errors_by_kind(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/content_validation").mock(
            return_value=httpx.Response(
                200,
                json={
                    "total_errors": 3,
                    "total_looks_validated": 50,
                    "total_dashboards_validated": 12,
                    "total_dashboard_elements_validated": 87,
                    "content_with_errors": [
                        {
                            "dashboard": {"id": "d1"},
                            "errors": [
                                {"kind": "missing_explore", "message": "..."},
                                {"kind": "missing_field", "message": "..."},
                            ],
                        },
                        {
                            "look": {"id": "l1"},
                            "errors": [{"kind": "missing_explore", "message": "..."}],
                        },
                    ],
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"content"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("validate_content", {})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                # Tool aggregates errors_by_kind from the nested shape.
                assert payload["errors_by_kind"]["missing_explore"] == 2
                assert payload["errors_by_kind"]["missing_field"] == 1
                assert payload["total_errors"] == 3
        finally:
            await looker_client.close()
