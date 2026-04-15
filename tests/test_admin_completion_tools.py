"""Tests for PR #13 admin-surface-completion additions.

All tests exercise the MCP tool layer via ``fastmcp.Client(mcp).call_tool``
so tool registration, argument mapping, and response shaping are under
test — not just the underlying HTTP behavior.

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


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


def _invoke_tool(mcp, tool_name: str, args: dict):
    """Call a tool through the MCP server and return the parsed payload."""

    async def _run():
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(tool_name, args)
            content = result.content[0]
            assert isinstance(content, TextContent)
            return json.loads(content.text)

    return _run


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

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_schedule",
                {"schedule_id": "42", "enabled": False, "crontab": "0 9 * * *"},
            )()
            assert payload["updated"] is True
            assert payload["id"] == "42"
            assert set(payload["fields_changed"]) == {"enabled", "crontab"}
            # Only the provided fields went on the wire — _set_if correctly
            # filtered out the None-valued optional args.
            assert captured["body"] == {"enabled": False, "crontab": "0 9 * * *"}
            assert "name" not in captured["body"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_fields_returns_error(self, config):
        _mock_login_logout()
        # No PATCH mock — any outbound PATCH would raise.

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "update_schedule", {"schedule_id": "42"})()
            assert payload["error"] == "No fields provided to update."
            assert "hint" in payload
            patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
            assert patch_calls == []
        finally:
            await looker_client.close()


class TestRunScheduleOnce:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_run_once_and_shapes_response(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/scheduled_plans/42/run_once").mock(
            return_value=httpx.Response(200, json={"id": "run-99"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "run_schedule_once", {"schedule_id": "42"})()
            assert payload["triggered"] is True
            assert payload["schedule_id"] == "42"
            assert payload["id"] == "run-99"
            # The tool's next_step field guides the caller to the audit group.
            assert "get_schedule_history" in payload["next_step"]
        finally:
            await looker_client.close()


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
                    {
                        "id": "1",
                        "name": "data-team",
                        "description": "...",  # not in trimmed output
                        "external_group_id": "okta-123",  # not in trimmed output
                    },
                    {
                        "id": "2",
                        "name": "analysts",
                        "description": "...",
                        "external_group_id": None,
                    },
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "get_role_groups", {"role_id": "5"})()
            assert len(payload) == 2
            # The tool trims to just id + name.
            assert payload[0] == {"id": "1", "name": "data-team"}
            assert payload[1] == {"id": "2", "name": "analysts"}
            # Extra fields from the raw API response are discarded.
            for row in payload:
                assert "description" not in row
                assert "external_group_id" not in row
        finally:
            await looker_client.close()


class TestGetRoleUsers:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_trimmed_user_summaries(self, config):
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
                        "credentials_email": {"email": "a@example.com"},  # trimmed out
                        "role_ids": [5, 6, 7],  # trimmed out
                    }
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "get_role_users", {"role_id": "5"})()
            assert len(payload) == 1
            row = payload[0]
            assert row == {
                "id": "42",
                "email": "a@example.com",
                "first_name": "Alice",
                "last_name": "Example",
                "is_disabled": False,
            }
            # Nested / verbose fields are not forwarded.
            assert "credentials_email" not in row
            assert "role_ids" not in row
        finally:
            await looker_client.close()


# ══ Modeling: datagroups ═════════════════════════════════════════════


class TestDatagroups:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_trimmed_summary(self, config):
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
                        "trigger_value": "select max(updated_at)",  # trimmed out
                    }
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            payload = await _invoke_tool(mcp, "list_datagroups", {})()
            assert len(payload) == 1
            row = payload[0]
            assert row["id"] == "ecommerce::daily"
            assert row["model_name"] == "ecommerce"
            assert row["stale_before"] == 0
            # Large / unused fields don't leak through.
            assert "trigger_value" not in row
        finally:
            await looker_client.close()

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
            payload = await _invoke_tool(
                mcp, "reset_datagroup", {"datagroup_id": "ecommerce::daily"}
            )()
            assert payload["reset"] is True
            # stale_before must be a plausible unix timestamp in seconds,
            # not milliseconds (which would be 1000x larger).
            assert 1_700_000_000 < captured["body"]["stale_before"] < 10_000_000_000
            assert captured["body"]["stale_before"] == payload["stale_before"]
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
            payload = await _invoke_tool(mcp, "validate_content", {})()
            # Tool aggregates errors_by_kind from the nested shape.
            assert payload["errors_by_kind"]["missing_explore"] == 2
            assert payload["errors_by_kind"]["missing_field"] == 1
            assert payload["total_errors"] == 3
            assert payload["total_dashboards_validated"] == 12
            # Broken content rows are preserved for deep inspection.
            assert len(payload["broken_content"]) == 2
        finally:
            await looker_client.close()
