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


WRITABLE_SCHEDULE_FIELDS = {
    "name",
    "user_id",
    "run_as_recipient",
    "enabled",
    "look_id",
    "dashboard_id",
    "lookml_dashboard_id",
    "filters_string",
    "require_results",
    "require_no_results",
    "require_change",
    "send_all_results",
    "crontab",
    "datagroup",
    "timezone",
    "run_once",
    "include_links",
    "custom_url_base",
    "custom_url_params",
    "custom_url_label",
    "show_custom_url",
    "pdf_paper_size",
    "pdf_landscape",
    "embed",
    "color_theme",
    "long_tables",
    "inline_table_width",
    "query_id",
    # Destinations are exposed as two flat params (recipients + destinations),
    # not a single nested array — keeps the tool surface ergonomic.
    "recipients",
    "destinations",
}


class TestCreateScheduleSurface:
    """The full WriteScheduledPlan surface must be reachable on create_schedule
    so agents can configure conditional delivery, datagroup triggers,
    custom URLs, PDF rendering, and non-email destinations end-to-end.
    """

    @pytest.mark.asyncio
    async def test_create_schedule_exposes_all_writable_fields(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["create_schedule"].parameters["properties"]
            missing = WRITABLE_SCHEDULE_FIELDS - props.keys()
            assert not missing, f"create_schedule missing writable fields: {sorted(missing)}"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_update_schedule_exposes_all_writable_fields(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["update_schedule"].parameters["properties"]
            missing = WRITABLE_SCHEDULE_FIELDS - props.keys()
            assert not missing, f"update_schedule missing writable fields: {sorted(missing)}"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_recipients_shorthand_builds_email_destinations(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "1", "name": "weekly"})

        respx.post(f"{API_URL}/scheduled_plans").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "weekly",
                    "crontab": "0 9 * * 1",
                    "dashboard_id": "42",
                    "recipients": ["a@x.com", "b@x.com"],
                },
            )()
            assert captured["body"]["scheduled_plan_destination"] == [
                # Recipients shorthand always sets format — Looker rejects
                # destinations without one. Default is ``wysiwyg_pdf`` (Looker
                # UI's default for dashboard email schedules).
                {"type": "email", "address": "a@x.com", "format": "wysiwyg_pdf"},
                {"type": "email", "address": "b@x.com", "format": "wysiwyg_pdf"},
            ]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_email_format_override_for_recipients_shorthand(self, config):
        # Common scenario: emailing a Look's data as CSV instead of the
        # default PDF. The email_format param overrides per-call so callers
        # don't have to fall back to the full destinations shape.
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "1", "name": "weekly-csv"})

        respx.post(f"{API_URL}/scheduled_plans").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "weekly-csv",
                    "crontab": "0 9 * * 1",
                    "look_id": "42",
                    "recipients": ["a@x.com"],
                    "email_format": "csv",
                },
            )()
            assert captured["body"]["scheduled_plan_destination"] == [
                {"type": "email", "address": "a@x.com", "format": "csv"},
            ]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_destinations_passes_through_full_shape(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "1", "name": "to-s3"})

        respx.post(f"{API_URL}/scheduled_plans").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            destinations = [
                {
                    "type": "s3",
                    "format": "csv",
                    "address": "my-bucket/exports",
                    "parameters": '{"region": "us-east-1"}',
                    "secret_parameters": '{"access_key_id": "AKIA...", "secret_access_key": "..."}',
                }
            ]
            await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "to-s3",
                    "crontab": "0 0 * * *",
                    "query_id": "Q123",
                    "destinations": destinations,
                },
            )()
            # Destinations are passed through unchanged so non-email types work.
            assert captured["body"]["scheduled_plan_destination"] == destinations
            # query_id is the target — look_id/dashboard_id absent.
            assert captured["body"]["query_id"] == "Q123"
            assert "look_id" not in captured["body"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_recipients_and_destinations_are_mutually_exclusive(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            # No outbound HTTP mock — any leak would raise.
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "crontab": "0 9 * * *",
                    "dashboard_id": "42",
                    "recipients": ["a@x.com"],
                    "destinations": [{"type": "email", "address": "b@x.com"}],
                },
            )()
            assert "not both" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_no_target_returns_actionable_error(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {"name": "x", "crontab": "0 9 * * *"},
            )()
            assert "No target" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_multiple_targets_returns_actionable_error(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "crontab": "0 9 * * *",
                    "look_id": "1",
                    "dashboard_id": "2",
                },
            )()
            assert "Multiple targets" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_advanced_fields_round_trip_through_body(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "9"})

        respx.post(f"{API_URL}/scheduled_plans").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "advanced",
                    "lookml_dashboard_id": "ml-99",
                    "datagroup": "daily_etl",
                    "timezone": "America/New_York",
                    "require_change": True,
                    "send_all_results": True,
                    "filters_string": "f[date]=last 7 days",
                    "pdf_landscape": True,
                    "pdf_paper_size": "letter",
                    "long_tables": True,
                    "color_theme": "dark",
                    "show_custom_url": True,
                    "custom_url_base": "https://reports.example.com",
                    "custom_url_label": "View in Reports",
                    "destinations": [{"type": "email", "address": "ops@x.com"}],
                },
            )()
            body = captured["body"]
            # Key conditional / trigger fields make it on the wire.
            assert body["datagroup"] == "daily_etl"
            assert body["require_change"] is True
            assert body["send_all_results"] is True
            assert body["timezone"] == "America/New_York"
            assert body["pdf_landscape"] is True
            assert body["color_theme"] == "dark"
            assert body["custom_url_base"] == "https://reports.example.com"
            # crontab was NOT provided — must NOT be sent (would conflict with datagroup).
            assert "crontab" not in body
        finally:
            await looker_client.close()


class TestUpdateScheduleAdvanced:
    @pytest.mark.asyncio
    @respx.mock
    async def test_update_rejects_empty_destinations(self, config):
        # Looker requires every ScheduledPlan to always have at least one
        # destination — an empty array is rejected with a 422. The tool
        # catches this preflight and returns an actionable error instead.
        # (Original test expected pass-through, which CodeRabbit's spec
        # research showed is wrong — Looker rejects the request.)
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_schedule",
                {"schedule_id": "42", "destinations": []},
            )()
            assert "Empty destination list is not allowed" in payload["error"]
            assert list(respx.calls) == [], (
                "empty-destinations path opened a Looker session — should short-circuit preflight"
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_rejects_multiple_targets(self, config):
        # A schedule has exactly one source (look / dashboard / lookml /
        # query). Retargeting to multiple at once is ambiguous and would
        # 422 at Looker. Mirror create_schedule's at-most-one-target guard.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_schedule",
                {"schedule_id": "42", "look_id": "1", "dashboard_id": "2"},
            )()
            assert "Multiple targets" in payload["error"]
            assert list(respx.calls) == []
        finally:
            await looker_client.close()


class TestScheduleTriggerValidation:
    """`crontab` and `datagroup` are mutually exclusive trigger modes per the
    WriteScheduledPlan spec — Looker rejects requests that set both. The tool
    catches this up front so callers see an actionable error attributing the
    failure to the offending parameters, not a Looker 422.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_rejects_both_crontab_and_datagroup(self, config):
        # No login or HTTP mocks of any kind — the preflight guard must
        # short-circuit before opening a Looker session, so the entire
        # respx.calls log should stay empty.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "dashboard_id": "1",
                    "crontab": "0 9 * * *",
                    "datagroup": "daily_etl",
                },
            )()
            assert "mutually exclusive" in payload["error"]
            assert list(respx.calls) == [], (
                "guard fired but a Looker session was still opened — "
                "validation should run before client.session()"
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_rejects_both_crontab_and_datagroup(self, config):
        # Same zero-HTTP invariant as the create-side test above.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_schedule",
                {"schedule_id": "42", "crontab": "0 9 * * *", "datagroup": "daily_etl"},
            )()
            assert "mutually exclusive" in payload["error"]
            assert list(respx.calls) == [], (
                "guard fired but a Looker session was still opened — "
                "validation should run before client.session()"
            )
        finally:
            await looker_client.close()


class TestCreateScheduleEmptyListSemantics:
    """`recipients=[]` and `destinations=[]` must be treated as "the caller
    explicitly cleared the destination list," not as "the caller omitted
    these arguments." Truthy checks would silently let recipients=[]
    bypass the mutual-exclusion guard.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_recipients_and_destinations_both_provided_is_rejected(self, config):
        _mock_login_logout()

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            # With truthy checks, empty lists fall through and BOTH are
            # accepted. With `is not None`, this is a mutual-exclusion error.
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "crontab": "0 9 * * *",
                    "dashboard_id": "1",
                    "recipients": [],
                    "destinations": [],
                },
            )()
            assert "not both" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_with_empty_destinations_is_rejected(self, config):
        # A schedule with no destinations cannot deliver. Reject up front
        # rather than letting Looker 422 with a less actionable message.
        # (On UPDATE, destinations=[] stays valid — that's how you clear
        # a previously-set destinations list.)
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "crontab": "0 9 * * *",
                    "dashboard_id": "1",
                    "destinations": [],
                },
            )()
            assert "No destinations" in payload["error"]
            # Short-circuited before any HTTP — proves the guard runs
            # preflight rather than after burning a login round-trip.
            assert list(respx.calls) == []
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_with_no_destinations_or_recipients_is_rejected(self, config):
        # The both-None case (omitting recipients AND destinations) is
        # equivalent to passing an empty list — same rejection path.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {"name": "x", "crontab": "0 9 * * *", "dashboard_id": "1"},
            )()
            assert "No destinations" in payload["error"]
            assert list(respx.calls) == []
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_with_no_trigger_is_rejected(self, config):
        # A schedule with neither crontab nor datagroup has no way to
        # fire. Reject up front.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "create_schedule",
                {
                    "name": "x",
                    "dashboard_id": "1",
                    "recipients": ["a@x.com"],
                },
            )()
            assert "No trigger" in payload["error"]
            assert list(respx.calls) == []
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


# ══ Admin: user/group completeness ═══════════════════════════════════


class TestUserSurface:
    @pytest.mark.asyncio
    async def test_create_user_exposes_full_writable_surface(self, config):
        # User schema writable fields: first_name, last_name, email,
        # role_ids, group_ids, is_disabled, home_folder_id, locale,
        # ui_state, models_dir_validated, can_manage_api3_creds.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            schema = tools["create_user"].parameters
            props = schema["properties"]
            required = set(schema.get("required", []))
            for f in (
                "first_name",
                "last_name",
                "email",
                "role_ids",
                "group_ids",
                "is_disabled",
                "home_folder_id",
                "locale",
                "ui_state",
                "models_dir_validated",
                "can_manage_api3_creds",
            ):
                assert f in props, f"create_user missing field: {f}"
            # email must remain OPTIONAL — SSO-only setups create users
            # without email and let SSO link credentials on first login. A
            # regression making email required would silently break that
            # workflow; lock it in.
            assert "email" not in required, (
                "create_user.email must be optional — required-state regression"
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_update_user_exposes_full_writable_surface(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["update_user"].parameters["properties"]
            for f in (
                "first_name",
                "last_name",
                "is_disabled",
                "role_ids",
                "home_folder_id",
                "locale",
                "ui_state",
                "models_dir_validated",
                "can_manage_api3_creds",
            ):
                assert f in props, f"update_user missing field: {f}"
            # email is intentionally NOT settable — managed via credentials_email.
            assert "email" not in props, (
                "update_user must NOT expose email — it's managed via the email "
                "credentials object, not directly on the user."
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_user_routes_advanced_fields_to_body(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "u-9", "email": "x@y.com"})

        respx.post(f"{API_URL}/users").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            await _invoke_tool(
                mcp,
                "create_user",
                {
                    "first_name": "Pat",
                    "last_name": "Doe",
                    "email": "pat@x.com",
                    "is_disabled": True,
                    "home_folder_id": "f-1",
                    "locale": "en",
                    "can_manage_api3_creds": True,
                },
            )()
            body = captured["body"]
            assert body["first_name"] == "Pat"
            assert body["is_disabled"] is True
            assert body["home_folder_id"] == "f-1"
            assert body["locale"] == "en"
            assert body["can_manage_api3_creds"] is True
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_user_no_fields_returns_actionable_error(self, config):
        # No login or PATCH mock — the short-circuit must run BEFORE a
        # Looker session is opened, so no HTTP at all should happen.
        # Pre-refactor the body validation lived inside `async with
        # client.session(ctx)` and burned a wasted login round-trip on the
        # error path.

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "update_user", {"user_id": "1"})()
            assert payload["error"] == "No fields provided to update."
            assert list(respx.calls) == [], (
                "no_fields path opened a Looker session — body validation "
                "should run before client.session()"
            )
        finally:
            await looker_client.close()


class TestGroupSurface:
    @pytest.mark.asyncio
    async def test_create_group_exposes_can_add_to_content_metadata(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["create_group"].parameters["properties"]
            assert "can_add_to_content_metadata" in props
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_update_group_is_registered(self, config):
        # Filling the most glaring admin gap — no update_group existed previously.
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            names = {t.name for t in await mcp.list_tools()}
            assert "update_group" in names
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["update_group"].parameters["properties"]
            assert {"group_id", "name", "can_add_to_content_metadata"} <= props.keys()
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_group_patches_only_provided_fields(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "g-1", "name": "renamed"})

        respx.patch(f"{API_URL}/groups/g-1").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_group",
                {"group_id": "g-1", "name": "renamed"},
            )()
            assert payload["updated"] is True
            assert captured["body"] == {"name": "renamed"}
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_group_no_fields_returns_error(self, config):
        # No login or PATCH mock — the short-circuit must run BEFORE a
        # Looker session is opened, so no HTTP at all should happen.

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "update_group", {"group_id": "g-1"})()
            assert payload["error"] == "No fields provided to update."
            assert list(respx.calls) == [], (
                "no_fields path opened a Looker session — body validation "
                "should run before client.session()"
            )
        finally:
            await looker_client.close()


class TestGroupHierarchy:
    """Group-in-group hierarchy lets parent-group role bindings propagate to
    sub-groups. Without the hierarchy tools, multi-team RBAC has to be
    flattened, which doesn't scale across tenants.
    """

    @pytest.mark.asyncio
    async def test_hierarchy_tools_register(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            names = {t.name for t in await mcp.list_tools()}
            for tool in (
                "list_group_users",
                "list_group_groups",
                "add_group_to_group",
                "remove_group_from_group",
            ):
                assert tool in names, f"missing tool: {tool}"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_add_group_to_group_posts_with_correct_body(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={})

        respx.post(f"{API_URL}/groups/parent-1/groups").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "add_group_to_group",
                {"parent_group_id": "parent-1", "child_group_id": "child-2"},
            )()
            assert payload["added"] is True
            # The wire body uses the field name `group_id`, not `child_group_id` —
            # matches the GroupIdForGroupInclusion shape.
            assert captured["body"] == {"group_id": "child-2"}
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_remove_group_from_group_uses_correct_path(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["path"] = str(request.url)
            return httpx.Response(204)

        respx.delete(f"{API_URL}/groups/parent-1/groups/child-2").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "remove_group_from_group",
                {"parent_group_id": "parent-1", "child_group_id": "child-2"},
            )()
            assert payload["removed"] is True
            assert "/groups/parent-1/groups/child-2" in captured["path"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_group_users_trims_response(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/groups/g-1/users").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "u-1",
                        "email": "a@x.com",
                        "first_name": "A",
                        "last_name": "X",
                        "is_disabled": False,
                        "ui_state": {"large_dict": "..."},  # not in trimmed output
                    }
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "list_group_users", {"group_id": "g-1"})()
            assert len(payload) == 1
            # Tool trims to id+email+name+is_disabled — keeps the response cheap.
            assert payload[0] == {
                "id": "u-1",
                "email": "a@x.com",
                "first_name": "A",
                "last_name": "X",
                "is_disabled": False,
            }
        finally:
            await looker_client.close()


# ══ Admin: email credentials lifecycle ════════════════════════════════


class TestUpdateCredentialsEmail:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_email_and_force_reset(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={"email": "renamed@x.com", "forced_password_reset_at_next_login": True},
            )

        respx.patch(f"{API_URL}/users/u-1/credentials_email").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_credentials_email",
                {
                    "user_id": "u-1",
                    "email": "renamed@x.com",
                    "forced_password_reset_at_next_login": True,
                },
            )()
            assert payload["updated"] is True
            assert captured["body"] == {
                "email": "renamed@x.com",
                "forced_password_reset_at_next_login": True,
            }
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_fields_returns_error(self, config):
        _mock_login_logout()

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "update_credentials_email", {"user_id": "u-1"})()
            assert payload["error"] == "No fields provided to update."
            patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
            assert patch_calls == []
        finally:
            await looker_client.close()


class TestEmailCredentialRegistration:
    @pytest.mark.asyncio
    async def test_email_lifecycle_tools_register(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            names = {t.name for t in await mcp.list_tools()}
            for tool in (
                "create_credentials_email",
                "get_credentials_email",
                "update_credentials_email",
                "delete_credentials_email",
                "send_password_reset",
            ):
                assert tool in names, f"missing email-credential tool: {tool}"

            # create_credentials_email gains forced_password_reset_at_next_login.
            tools = {t.name: t for t in await mcp.list_tools()}
            create_props = tools["create_credentials_email"].parameters["properties"]
            assert "forced_password_reset_at_next_login" in create_props
        finally:
            await looker_client.close()


class TestGetCredentialsEmailCurating:
    """Forwarding the raw upstream payload would risk surfacing fields outside
    the documented contract. The curator pins the metadata shape so the MCP
    response stays stable across Looker versions.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_response_is_curated_to_documented_fields(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/users/u-1/credentials_email").mock(
            return_value=httpx.Response(
                200,
                json={
                    "email": "user@x.com",
                    "is_disabled": False,
                    "has_password": True,
                    "forced_password_reset_at_next_login": True,
                    "created_at": "2026-01-01T00:00:00Z",
                    "logged_in_at": "2026-04-01T00:00:00Z",
                    "password_reset_url_expired": True,
                    "account_setup_url_expired": True,
                    # Out-of-contract — must NOT appear in the curated response.
                    "type": "email",
                    "url": "https://test.looker.com/api/4.0/users/u-1/credentials_email",
                    "user_url": "https://test.looker.com/api/4.0/users/u-1",
                    "can": {"do_thing": True},
                    # Looker may include the one-time-use reset URLs themselves
                    # in the GET response — these are sensitive and must NEVER
                    # leak through the curated tool output.
                    "password_reset_url": "https://test.looker.com/password_reset/SECRET",
                    "account_setup_url": "https://test.looker.com/setup/SECRET",
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"admin"})
        try:
            payload = await _invoke_tool(mcp, "get_credentials_email", {"user_id": "u-1"})()
            assert payload["email"] == "user@x.com"
            assert payload["forced_password_reset_at_next_login"] is True
            assert payload["password_reset_url_expired"] is True
            # has_password — boolean flag for whether a password is set.
            # Useful for callers verifying credential setup without making
            # a separate call to inspect the user.
            assert payload["has_password"] is True
            # Sensitive one-time URLs must NOT leak.
            assert "password_reset_url" not in payload
            assert "account_setup_url" not in payload
            # Out-of-contract metadata must NOT leak.
            for leaky in ("type", "url", "user_url", "can"):
                assert leaky not in payload, f"{leaky} leaked into curated response"
        finally:
            await looker_client.close()
