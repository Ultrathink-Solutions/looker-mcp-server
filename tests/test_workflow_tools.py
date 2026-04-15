"""Tests for workflows tool group — task-oriented compositions."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.config import LookerConfig
from looker_mcp_server.server import create_server


def _iso_days_ago(days: int) -> str:
    """Format a timestamp N days before now in Looker's ISO 8601 shape.

    Using relative-to-now timestamps keeps the stale-session tests from
    silently breaking as the calendar advances past the hard-coded dates
    they originally used.
    """
    return (
        (datetime.now(UTC) - timedelta(days=days))
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


async def _invoke_tool(mcp, tool_name: str, args: dict):
    """Call a tool through the MCP server and return the parsed payload."""
    async with Client(mcp) as mcp_client:
        result = await mcp_client.call_tool(tool_name, args)
        content = result.content[0]
        assert isinstance(content, TextContent)
        return json.loads(content.text)


# ══ provision_connection ═════════════════════════════════════════════


class TestProvisionConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_then_tests(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/connections").mock(
            return_value=httpx.Response(
                201, json={"name": "warehouse", "dialect_name": "snowflake"}
            )
        )
        respx.put(f"{API_URL}/connections/warehouse/test").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"name": "connect", "status": "success", "message": "OK"},
                    {"name": "query", "status": "success", "message": "OK"},
                    {"name": "tmp_table", "status": "success", "message": "OK"},
                ],
            )
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "provision_connection",
                {"name": "warehouse", "dialect_name": "snowflake"},
            )
            assert payload["created"] is True
            assert payload["test"]["ran"] is True
            assert payload["test"]["healthy"] is True
            assert len(payload["test"]["checks"]) == 3
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_keeps_connection_when_test_fails(self, config):
        """Partial failure: connection created, but a check failed. We
        report the breakdown rather than rolling back the connection."""
        _mock_login_logout()
        respx.post(f"{API_URL}/connections").mock(
            return_value=httpx.Response(
                201, json={"name": "warehouse", "dialect_name": "snowflake"}
            )
        )
        respx.put(f"{API_URL}/connections/warehouse/test").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"name": "connect", "status": "success", "message": "OK"},
                    {
                        "name": "tmp_table",
                        "status": "error",
                        "message": "Missing scratch schema",
                    },
                ],
            )
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "provision_connection",
                {"name": "warehouse", "dialect_name": "snowflake"},
            )
            assert payload["created"] is True
            assert payload["test"]["ran"] is True
            assert payload["test"]["healthy"] is False
            # The failing check is surfaced so agent can decide next step.
            failing = [c for c in payload["test"]["checks"] if c["status"] == "error"]
            assert len(failing) == 1
            assert failing[0]["check"] == "tmp_table"
        finally:
            await client.close()


# ══ bootstrap_lookml_project ═════════════════════════════════════════


class TestBootstrapLookmlProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_chains_create_update_deploy_key(self, config):
        _mock_login_logout()

        respx.post(f"{API_URL}/projects").mock(
            return_value=httpx.Response(201, json={"id": "analytics", "name": "analytics"})
        )
        captured_patch: dict = {}

        def capture_patch(request: httpx.Request) -> httpx.Response:
            captured_patch["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "analytics"})

        respx.patch(f"{API_URL}/projects/analytics").mock(side_effect=capture_patch)
        respx.post(f"{API_URL}/projects/analytics/git/deploy_key").mock(
            return_value=httpx.Response(200, json="ssh-ed25519 AAAAC3Nz...")
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "bootstrap_lookml_project",
                {
                    "name": "analytics",
                    "git_remote_url": "git@github.com:example/analytics.git",
                    "git_service_name": "github",
                },
            )
            assert payload["created"] is True
            assert payload["project_id"] == "analytics"
            assert "ssh-ed25519" in payload["deploy_key_public"]
            # Git config patched with the right fields.
            assert captured_patch["body"]["git_remote_url"].endswith("analytics.git")
            assert captured_patch["body"]["git_service_name"] == "github"
        finally:
            await client.close()


# ══ deploy_lookml_changes ════════════════════════════════════════════


class TestDeployLookmlChanges:
    @pytest.mark.asyncio
    @respx.mock
    async def test_skips_deploy_on_validation_errors(self, config):
        """Critical safety property: validation errors must not trigger
        a deploy. The test fails if the POST /deploy_to_production mock
        is called."""
        _mock_login_logout()

        # File writes — PATCH succeeds for both files.
        respx.patch(url__regex=rf"{API_URL}/projects/analytics/files/.*").mock(
            return_value=httpx.Response(200, json={"id": "whatever"})
        )

        # Validation returns errors.
        respx.post(f"{API_URL}/projects/analytics/lookml_validation").mock(
            return_value=httpx.Response(
                200,
                json={
                    "errors": [
                        {
                            "severity": "error",
                            "message": "unknown field",
                            "source_file": "views/orders.view.lkml",
                            "line": 42,
                        }
                    ],
                    "warnings": [],
                },
            )
        )

        # No mock for deploy_to_production — if it's called, the test errors.

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_lookml_changes",
                {
                    "project_id": "analytics",
                    "files": {"views/orders.view.lkml": "view: orders {}"},
                    "validate": True,
                },
            )
            assert payload["deployed"] is False
            assert payload["validation"]["valid"] is False
            assert payload["validation"]["error_count"] == 1

            # Verify no deploy call happened.
            deploy_calls = [
                c for c in respx.calls if c.request.url.path.endswith("/deploy_to_production")
            ]
            assert deploy_calls == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_fails_on_non_404_patch_error(self, config):
        """Non-404 PATCH failures (auth, 5xx, etc.) must propagate — we
        must NOT silently fall back to POST, which would double side
        effects and mask the real cause."""
        _mock_login_logout()

        # PATCH returns 500 — simulating a server error / transient.
        respx.patch(url__regex=rf"{API_URL}/projects/analytics/files/.*").mock(
            return_value=httpx.Response(500, json={"message": "internal server error"})
        )
        # Explicit routes for the forbidden calls so .called gives a
        # clear yes/no signal rather than having to filter respx.calls.
        create_fallback_route = respx.post(
            url__regex=rf"{API_URL}/projects/analytics/files/.*"
        ).mock(return_value=httpx.Response(201, json={"id": "unexpected"}))
        deploy_route = respx.post(f"{API_URL}/projects/analytics/deploy_to_production").mock(
            return_value=httpx.Response(200, json={})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_lookml_changes",
                {
                    "project_id": "analytics",
                    "files": {"views/orders.view.lkml": "view: orders {}"},
                    "validate": True,
                },
            )
            # Error surfaces to caller — tool's top-level exception
            # handler returns the format_api_error envelope.
            assert "error" in payload
            # Safety invariants: no create-as-fallback, no deploy.
            # The tool fails fast on the first bad PATCH.
            assert create_fallback_route.called is False
            assert deploy_route.called is False
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_missing_file_on_404(self, config):
        """The 404→POST fallback path still works for genuinely missing
        files. Complements test_fails_on_non_404_patch_error."""
        _mock_login_logout()

        respx.patch(url__regex=rf"{API_URL}/projects/analytics/files/.*").mock(
            return_value=httpx.Response(404, json={"message": "file not found"})
        )
        create_captured: dict = {"called": False}

        def capture_post(request: httpx.Request) -> httpx.Response:
            create_captured["called"] = True
            return httpx.Response(200, json={"id": "views/new.view.lkml"})

        respx.post(url__regex=rf"{API_URL}/projects/analytics/files/.*").mock(
            side_effect=capture_post
        )
        respx.post(f"{API_URL}/projects/analytics/lookml_validation").mock(
            return_value=httpx.Response(200, json={"errors": [], "warnings": []})
        )
        respx.post(f"{API_URL}/projects/analytics/deploy_to_production").mock(
            return_value=httpx.Response(200, json={})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_lookml_changes",
                {
                    "project_id": "analytics",
                    "files": {"views/new.view.lkml": "view: new {}"},
                },
            )
            assert payload["deployed"] is True
            assert payload["files"][0]["action"] == "created"
            assert create_captured["called"] is True
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_deploys_when_validation_passes(self, config):
        _mock_login_logout()
        respx.patch(url__regex=rf"{API_URL}/projects/analytics/files/.*").mock(
            return_value=httpx.Response(200, json={"id": "whatever"})
        )
        respx.post(f"{API_URL}/projects/analytics/lookml_validation").mock(
            return_value=httpx.Response(200, json={"errors": [], "warnings": []})
        )
        deploy_captured: dict = {"called": False}

        def capture_deploy(request: httpx.Request) -> httpx.Response:
            deploy_captured["called"] = True
            return httpx.Response(200, json={})

        respx.post(f"{API_URL}/projects/analytics/deploy_to_production").mock(
            side_effect=capture_deploy
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_lookml_changes",
                {
                    "project_id": "analytics",
                    "files": {"views/orders.view.lkml": "view: orders {}"},
                },
            )
            assert payload["deployed"] is True
            assert deploy_captured["called"] is True
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_files_returns_error(self, config):
        _mock_login_logout()

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_lookml_changes",
                {"project_id": "analytics", "files": {}},
            )
            assert payload["error"] == "No files provided."
        finally:
            await client.close()


# ══ rollback_to_production ═══════════════════════════════════════════


class TestRollbackToProduction:
    @pytest.mark.asyncio
    @respx.mock
    async def test_requires_confirm_flag(self, config):
        _mock_login_logout()
        # No reset mock — if the tool forgets the confirm check, the test errors.

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "rollback_to_production", {"project_id": "analytics"})
            assert payload["error"] == "Confirmation required."
            # Safety: no POST to reset_to_production happened.
            assert [c for c in respx.calls if "reset_to_production" in c.request.url.path] == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_resets_when_confirmed(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/projects/analytics/reset_to_production").mock(
            return_value=httpx.Response(200, json={})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "rollback_to_production",
                {"project_id": "analytics", "confirm": True},
            )
            assert payload["reset"] is True
        finally:
            await client.close()


# ══ provision_user ═══════════════════════════════════════════════════


class TestProvisionUser:
    @pytest.mark.asyncio
    @respx.mock
    async def test_chains_all_steps(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/users").mock(
            return_value=httpx.Response(201, json={"id": "99", "email": "a@example.com"})
        )
        respx.post(f"{API_URL}/users/99/credentials_email").mock(
            return_value=httpx.Response(201, json={"email": "a@example.com"})
        )
        respx.patch(f"{API_URL}/users/99/attribute_values/5").mock(
            return_value=httpx.Response(200, json={"value": "EMEA"})
        )
        respx.post(f"{API_URL}/users/99/credentials_email/send_password_reset").mock(
            return_value=httpx.Response(200, json={})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "provision_user",
                {
                    "email": "a@example.com",
                    "first_name": "Alice",
                    "last_name": "Example",
                    "role_ids": [1, 2],
                    "group_ids": [3],
                    "user_attribute_values": {"5": "EMEA"},
                    "send_invite": True,
                },
            )
            assert payload["all_steps_ok"] is True
            step_names = [s["step"] for s in payload["steps"]]
            assert "create_user" in step_names
            assert "create_credentials_email" in step_names
            assert "set_user_attribute_user_value" in step_names
            assert "send_password_reset" in step_names
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_reports_partial_failure_without_rollback(self, config):
        """If credential attachment fails, the user record stays put and
        the failure is reported per-step."""
        _mock_login_logout()
        respx.post(f"{API_URL}/users").mock(
            return_value=httpx.Response(201, json={"id": "99", "email": "a@example.com"})
        )
        respx.post(f"{API_URL}/users/99/credentials_email").mock(
            return_value=httpx.Response(
                409, json={"message": "Credentials already exist for email"}
            )
        )
        respx.post(f"{API_URL}/users/99/credentials_email/send_password_reset").mock(
            return_value=httpx.Response(200, json={})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "provision_user",
                {
                    "email": "a@example.com",
                    "first_name": "Alice",
                    "last_name": "Example",
                    "send_invite": True,
                },
            )
            assert payload["all_steps_ok"] is False
            creds_step = next(
                s for s in payload["steps"] if s["step"] == "create_credentials_email"
            )
            assert creds_step["ok"] is False
            # User was NOT rolled back — they exist and have an id.
            assert payload["user_id"] == "99"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_short_circuits_on_empty_user_id(self, config):
        """If POST /users returns no id, downstream calls would get a
        malformed URL like /users//credentials_email. Tool must detect
        this and short-circuit with a clear error."""
        _mock_login_logout()
        # POST /users succeeds but returns no id field.
        respx.post(f"{API_URL}/users").mock(
            return_value=httpx.Response(201, json={"email": "a@example.com"})
        )
        # No mocks for downstream endpoints — if any are called, the test errors.

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "provision_user",
                {
                    "email": "a@example.com",
                    "first_name": "Alice",
                    "last_name": "Example",
                    "user_attribute_values": {"5": "EMEA"},
                    "send_invite": True,
                },
            )
            assert "error" in payload
            assert "no id" in payload["error"].lower()
            # No downstream POSTs happened — tool failed fast.
            post_calls = [c for c in respx.calls if c.request.method == "POST"]
            # Exactly two: /login (session setup), and POST /users.
            post_paths = {c.request.url.path for c in post_calls}
            assert all(p.endswith("/login") or p.endswith("/users") for p in post_paths), (
                f"Unexpected POST calls: {post_paths}"
            )
        finally:
            await client.close()


# ══ grant_access ═════════════════════════════════════════════════════


class TestGrantAccess:
    @pytest.mark.asyncio
    @respx.mock
    async def test_adds_user_to_role(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/roles/5/users").mock(
            return_value=httpx.Response(200, json=[{"id": "1", "email": "existing@example.com"}])
        )
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json=[])

        respx.put(f"{API_URL}/roles/5/users").mock(side_effect=capture)

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "grant_access",
                {"principal_type": "user", "principal_id": "42", "role_id": "5"},
            )
            assert payload["granted"] is True
            # Augmented list preserves the existing member.
            assert set(captured["body"]) == {1, 42}
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_idempotent_when_already_granted(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/roles/5/groups").mock(
            return_value=httpx.Response(200, json=[{"id": "7", "name": "analysts"}])
        )
        # No PUT mock — if the tool forgets the idempotency check, the test errors.

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "grant_access",
                {"principal_type": "group", "principal_id": "7", "role_id": "5"},
            )
            assert payload["already_granted"] is True
            # Verify no PUT call was made.
            assert [c for c in respx.calls if c.request.method == "PUT"] == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_rejects_invalid_principal_type(self, config):
        _mock_login_logout()

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "grant_access",
                {"principal_type": "service", "principal_id": "1", "role_id": "5"},
            )
            assert "error" in payload
            assert "principal_type" in payload["error"]
        finally:
            await client.close()


# ══ offboard_user ════════════════════════════════════════════════════


class TestOffboardUser:
    @pytest.mark.asyncio
    @respx.mock
    async def test_terminates_sessions_revokes_keys_disables(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 1001, "user_id": 42, "created_at": "2026-04-10T00:00:00Z"},
                    {"id": 1002, "user_id": 7, "created_at": "2026-04-10T00:00:00Z"},
                ],
            )
        )
        terminated: list = []

        def capture_terminate(request: httpx.Request) -> httpx.Response:
            terminated.append(request.url.path)
            return httpx.Response(204)

        respx.delete(url__regex=rf"{API_URL}/sessions/\d+").mock(side_effect=capture_terminate)
        respx.get(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(200, json=[{"id": "99", "client_id": "abc"}])
        )
        respx.delete(f"{API_URL}/users/42/credentials_api3/99").mock(
            return_value=httpx.Response(204)
        )
        respx.patch(f"{API_URL}/users/42").mock(
            return_value=httpx.Response(200, json={"id": "42", "is_disabled": True})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp, "offboard_user", {"user_id": "42", "deactivate_only": True}
            )
            assert payload["all_steps_ok"] is True
            assert payload["deactivated"] is True
            # Terminated the session belonging to user 42, not the other one.
            assert len(terminated) == 1
            assert "/sessions/1001" in terminated[0]
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_mode_requires_explicit_flag(self, config):
        """When deactivate_only=False, DELETE is used instead of PATCH."""
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(200, json=[])
        )

        delete_captured = {"called": False}

        def capture_delete(request: httpx.Request) -> httpx.Response:
            delete_captured["called"] = True
            return httpx.Response(204)

        respx.delete(f"{API_URL}/users/42").mock(side_effect=capture_delete)

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp, "offboard_user", {"user_id": "42", "deactivate_only": False}
            )
            assert payload["deleted"] is True
            assert payload["deactivated"] is False
            assert payload["requested_action"] == "delete"
            assert delete_captured["called"] is True
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_deactivated_flag_reflects_outcome_not_request(self, config):
        """If the disable PATCH fails, `deactivated` must be False even
        though the caller requested deactivate_only=True. Previously the
        response mirrored the input, making a failed offboard look
        successful to callers who only checked `deactivated`."""
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(return_value=httpx.Response(200, json=[]))
        respx.get(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(200, json=[])
        )
        # PATCH /users/42 fails — Looker 403.
        respx.patch(f"{API_URL}/users/42").mock(
            return_value=httpx.Response(403, json={"message": "not authorized"})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp, "offboard_user", {"user_id": "42", "deactivate_only": True}
            )
            assert payload["all_steps_ok"] is False
            # Critical: request was 'deactivate' but outcome was failure,
            # so deactivated must be False (not True as before).
            assert payload["requested_action"] == "deactivate"
            assert payload["deactivated"] is False
            assert payload["deleted"] is False
            # The failing step is visible in the steps list.
            failed = [s for s in payload["steps"] if not s.get("ok", True)]
            assert len(failed) == 1
            assert failed[0]["step"] == "disable_user"
        finally:
            await client.close()


# ══ rotate_api_credentials ═══════════════════════════════════════════


class TestRotateApiCredentials:
    @pytest.mark.asyncio
    @respx.mock
    async def test_create_only_when_no_previous_id(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(
                201,
                json={"id": "99", "client_id": "new", "client_secret": "ONCE-ONLY"},
            )
        )
        # No delete mock — should not be called.

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "rotate_api_credentials", {"user_id": "42"})
            assert payload["rotated"] is True
            assert payload["new_credentials"]["client_secret"] == "ONCE-ONLY"
            assert payload["old_pair_deletion"] is None
            # Verify no DELETE calls to credentials_api3/* happened.
            del_calls = [
                c
                for c in respx.calls
                if c.request.method == "DELETE" and "credentials_api3" in c.request.url.path
            ]
            assert del_calls == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_deletes_previous_when_id_provided(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(
                201, json={"id": "99", "client_id": "new", "client_secret": "ONCE"}
            )
        )
        respx.delete(f"{API_URL}/users/42/credentials_api3/old").mock(
            return_value=httpx.Response(204)
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "rotate_api_credentials",
                {"user_id": "42", "delete_previous_id": "old"},
            )
            assert payload["old_pair_deletion"]["ok"] is True
            assert payload["old_pair_deletion"]["deleted_previous_id"] == "old"
        finally:
            await client.close()


# ══ audit_query_activity (scope enum) ════════════════════════════════


class TestAuditQueryActivity:
    @pytest.mark.asyncio
    @respx.mock
    async def test_errors_scope_filters_to_non_complete(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "q1"})

        respx.post(f"{API_URL}/queries").mock(side_effect=capture)
        respx.get(f"{API_URL}/queries/q1/run/json").mock(return_value=httpx.Response(200, json=[]))

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            await _invoke_tool(mcp, "audit_query_activity", {"scope": "errors"})
            assert captured["body"]["filters"]["history.status"] == "-complete"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_by_user_scope_requires_user_email(self, config):
        _mock_login_logout()
        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "audit_query_activity", {"scope": "by_user"})
            assert "error" in payload
            assert "user_email" in payload["error"]
            # No POST /queries should have fired.
            assert [c for c in respx.calls if c.request.url.path.endswith("/queries")] == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_rejects_unknown_scope(self, config):
        _mock_login_logout()
        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "audit_query_activity", {"scope": "everything"})
            assert "error" in payload
            assert "scope" in payload["error"]
        finally:
            await client.close()


# ══ audit_instance_health ════════════════════════════════════════════


class TestAuditInstanceHealth:
    @pytest.mark.asyncio
    @respx.mock
    async def test_aggregates_three_sections(self, config):
        _mock_login_logout()

        # Each POST /queries returns a different query id so we can mock
        # the run/json responses distinctly.
        query_ids = iter(["pdt-q", "sched-q"])

        def post_queries(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201, json={"id": next(query_ids)})

        respx.post(f"{API_URL}/queries").mock(side_effect=post_queries)
        respx.get(f"{API_URL}/queries/pdt-q/run/json").mock(
            return_value=httpx.Response(
                200,
                json=[{"pdt_event_log.status_code": "error", "pdt_event_log.message": "x"}],
            )
        )
        respx.get(f"{API_URL}/queries/sched-q/run/json").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{API_URL}/content_validation").mock(
            return_value=httpx.Response(
                200,
                json={
                    "total_errors": 2,
                    "total_looks_validated": 50,
                    "total_dashboards_validated": 10,
                },
            )
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "audit_instance_health", {})
            assert payload["healthy"] is False
            assert payload["partial_failure"] is False
            # 1 PDT-build sample + 0 schedule samples + 2 content_validation errors = 3
            assert payload["sample_issue_count"] == 3
            assert payload["sections"]["failed_pdt_builds"]["sample_count"] == 1
            assert payload["sections"]["failed_pdt_builds"]["truncated"] is False
            assert payload["sections"]["content_validation"]["total_errors"] == 2
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_section_error_flips_healthy_false(self, config):
        """A section whose query errors out must force healthy=False and
        partial_failure=True, so the caller can never mistake an
        uninspectable instance for a healthy one."""
        _mock_login_logout()

        # PDT query errors; schedule query succeeds with zero failures;
        # content_validation succeeds with zero errors. Under the old
        # shape the tool reported healthy=True because the erroring
        # section contributed 0 to the total. Fixed: section error
        # flips healthy to False regardless.
        respx.post(f"{API_URL}/queries").mock(
            side_effect=[
                httpx.Response(500, json={"message": "server error"}),
                httpx.Response(201, json={"id": "sched-q"}),
            ]
        )
        respx.get(f"{API_URL}/queries/sched-q/run/json").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx.get(f"{API_URL}/content_validation").mock(
            return_value=httpx.Response(200, json={"total_errors": 0, "total_looks_validated": 50})
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(mcp, "audit_instance_health", {})
            assert payload["healthy"] is False
            assert payload["partial_failure"] is True
            assert "error" in payload["sections"]["failed_pdt_builds"]
        finally:
            await client.close()


# ══ investigate_runaway_queries ══════════════════════════════════════


class TestInvestigateRunawayQueries:
    @pytest.mark.asyncio
    @respx.mock
    async def test_report_lists_above_threshold(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/running_queries").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"query_task_id": "a", "runtime": 400, "source": "dashboard"},
                    {"query_task_id": "b", "runtime": 60, "source": "api"},
                    {"query_task_id": "c", "runtime": 600, "source": "explore"},
                ],
            )
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "investigate_runaway_queries",
                {"threshold_seconds": 300, "action": "report"},
            )
            assert payload["runaway_count"] == 2
            task_ids = {r["query_task_id"] for r in payload["runaways"]}
            assert task_ids == {"a", "c"}
            # In report mode, no DELETE should fire.
            assert payload["killed"] is None
            assert [
                c
                for c in respx.calls
                if c.request.method == "DELETE" and "running_queries" in c.request.url.path
            ] == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_kill_terminates_each_runaway(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/running_queries").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"query_task_id": "a", "runtime": 400},
                    {"query_task_id": "c", "runtime": 600},
                ],
            )
        )
        killed_paths: list = []

        def capture_kill(request: httpx.Request) -> httpx.Response:
            killed_paths.append(request.url.path)
            return httpx.Response(204)

        respx.delete(url__regex=rf"{API_URL}/running_queries/.+").mock(side_effect=capture_kill)

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "investigate_runaway_queries",
                {"threshold_seconds": 100, "action": "kill"},
            )
            assert payload["runaway_count"] == 2
            assert len(payload["killed"]) == 2
            assert all(k["ok"] for k in payload["killed"])
        finally:
            await client.close()


# ══ find_stale_content ═══════════════════════════════════════════════


class TestFindStaleContent:
    @pytest.mark.asyncio
    @respx.mock
    async def test_scopes_content_type_and_sorts_oldest_first(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "q1"})

        respx.post(f"{API_URL}/queries").mock(side_effect=capture)
        respx.get(f"{API_URL}/queries/q1/run/json").mock(return_value=httpx.Response(200, json=[]))

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            await _invoke_tool(
                mcp,
                "find_stale_content",
                {"min_days_unused": 120, "content_type": "dashboard"},
            )
            assert captured["body"]["view"] == "content_usage"
            assert captured["body"]["filters"]["content_usage.content_type"] == "dashboard"
            assert captured["body"]["sorts"] == ["content_usage.last_accessed_date asc"]
        finally:
            await client.close()


# ══ disable_stale_sessions ═══════════════════════════════════════════


class TestDisableStaleSessions:
    @pytest.mark.asyncio
    @respx.mock
    async def test_report_lists_without_terminating(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(
            return_value=httpx.Response(
                200,
                json=[
                    # ~200 days old — stale at the default 90d threshold.
                    {"id": 1, "user_id": 7, "created_at": _iso_days_ago(200)},
                    # Recent — within the threshold, must NOT be reported.
                    {"id": 2, "user_id": 8, "created_at": _iso_days_ago(5)},
                ],
            )
        )

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "disable_stale_sessions",
                {"max_age_days": 90, "action": "report"},
            )
            assert payload["stale_count"] == 1
            assert payload["stale_sessions"][0]["id"] == 1
            # Report mode: no terminated list in response.
            assert payload["terminated"] is None
            # Safety: no DELETE calls.
            assert [
                c
                for c in respx.calls
                if c.request.method == "DELETE" and "/sessions/" in c.request.url.path
            ] == []
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_terminate_kills_each_stale(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"id": 1, "created_at": _iso_days_ago(220)},
                    {"id": 2, "created_at": _iso_days_ago(180)},
                    {"id": 3, "created_at": _iso_days_ago(5)},  # recent
                ],
            )
        )
        respx.delete(url__regex=rf"{API_URL}/sessions/\d+").mock(return_value=httpx.Response(204))

        mcp, client = create_server(config, enabled_groups={"workflows"})
        try:
            payload = await _invoke_tool(
                mcp,
                "disable_stale_sessions",
                {"max_age_days": 90, "action": "terminate"},
            )
            assert payload["stale_count"] == 2
            assert len(payload["terminated"]) == 2
            assert all(t["ok"] for t in payload["terminated"])
        finally:
            await client.close()



# ══ Registration ═════════════════════════════════════════════════════


class TestServerRegistration:
    def test_workflows_in_all_groups(self):
        from looker_mcp_server.config import ALL_GROUPS

        assert "workflows" in ALL_GROUPS
