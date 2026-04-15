"""Tests for audit tool group — query history, content usage, live-ops observability."""

import json

import httpx
import pytest
import respx

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


@pytest.fixture
async def server_and_client(config):
    mcp, client = create_server(config, enabled_groups={"audit"})
    try:
        yield mcp, client
    finally:
        await client.close()


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


def _mock_system_activity_query(
    captured_body: dict,
    rows: list[dict],
    query_id: str = "abc123",
) -> None:
    """Stand up the two-hop query flow: POST /queries, then GET /queries/{id}/run/json."""

    def capture(request: httpx.Request) -> httpx.Response:
        captured_body["body"] = json.loads(request.content.decode())
        return httpx.Response(201, json={"id": query_id})

    respx.post(f"{API_URL}/queries").mock(side_effect=capture)
    respx.get(f"{API_URL}/queries/{query_id}/run/json").mock(
        return_value=httpx.Response(200, json=rows)
    )


class TestServerRegistration:
    @pytest.mark.asyncio
    async def test_audit_registers(self, server_and_client):
        mcp, _ = server_and_client
        assert mcp is not None

    def test_audit_in_all_groups(self):
        from looker_mcp_server.config import ALL_GROUPS

        assert "audit" in ALL_GROUPS


# ══ system__activity wrappers ════════════════════════════════════════


class TestGetQueryHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_builds_system_activity_query(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(
            captured,
            rows=[
                {
                    "history.created_time": "2026-04-15 10:00",
                    "user.email": "a@example.com",
                    "query.model": "ecommerce",
                    "history.runtime": 12.3,
                    "history.status": "complete",
                }
            ],
        )

        from fastmcp import Client
        from mcp.types import TextContent

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "get_query_history",
                    {"date_range": "7 days", "errors_only": True},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["row_count"] == 1

                # The POST body should be the system__activity history query
                # with our filters applied.
                assert captured["body"]["model"] == "system__activity"
                assert captured["body"]["view"] == "history"
                assert "history.created_time" in captured["body"]["filters"]
                assert captured["body"]["filters"]["history.status"] == "-complete"
                assert captured["body"]["sorts"] == ["history.runtime desc"]
        finally:
            await client.close()


class TestGetContentUsage:
    @pytest.mark.asyncio
    @respx.mock
    async def test_scopes_to_content_type(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(captured, rows=[])

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "get_content_usage",
                    {"content_type": "dashboard", "min_views": 10},
                )
                assert captured["body"]["view"] == "content_usage"
                assert captured["body"]["filters"]["content_usage.content_type"] == "dashboard"
                assert captured["body"]["filters"]["content_usage.view_count"] == ">=10"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_all_content_type_does_not_filter(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(captured, rows=[])

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool("get_content_usage", {"content_type": "all"})
                # 'all' means no content_type filter — must not be in the body.
                assert "content_usage.content_type" not in captured["body"]["filters"]
        finally:
            await client.close()


class TestGetPdtBuildLog:
    @pytest.mark.asyncio
    @respx.mock
    async def test_failed_only_filter(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(captured, rows=[])

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "get_pdt_build_log", {"failed_only": True, "model": "ecommerce"}
                )
                assert captured["body"]["view"] == "pdt_event_log"
                assert captured["body"]["filters"]["pdt_event_log.status_code"] == "error"
                assert captured["body"]["filters"]["pdt_event_log.model_name"] == "ecommerce"
        finally:
            await client.close()


class TestGetScheduleHistory:
    @pytest.mark.asyncio
    @respx.mock
    async def test_failed_only_uses_negative_filter(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(captured, rows=[])

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool("get_schedule_history", {"failed_only": True})
                # Looker filter syntax: "-value" means "not equal to".
                assert captured["body"]["filters"]["scheduled_job.status"] == "-success"
        finally:
            await client.close()


class TestGetUserActivityLog:
    @pytest.mark.asyncio
    @respx.mock
    async def test_event_types_pass_through(self, config):
        _mock_login_logout()
        captured: dict = {}
        _mock_system_activity_query(captured, rows=[])

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "get_user_activity_log",
                    {
                        "event_types": "login,logout",
                        "user_email": "a@example.com",
                    },
                )
                assert captured["body"]["view"] == "event"
                assert captured["body"]["filters"]["event.name"] == "login,logout"
                assert captured["body"]["filters"]["user.email"] == "a@example.com"
        finally:
            await client.close()


# ══ Live-ops: running queries + sessions ═════════════════════════════


class TestRunningQueries:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_trimmed_summary(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/running_queries").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "query_task_id": "task-1",
                        "query_id": 42,
                        "source": "dashboard",
                        "runtime": 8.1,
                        "user_id": 7,
                        "user": {"email": "ops@example.com"},
                    }
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_running_queries", "audit")
        try:
            async with client.session(ctx) as session:
                running = await session.get("/running_queries")
                assert running[0]["query_task_id"] == "task-1"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_kill_hits_delete_endpoint(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/running_queries/task-1").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("kill_query", "audit", {"query_task_id": "task-1"})
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/running_queries/task-1") is None
        finally:
            await client.close()


class TestSessions:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_active_sessions(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/sessions").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": 1001,
                        "user_id": 42,
                        "ip_address": "10.0.0.1",
                        "browser": "Chrome",
                    }
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_active_sessions", "audit")
        try:
            async with client.session(ctx) as session:
                sessions_list = await session.get("/sessions")
                assert len(sessions_list) == 1
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_terminate_session(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/sessions/1001").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("terminate_session", "audit", {"session_id": "1001"})
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/sessions/1001") is None
        finally:
            await client.close()


# ══ Live-ops: project CI runs ════════════════════════════════════════


class TestProjectCi:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_runs(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics/ci/runs").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "run-1",
                        "status": "success",
                        "branch": "main",
                        "started_at": "2026-04-15T10:00:00Z",
                    }
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_project_ci_runs", "audit", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                runs = await session.get("/projects/analytics/ci/runs")
                assert runs[0]["status"] == "success"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_trigger_sends_branch_when_provided(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "run-99", "status": "pending"})

        respx.post(f"{API_URL}/projects/analytics/ci/runs").mock(side_effect=capture)

        from fastmcp import Client

        mcp, client = create_server(config, enabled_groups={"audit"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "trigger_project_ci_run",
                    {"project_id": "analytics", "branch": "feature-x"},
                )
                assert captured["body"] == {"branch": "feature-x"}
        finally:
            await client.close()


# ══ Path encoding regression ═════════════════════════════════════════


class TestPathEncoding:
    """Defensive encoding: query_task_id / session_id / project_id flow through _path_seg."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_session_id_with_slash_is_encoded(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(204)

        respx.delete(url__regex=rf"{API_URL}/sessions/.*").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("terminate_session", "audit", {"session_id": "../boom"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.delete(f"/sessions/{quote('../boom', safe='')}")
                raw = captured["raw_path"]
                # Either full encoding of `..` or just the slash — both prevent traversal.
                assert "%2E%2E%2Fboom" in raw or "..%2Fboom" in raw
                assert "/sessions/../boom" not in raw
        finally:
            await client.close()
