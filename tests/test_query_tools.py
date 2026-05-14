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

        # Atomicity isn't just call counts — the order matters. A regression
        # that restored the branch before running the query would still
        # produce two PUTs with the right bodies; this assertion locks the
        # actual swap → run → restore sequence so that can't drift silently.
        events = [(call.request.method, call.request.url.path) for call in respx.calls]
        swap_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "PUT" and path.endswith("/projects/proj1/git_branch")
        )
        run_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "GET" and path.endswith("/queries/q1/run/json")
        )
        restore_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "PUT" and path.endswith("/projects/proj1/git_branch") and i > swap_idx
        )
        assert swap_idx < run_idx < restore_idx, (
            f"swap/run/restore must be in order, got events: {events}"
        )

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_without_project_id_returns_clean_validation_error(self, config):
        _mock_login_logout()
        # Mock both endpoints we expect NOT to be called — the validation
        # must short-circuit before any session/query API side effects.
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        create_query = respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(201, json={"id": "q1"})
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

        # Validation must precede every Looker side effect — neither the
        # workspace switch nor the query create should have fired.
        assert not patch_session.called
        assert not create_query.called

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


class TestRunQuery:
    """``run_query(query_id=…)`` wraps ``GET /queries/{id}/run/{format}`` so
    callers can re-run an existing saved Query without re-specifying its
    body. The key invariants tested here:

    1. JSON formats route through ``session.get`` (parsed response).
    2. text/plain formats (csv, txt) route through ``session.get_text`` and
       are wrapped in a JSON envelope for MCP transport.
    3. Optional booleans are serialized as lowercase ``true``/``false`` so
       Looker's query-string parser accepts them.
    4. Branch swap is atomic and wraps the run (mirrors ``query(branch=…)``).
    5. ``branch=`` without ``project_id`` fails-fast before any side effect.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_runs_existing_query_by_id_returning_rows(self, config):
        _mock_login_logout()
        run_route = respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"orders.region": "west", "orders.total": 42},
                    {"orders.region": "east", "orders.total": 17},
                ],
            )
        )
        # No POST /queries — that's the whole point of run_query vs query.
        create_route = respx.post(f"{API_URL}/queries").mock(
            return_value=httpx.Response(201, json={"id": "should-not-be-called"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("run_query", {"query_id": "q1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["row_count"] == 2
                assert payload["data"][0] == {"orders.region": "west", "orders.total": 42}
        finally:
            await looker_client.close()

        assert run_route.called, "must GET the run endpoint by ID"
        assert not create_route.called, "must NOT re-create the query via POST /queries"

    @pytest.mark.asyncio
    @respx.mock
    async def test_csv_format_routes_through_text_plain(self, config):
        """``result_format='csv'`` returns text/plain — must use get_text and
        wrap the raw payload in a JSON envelope."""
        _mock_login_logout()
        csv_body = "orders.region,orders.total\nwest,42\neast,17\n"
        respx.get(f"{API_URL}/queries/q1/run/csv").mock(
            return_value=httpx.Response(
                200,
                text=csv_body,
                headers={"content-type": "text/plain"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "run_query",
                    {"query_id": "q1", "result_format": "csv"},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["format"] == "csv"
                assert payload["data"] == csv_body
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_bool_params_serialize_as_lowercase_strings(self, config):
        """httpx's default ``str(True)`` → ``'True'`` is *not* what Looker's
        query-string parser accepts. The helper must lowercase booleans
        explicitly. This test pins that — a regression to httpx's default
        would silently make ``apply_formatting`` etc. no-ops."""
        _mock_login_logout()
        run_route = respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "run_query",
                    {
                        "query_id": "q1",
                        "limit": 250,
                        "apply_formatting": True,
                        "apply_vis": False,
                        "server_table_calcs": True,
                        "cache": False,
                    },
                )
        finally:
            await looker_client.close()

        assert run_route.called
        sent_url = run_route.calls.last.request.url
        # ``params`` is httpx.QueryParams — supports membership + getitem.
        sent_params = sent_url.params
        assert sent_params["limit"] == "250"
        assert sent_params["apply_formatting"] == "true"
        assert sent_params["apply_vis"] == "false"
        assert sent_params["server_table_calcs"] == "true"
        assert sent_params["cache"] == "false"

    @pytest.mark.asyncio
    @respx.mock
    async def test_default_call_produces_exact_wire_param_map(self, config):
        """With only ``query_id`` provided, every tool-layer default still
        flows down to the helper (the tool forwards its defaults
        unconditionally). The helper's ``is not None`` gate keeps ``None``
        values off the wire but lets ``False`` through — so each tool
        default that isn't ``None`` produces a deterministic
        ``true``/``false`` param.

        Pinning the *exact* wire map is the only way to keep this contract
        honest: a permissive 'if key present then check value' check would
        silently tolerate either future-omission or future-rename of any
        of these knobs. ``query_id`` is path-interpolated, not a query
        param, so it deliberately doesn't appear here; ``limit`` is
        ``None`` by default so it's correctly omitted."""
        _mock_login_logout()
        run_route = respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                # Only query_id — every other arg uses tool default.
                await mcp_client.call_tool("run_query", {"query_id": "q1"})
        finally:
            await looker_client.close()

        assert run_route.called
        sent_params = dict(run_route.calls.last.request.url.params)
        assert sent_params == {
            "apply_formatting": "false",
            "apply_vis": "false",
            "server_table_calcs": "false",
            "cache": "true",
        }

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_arg_drives_save_swap_run_restore(self, config):
        """Mirrors the ``query(branch=…)`` atomicity test — run_query must
        share the same swap → run → restore sequence."""
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        get_branch = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        put_branch = respx.put(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )
        respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[{"orders.region": "west"}])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "run_query",
                    {
                        "query_id": "q1",
                        "branch": "feature-x",
                        "project_id": "proj1",
                    },
                )
        finally:
            await looker_client.close()

        assert patch_session.called, "branch= should imply dev_mode and PATCH /session"
        assert get_branch.called, "must read current branch before swap"
        assert put_branch.call_count == 2, "swap + restore"
        bodies = [json.loads(c.request.content.decode()) for c in put_branch.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]

        events = [(call.request.method, call.request.url.path) for call in respx.calls]
        swap_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "PUT" and path.endswith("/projects/proj1/git_branch")
        )
        run_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "GET" and path.endswith("/queries/q1/run/json")
        )
        restore_idx = next(
            i
            for i, (method, path) in enumerate(events)
            if method == "PUT" and path.endswith("/projects/proj1/git_branch") and i > swap_idx
        )
        assert swap_idx < run_idx < restore_idx, (
            f"swap/run/restore must be in order, got events: {events}"
        )

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_without_project_id_returns_clean_validation_error(self, config):
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        run_route = respx.get(f"{API_URL}/queries/q1/run/json").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "run_query",
                    {
                        "query_id": "q1",
                        "branch": "feature-x",
                        # No project_id — must short-circuit.
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert "branch=" in payload["error"]
                assert "project_id" in payload["error"]
        finally:
            await looker_client.close()

        # Validation precedes every Looker side effect.
        assert not patch_session.called
        assert not run_route.called


class TestRunDashboardSharesRunQueryPath:
    """``run_dashboard`` runs each tile's saved Query via the same helper
    as ``run_query``. This test pins that the per-element call still hits
    ``GET /queries/{id}/run/json`` (regression guard against the refactor
    drifting from the documented contract)."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_dashboard_element_executes_saved_query_by_id(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/dashboards/d1").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Ops",
                    "description": None,
                    "dashboard_elements": [
                        {
                            "title": "Top regions",
                            "type": "vis",
                            "query": {"id": "qA"},
                        }
                    ],
                },
            )
        )
        run_route = respx.get(f"{API_URL}/queries/qA/run/json").mock(
            return_value=httpx.Response(200, json=[{"orders.region": "west"}])
        )

        mcp, looker_client = create_server(config, enabled_groups={"query"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("run_dashboard", {"dashboard_id": "d1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["element_count"] == 1
                assert payload["elements"][0]["row_count"] == 1
        finally:
            await looker_client.close()

        assert run_route.called, (
            "run_dashboard must route its per-tile run through the shared "
            "saved-query helper that backs run_query."
        )
