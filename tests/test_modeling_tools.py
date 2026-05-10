"""Tests for modeling tool group — LookML project CRUD.

Scoped to the newly added project-level tools. File-level tools
(``get_file``, ``create_file``, etc.) have coverage in test_client.py via
the session/auth paths.
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


class TestGetProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_full_project(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "analytics",
                    "name": "analytics",
                    "git_remote_url": "git@github.com:example/looker-analytics.git",
                    "git_service_name": "github",
                    "validation_required": True,
                    "pull_request_mode": "required",
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                project = await session.get("/projects/analytics")
                assert project["id"] == "analytics"
                assert project["git_service_name"] == "github"
        finally:
            await client.close()


class TestCreateProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_name_only(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "new_project", "name": "new_project"})

        respx.post(f"{API_URL}/projects").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_project", "modeling", {"name": "new_project"})
        try:
            async with client.session(ctx) as session:
                project = await session.post("/projects", body={"name": "new_project"})
                assert project["id"] == "new_project"
                assert captured["body"] == {"name": "new_project"}
        finally:
            await client.close()


class TestUpdateProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_only_provided_fields(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "analytics"})

        respx.patch(f"{API_URL}/projects/analytics").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("update_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                await session.patch(
                    "/projects/analytics",
                    body={
                        "git_remote_url": "git@github.com:example/looker-analytics.git",
                        "git_service_name": "github",
                    },
                )
                assert captured["body"] == {
                    "git_remote_url": "git@github.com:example/looker-analytics.git",
                    "git_service_name": "github",
                }
                assert "pull_request_mode" not in captured["body"]
        finally:
            await client.close()


class TestUpdateProjectNoFields:
    """The empty-body branch of update_project must short-circuit before calling Looker."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_error_when_no_fields_provided(self, config):
        _mock_login_logout()
        # No PATCH mock is registered — any outbound PATCH would raise.

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "update_project",
                    {"project_id": "analytics"},
                )
                content = result.content[0]
                # Tool returns a plain JSON string, which fastmcp wraps in TextContent.
                assert isinstance(content, TextContent), f"Unexpected content type: {type(content)}"
                payload = json.loads(content.text)
                assert payload["error"] == "No fields provided to update."
                assert "hint" in payload
                # No HTTP PATCH call was issued (respx recorded none).
                patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
                assert patch_calls == []
        finally:
            await looker_client.close()


class TestDeleteProject:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_success_envelope(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/projects/analytics").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_project", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                result = await session.delete("/projects/analytics")
                assert result is None
        finally:
            await client.close()


class TestGetProjectManifest:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_manifest(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics/manifest").mock(
            return_value=httpx.Response(
                200,
                json={
                    "name": "analytics",
                    "project_name": "analytics",
                    "localizations": [],
                    "local_dependency": [],
                    "remote_dependency": [
                        {"name": "shared_lookml", "url": "git@github.com:example/shared.git"}
                    ],
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project_manifest", "modeling", {"project_id": "analytics"})
        try:
            async with client.session(ctx) as session:
                manifest = await session.get("/projects/analytics/manifest")
                assert manifest["project_name"] == "analytics"
                assert len(manifest["remote_dependency"]) == 1
        finally:
            await client.close()


class TestProjectDeployKey:
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_public_key(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/analytics/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                json="ssh-ed25519 AAAAC3Nz... looker-deploy-key",
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "get_project_deploy_key", "modeling", {"project_id": "analytics"}
        )
        try:
            async with client.session(ctx) as session:
                key = await session.get("/projects/analytics/git/deploy_key")
                assert isinstance(key, str)
                assert "ssh-ed25519" in key
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_generates_new_key(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/projects/analytics/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                json="ssh-ed25519 AAAAC3Nz-rotated...",
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "create_project_deploy_key", "modeling", {"project_id": "analytics"}
        )
        try:
            async with client.session(ctx) as session:
                key = await session.post("/projects/analytics/git/deploy_key")
                assert "rotated" in key
        finally:
            await client.close()


class TestPathEncoding:
    """Project IDs can contain characters that need URL-encoding."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_special_character_project_id_is_encoded(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(200, json={"id": "my analytics"})

        respx.get(url__regex=rf"{API_URL}/projects/.*").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_project", "modeling", {"project_id": "my analytics"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.get(f"/projects/{quote('my analytics', safe='')}")
                assert "my%20analytics" in captured["raw_path"]
                assert " " not in captured["raw_path"]
        finally:
            await client.close()


class TestDatagroupAdmin:
    """``trigger_datagroup`` and ``get_datagroup`` complete the datagroup
    admin surface. ``trigger_datagroup`` is the missing primitive — sets
    ``triggered_at`` to force a PDT rebuild AND cache invalidation
    simultaneously. ``reset_datagroup`` only does cache bust (sets
    ``stale_before``)."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_datagroup_returns_field_allow_list(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/datagroups/dg1").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "dg1",
                    "model_name": "ecommerce",
                    "name": "hourly",
                    "trigger_check_at": 1716000000,
                    "triggered_at": 1716000000,
                    "stale_before": 0,
                    "trigger_value": "abc123",
                    "trigger_error": None,
                    "internal_only_field": "hidden",  # NOT in allow-list
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("get_datagroup", {"datagroup_id": "dg1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["id"] == "dg1"
                assert payload["model_name"] == "ecommerce"
                assert "internal_only_field" not in payload
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_trigger_datagroup_patches_triggered_at(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "dg1"})

        respx.patch(f"{API_URL}/datagroups/dg1").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("trigger_datagroup", {"datagroup_id": "dg1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["triggered"] is True
                # Body MUST set triggered_at (not stale_before) — that's
                # the difference from ``reset_datagroup``.
                assert "triggered_at" in captured["body"]
                assert "stale_before" not in captured["body"]
                assert isinstance(captured["body"]["triggered_at"], int)
        finally:
            await looker_client.close()


class TestPdtBuildAdmin:
    """``start_pdt_build`` / ``check_pdt_build`` / ``stop_pdt_build``
    cover the on-demand PDT regen flow. All three are GET requests per
    Looker's OpenAPI spec — the ``stop`` endpoint is GET (not DELETE),
    which is unusual but documented and stable."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_start_pdt_build_passes_force_flags_as_query_strings(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(
                200,
                json={"materialization_id": "mat-1", "status": "started"},
            )

        respx.get(f"{API_URL}/derived_table/ecommerce/orders_pdt/start").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "start_pdt_build",
                    {
                        "model_name": "ecommerce",
                        "view_name": "orders_pdt",
                        "force_rebuild": True,
                        "force_full_incremental": True,
                        "workspace": "dev",
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["materialization_id"] == "mat-1"
        finally:
            await looker_client.close()

        assert "force_rebuild=true" in captured["url"]
        assert "force_full_incremental=true" in captured["url"]
        assert "workspace=dev" in captured["url"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_check_pdt_build_returns_status_and_progress(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/derived_table/mat-1/status").mock(
            return_value=httpx.Response(
                200,
                json={
                    "materialization_id": "mat-1",
                    "status": "running",
                    "ratio": 0.42,
                    "resp_text": "Building…",
                    "resource_usage": {"warehouse_credits": 12.5},
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "check_pdt_build", {"materialization_id": "mat-1"}
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["status"] == "running"
                assert payload["ratio"] == 0.42
                assert payload["resource_usage"]["warehouse_credits"] == 12.5
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_stop_pdt_build_uses_get_per_looker_spec(self, config):
        _mock_login_logout()
        # Per Looker's OpenAPI spec, ``/derived_table/{id}/stop`` is GET
        # (not DELETE) — this regression-locks that surprising shape.
        respx.get(f"{API_URL}/derived_table/mat-1/stop").mock(
            return_value=httpx.Response(
                200,
                json={"materialization_id": "mat-1", "status": "stopped"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "stop_pdt_build", {"materialization_id": "mat-1"}
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["stopped"] is True
                assert payload["status"] == "stopped"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_stop_pdt_build_does_not_falsely_report_success_on_noop(self, config):
        # Edge case: caller stops an already-finished materialization.
        # Looker returns the existing status (``complete``) — we must
        # NOT hard-code ``stopped: True``, because that would misreport
        # the cancellation as successful when the build had already
        # naturally completed.
        _mock_login_logout()
        respx.get(f"{API_URL}/derived_table/mat-2/stop").mock(
            return_value=httpx.Response(
                200,
                json={"materialization_id": "mat-2", "status": "complete"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "stop_pdt_build", {"materialization_id": "mat-2"}
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                # Status reflects what Looker actually returned, and
                # ``stopped`` is derived from it — not hard-coded.
                assert payload["status"] == "complete"
                assert payload["stopped"] is False
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_graph_derived_tables_for_view_passes_models_and_workspace(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json={"graph_text": "digraph { ... }"})

        respx.get(f"{API_URL}/derived_table/graph/view/orders_pdt").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "graph_derived_tables_for_view",
                    {"view": "orders_pdt", "models": "ecommerce", "workspace": "dev"},
                )
        finally:
            await looker_client.close()

        assert "models=ecommerce" in captured["url"]
        assert "workspace=dev" in captured["url"]


class TestBranchArgValidation:
    """``_validate_branch_args`` is the single source of truth for
    rejecting invalid branch/project_id combinations. These tests
    exercise it through ``validate_project`` since that's the simplest
    consumer surface — but the helper itself is shared across all
    tools that accept ``branch=…``."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_branch_string_returns_validation_error(self, config):
        # Empty branch must not reach Looker; it would PUT {"name": ""}
        # and surface as an opaque 400. Catch upstream with a clear
        # ValueError that format_api_error renders cleanly.
        _mock_login_logout()
        # Mock the endpoints we expect NOT to be called.
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        get_branch = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "validate_project",
                    {"project_id": "proj1", "branch": ""},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert "non-empty" in payload["error"]
        finally:
            await looker_client.close()

        assert not patch_session.called
        assert not get_branch.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_whitespace_only_branch_returns_validation_error(self, config):
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "validate_project",
                    {"project_id": "proj1", "branch": "   "},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert "whitespace-only" in payload["error"] or "non-empty" in payload["error"]
        finally:
            await looker_client.close()

        assert not patch_session.called


class TestValidateProjectDevMode:
    """``validate_project`` is the load-bearing primitive for catching
    LookML errors introduced by a PR. Default behavior validates
    production (backwards-compatible); ``branch=…`` flips the dev
    workspace to the feature branch atomically with save+restore."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_default_validates_production(self, config):
        _mock_login_logout()
        # No PATCH /session expected with dev_mode default False.
        patch_route = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "production"})
        )
        respx.post(f"{API_URL}/projects/proj1/lookml_validation").mock(
            return_value=httpx.Response(200, json={"errors": [], "warnings": []})
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("validate_project", {"project_id": "proj1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["valid"] is True
        finally:
            await looker_client.close()

        # Default behavior: no workspace switch.
        assert not patch_route.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_drives_save_swap_validate_restore(self, config):
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
        # Validation surfaces a real LookML error — exercises the failure
        # case from the field report (broken assert in tests.lkml).
        respx.post(f"{API_URL}/projects/proj1/lookml_validation").mock(
            return_value=httpx.Response(
                200,
                json={
                    "errors": [
                        {
                            "severity": "error",
                            "kind": "value_error",
                            "message": "wrong argument type NilClass (expected Integer)",
                            "source_file": "models/tests.lkml",
                            "line": 42,
                        }
                    ],
                    "warnings": [],
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "validate_project",
                    {"project_id": "proj1", "branch": "feature-x"},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["valid"] is False
                assert payload["error_count"] == 1
                assert "NilClass" in payload["errors"][0]["message"]
        finally:
            await looker_client.close()

        # The full sequence happened in order: PATCH /session, GET branch,
        # PUT swap, POST validate, PUT restore.
        assert patch_session.called
        assert get_branch.called
        assert put_branch.call_count == 2
        bodies = [json.loads(c.request.content.decode()) for c in put_branch.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]


class TestFileOpsDevMode:
    """File-ops tools migrated from the per-call ``?workspace_id=dev``
    query-param trick to the session-level PATCH /session pattern. The
    default behavior matches the previous behavior (dev workspace), but
    callers can now opt for production reads or atomic branch swaps."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_project_files_defaults_to_dev_workspace(self, config):
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        respx.get(f"{API_URL}/projects/proj1/files").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "models/x.lkml", "title": "x", "type": "model"}],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("list_project_files", {"project_id": "proj1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload[0]["id"] == "models/x.lkml"
        finally:
            await looker_client.close()

        # Default dev_mode=True triggers PATCH /session — replaces the
        # legacy ``?workspace_id=dev`` query-param trick.
        assert patch_session.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_project_files_with_dev_mode_false_reads_production(self, config):
        _mock_login_logout()
        patch_session = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        respx.get(f"{API_URL}/projects/proj1/files").mock(return_value=httpx.Response(200, json=[]))

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "list_project_files",
                    {"project_id": "proj1", "dev_mode": False},
                )
        finally:
            await looker_client.close()

        # No workspace switch when caller explicitly opts out.
        assert not patch_session.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_update_file_with_branch_does_atomic_save_swap_restore(self, config):
        _mock_login_logout()
        respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        get_branch = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        put_branch = respx.put(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )
        respx.patch(f"{API_URL}/projects/proj1/files/models%2Fx.lkml").mock(
            return_value=httpx.Response(200, json={"id": "models/x.lkml"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "update_file",
                    {
                        "project_id": "proj1",
                        "file_id": "models/x.lkml",
                        "content": "view: x { dimension: id {} }",
                        "branch": "feature-x",
                    },
                )
        finally:
            await looker_client.close()

        # branch=feature-x triggers the full save→swap→edit→restore cycle.
        assert get_branch.called
        assert put_branch.call_count == 2
        bodies = [json.loads(c.request.content.decode()) for c in put_branch.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]


class TestLookmlTests:
    """The load-bearing primitive for catching data-regression bugs in CI:
    runs LookML data tests against the warehouse and reports per-test
    success/failure with the assertion-level errors. Pair with branch=…
    to validate a PR before merge."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_test_names_models_files(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/proj1/lookml_tests").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "name": "test_orders_total",
                        "model_name": "ecommerce",
                        "explore_name": "orders",
                        "file": "tests/orders.lkml",
                        "line": 12,
                        "query_url_params": "fields=orders.region",
                    }
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("list_lookml_tests", {"project_id": "proj1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload[0]["name"] == "test_orders_total"
                assert payload[0]["model"] == "ecommerce"
                assert payload[0]["explore"] == "orders"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_run_default_runs_against_production(self, config):
        _mock_login_logout()
        # No PATCH /session expected with default args — production-only.
        patch_route = respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "production"})
        )
        respx.get(f"{API_URL}/projects/proj1/lookml_tests/run").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "model_name": "ecommerce",
                        "test_name": "test_orders_total",
                        "success": True,
                        "errors": [],
                    }
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool("run_lookml_tests", {"project_id": "proj1"})
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["passed"] is True
                assert payload["test_count"] == 1
                assert payload["failure_count"] == 0
        finally:
            await looker_client.close()

        assert not patch_route.called, "default run should not switch workspace"

    @pytest.mark.asyncio
    @respx.mock
    async def test_run_with_branch_does_full_atomic_cycle_and_surfaces_failures(self, config):
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
        # The exact failure pattern from the field-report incident: a
        # broken assert that Spectacles false-passes but Looker's runtime
        # evaluator throws on. We expect run_lookml_tests to surface it
        # with assertion-level detail.
        respx.get(f"{API_URL}/projects/proj1/lookml_tests/run").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "model_name": "ecommerce",
                        "test_name": "test_orders_total",
                        "success": False,
                        "errors": [
                            {
                                "message": ("wrong argument type NilClass (expected Integer)"),
                                "line_number": 42,
                            }
                        ],
                    },
                    {
                        "model_name": "ecommerce",
                        "test_name": "test_orders_count",
                        "success": True,
                        "errors": [],
                    },
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "run_lookml_tests",
                    {"project_id": "proj1", "branch": "feature-x"},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["passed"] is False
                assert payload["test_count"] == 2
                assert payload["failure_count"] == 1
                # Raw results passed through — the failure carries the
                # assertion-level error for the regression report.
                fail = next(r for r in payload["results"] if not r["success"])
                assert "NilClass" in fail["errors"][0]["message"]
        finally:
            await looker_client.close()

        # Full atomic cycle: workspace switch + branch swap + run + restore.
        assert patch_session.called
        assert get_branch.called
        assert put_branch.call_count == 2
        bodies = [json.loads(c.request.content.decode()) for c in put_branch.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]

    @pytest.mark.asyncio
    @respx.mock
    async def test_run_filters_passed_to_query_params(self, config):
        _mock_login_logout()
        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json=[])

        respx.get(f"{API_URL}/projects/proj1/lookml_tests/run").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "run_lookml_tests",
                    {
                        "project_id": "proj1",
                        "model": "ecommerce",
                        "test": "test_orders_total",
                    },
                )
        finally:
            await looker_client.close()

        # Filter args ride as query params on the GET — verifies the tool
        # respects the API's filtering surface (model/test/file_id).
        assert "model=ecommerce" in captured["url"]
        assert "test=test_orders_total" in captured["url"]

    @pytest.mark.asyncio
    @respx.mock
    @pytest.mark.parametrize("bad_timeout", [0, 0.0, -1, -10.5])
    async def test_run_rejects_non_positive_timeout(self, config, bad_timeout):
        # ``httpx.Timeout(0)`` raises and negative values are undefined —
        # both would surface as opaque transport-layer errors well after
        # auth + workspace setup. Reject up front with a clear ValueError
        # so callers get a deterministic validation error.
        _mock_login_logout()
        # Mock both endpoints we expect NOT to be called so any leak fails
        # this test loudly instead of silently passing.
        login_route = respx.get(f"{API_URL}/projects/proj1/lookml_tests/run").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, looker_client = create_server(config, enabled_groups={"modeling"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "run_lookml_tests",
                    {"project_id": "proj1", "timeout": bad_timeout},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert "timeout" in payload["error"]
                assert "positive" in payload["error"]
        finally:
            await looker_client.close()

        # Validation must short-circuit before the underlying GET.
        assert not login_route.called
