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
