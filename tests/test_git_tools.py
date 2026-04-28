"""Tests for git tool group — LookML version control, deploy keys, diagnostics.

All tests invoke through ``fastmcp.Client(mcp).call_tool`` so the MCP tool
wrappers are actually exercised — argument plumbing, response shaping,
and ``format_api_error`` handling are all under test, not just the
underlying HTTP layer.
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


# Tools the git group must expose. Acts as a registration regression test —
# a future refactor that drops a tool will fail loudly here.
EXPECTED_GIT_TOOLS = {
    "get_git_branch",
    "list_git_branches",
    "get_git_branch_by_name",
    "create_git_branch",
    "switch_git_branch",
    "delete_git_branch",
    "deploy_to_production",
    "reset_to_production",
    "get_git_deploy_key",
    "create_git_deploy_key",
    "list_git_connection_tests",
    "run_git_connection_test",
}


class TestGitToolRegistration:
    @pytest.mark.asyncio
    async def test_all_git_tools_register(self, config):
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            names = {t.name for t in await mcp.list_tools()}
            missing = EXPECTED_GIT_TOOLS - names
            assert not missing, f"git tool group missing tools: {sorted(missing)}"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    async def test_deploy_to_production_accepts_branch_and_ref(self, config):
        # Looker spec: branch + ref are query params on POST
        # /projects/{id}/deploy_ref_to_production. Without them the tool can
        # only deploy the current dev ref.
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            tools = {t.name: t for t in await mcp.list_tools()}
            props = tools["deploy_to_production"].parameters["properties"]
            assert {"project_id", "branch", "ref"} <= props.keys()
        finally:
            await looker_client.close()


class TestDeleteGitBranch:
    @pytest.mark.asyncio
    @respx.mock
    async def test_invokes_tool_and_returns_envelope(self, config):
        # End-to-end through the MCP tool: argument plumbing, URL
        # construction, response shaping all under test.
        _mock_login_logout()
        respx.delete(f"{API_URL}/projects/proj1/git_branch/feature_x").mock(
            return_value=httpx.Response(204)
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "delete_git_branch",
                {"project_id": "proj1", "branch_name": "feature_x"},
            )()
            assert payload == {
                "deleted": True,
                "project_id": "proj1",
                "branch_name": "feature_x",
            }
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_branch_name_with_slash_is_url_encoded(self, config):
        # Looker branch names commonly contain '/' (e.g. 'feature/foo'). They
        # MUST be percent-encoded so the path doesn't fan out into nested
        # path segments and hit the wrong endpoint.
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(204)

        respx.delete(url__regex=rf"{API_URL}/projects/.*/git_branch/.*").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            await _invoke_tool(
                mcp,
                "delete_git_branch",
                {"project_id": "proj1", "branch_name": "feature/foo"},
            )()
            # The slash inside the branch name must be encoded as %2F so
            # 'feature/foo' is not interpreted as a sub-resource.
            assert "feature%2Ffoo" in captured["raw_path"]
            assert "git_branch/feature/foo" not in captured["raw_path"]
        finally:
            await looker_client.close()


class TestDeployToProduction:
    @pytest.mark.asyncio
    @respx.mock
    async def test_passes_branch_and_ref_as_query_params(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json={})

        respx.post(url__regex=rf"{API_URL}/projects/proj1/deploy_ref_to_production.*").mock(
            side_effect=capture
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "deploy_to_production",
                {"project_id": "proj1", "branch": "release/v2", "ref": "abc123"},
            )()
            assert payload["deployed"] is True
            assert payload["branch"] == "release/v2"
            assert "branch=release" in captured["url"]
            assert "ref=abc123" in captured["url"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_omitting_branch_and_ref_sends_no_query_string(self, config):
        # Default behavior — deploy current dev ref — must not put empty
        # branch/ref params on the wire.
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json={})

        respx.post(url__regex=rf"{API_URL}/projects/proj1/deploy_ref_to_production.*").mock(
            side_effect=capture
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            await _invoke_tool(mcp, "deploy_to_production", {"project_id": "proj1"})()
            assert "branch=" not in captured["url"]
            assert "ref=" not in captured["url"]
        finally:
            await looker_client.close()


class TestGitDeployKey:
    """The deploy-key endpoints return text/plain (raw SSH public key), not
    JSON. The session's text-aware methods (``get_text`` / ``post_text``)
    handle this correctly; tests with JSON mocks would have hidden a real
    production failure caused by ``response.json()`` raising on a
    text/plain body.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_public_key_from_text_response(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/proj1/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                text="ssh-rsa AAAA...example",
                headers={"content-type": "text/plain; charset=utf-8"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(mcp, "get_git_deploy_key", {"project_id": "proj1"})()
            assert payload["project_id"] == "proj1"
            assert payload["public_key"] == "ssh-rsa AAAA...example"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_rotates_and_returns_new_key(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/projects/proj1/git/deploy_key").mock(
            return_value=httpx.Response(
                200,
                text="ssh-rsa NEW...rotated",
                headers={"content-type": "text/plain; charset=utf-8"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(mcp, "create_git_deploy_key", {"project_id": "proj1"})()
            assert payload["rotated"] is True
            assert "NEW" in payload["public_key"]
            # Tool surfaces the registration next-step, useful for agents.
            assert "next_step" in payload
        finally:
            await looker_client.close()


class TestGitConnectionTest:
    @pytest.mark.asyncio
    @respx.mock
    async def test_run_returns_pass_or_fail(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/proj1/git_connection_tests/git_remote_check").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "git_remote_check",
                    "status": "fail",
                    "message": "Permission denied (publickey)",
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "run_git_connection_test",
                {"project_id": "proj1", "test_id": "git_remote_check"},
            )()
            assert payload["status"] == "fail"
            assert payload["passed"] is False
            assert "publickey" in payload["message"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_passes_remote_url_and_use_production_query_params(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json={"id": "t", "status": "pass"})

        respx.get(url__regex=rf"{API_URL}/projects/proj1/git_connection_tests/.*").mock(
            side_effect=capture
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            await _invoke_tool(
                mcp,
                "run_git_connection_test",
                {
                    "project_id": "proj1",
                    "test_id": "remote_dep",
                    "remote_url": "git@github.com:org/repo.git",
                    "use_production": "true",
                },
            )()
            assert "remote_url=" in captured["url"]
            assert "use_production=true" in captured["url"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_trimmed_test_descriptors(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/proj1/git_connection_tests").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "git_remote_check",
                        "description": "Verify the project can talk to its git remote.",
                        "can": {"run": True},  # not surfaced — helps confirm the trim
                    },
                    {"id": "test_2", "description": "Second test."},
                ],
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp, "list_git_connection_tests", {"project_id": "proj1"}
            )()
            assert payload == [
                {
                    "id": "git_remote_check",
                    "description": "Verify the project can talk to its git remote.",
                },
                {"id": "test_2", "description": "Second test."},
            ]
        finally:
            await looker_client.close()


class TestProjectIdPathEncoding:
    """The legacy git tools previously embedded raw ``project_id`` in URL
    paths. After this PR, every git tool routes through ``_path_seg``;
    pinning the encoding behavior here makes regressions visible.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_legacy_get_git_branch_encodes_project_id(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(200, json={"name": "main"})

        respx.get(url__regex=rf"{API_URL}/projects/.*/git_branch").mock(side_effect=capture)

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            # Project id with a space would otherwise produce a malformed path.
            await _invoke_tool(mcp, "get_git_branch", {"project_id": "my project"})()
            assert "my%20project" in captured["raw_path"]
        finally:
            await looker_client.close()
