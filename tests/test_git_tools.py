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
import structlog
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.config import LookerConfig
from looker_mcp_server.server import create_server


@pytest.fixture
def config():
    # ``sudo_as_user=True`` is the default for any deployment configured
    # with admin credentials, and is also the gate for ``act_as_user``.
    # Tests in TestActAsUser require the wrapper to be installed; tests
    # that don't pass ``act_as_user`` are unaffected — with no email
    # header and no argument, the wrapper falls through to its inner
    # provider which falls through to api_key behavior.
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=True,
        _env_file=None,  # type: ignore[call-arg]
    )


@pytest.fixture
def config_sudo_disabled():
    # Used to verify the gate: ``act_as_user`` must fail loudly when
    # the operator has explicitly disabled sudo, not silently route the
    # call under the configured identity.
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
    # Dev-mode-required tools (switch/create/delete branch, reset_to_production)
    # call ``update_workspace("dev")`` immediately after login. Mock the
    # session-update endpoint so respx doesn't reject it as un-mocked.
    respx.patch(f"{API_URL}/session").mock(
        return_value=httpx.Response(200, json={"workspace_id": "dev"})
    )


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
    "reset_git_branch_to_remote",
    "update_git_branch",
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


class TestActAsUser:
    """Per-call admin impersonation via ``act_as_user``.

    These tests pin the integrated behavior of
    ``ArgumentSudoIdentityProvider`` + ``LookerClient.session()`` — when a
    git tool receives ``act_as_user`` we expect a sudo session to be
    established (admin login → ``login_user`` → action → double-logout)
    and an INFO-level audit line to be emitted.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_numeric_user_id_triggers_sudo(self, config):
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        sudo_route = respx.post(f"{API_URL}/login/42").mock(
            return_value=httpx.Response(200, json={"access_token": "sudo-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))
        # delete_git_branch enables dev_mode, which triggers PATCH /session.
        respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        delete_route = respx.delete(f"{API_URL}/projects/p1/git_branch/tmp_ci_abc").mock(
            return_value=httpx.Response(204)
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "delete_git_branch",
                {
                    "project_id": "p1",
                    "branch_name": "tmp_ci_abc",
                    "act_as_user": "42",
                },
            )()
            assert sudo_route.called, "login_user must be called for the target user"
            assert delete_route.called
            assert payload == {
                "deleted": True,
                "project_id": "p1",
                "branch_name": "tmp_ci_abc",
            }
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_email_resolves_via_lookup_then_sudo(self, config):
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        # ``GET /users?email=...&limit=1`` is what ``lookup_user_by_email``
        # issues internally — the wrapper provider's lookup_fn is wired
        # to that helper at bootstrap.
        lookup_route = respx.get(f"{API_URL}/users").mock(
            return_value=httpx.Response(200, json=[{"id": "77", "email": "ci-bot@example.com"}])
        )
        sudo_route = respx.post(f"{API_URL}/login/77").mock(
            return_value=httpx.Response(200, json={"access_token": "sudo-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))
        # delete_git_branch enables dev_mode, which triggers PATCH /session.
        respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        delete_route = respx.delete(f"{API_URL}/projects/p1/git_branch/tmp_ci_abc").mock(
            return_value=httpx.Response(204)
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            await _invoke_tool(
                mcp,
                "delete_git_branch",
                {
                    "project_id": "p1",
                    "branch_name": "tmp_ci_abc",
                    "act_as_user": "ci-bot@example.com",
                },
            )()
            assert lookup_route.called, "email must be resolved via /users lookup"
            assert sudo_route.called, "sudo target must be the resolved user_id"
            assert delete_route.called
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_email_lookup_miss_surfaces_clean_error(self, config):
        # Fail-loud is deliberate: a typo'd email must NOT silently fall
        # back to the configured admin identity (which would perform the
        # action under the wrong user).
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.get(f"{API_URL}/users").mock(return_value=httpx.Response(200, json=[]))
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "delete_git_branch",
                {
                    "project_id": "p1",
                    "branch_name": "tmp_ci_abc",
                    "act_as_user": "ghost@example.com",
                },
            )()
            assert "error" in payload
            assert "ghost@example.com" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_emits_audit_log_on_argument_driven_sudo(self, config):
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.post(f"{API_URL}/login/42").mock(
            return_value=httpx.Response(200, json={"access_token": "sudo-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))
        # delete_git_branch enables dev_mode, which triggers PATCH /session.
        respx.patch(f"{API_URL}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        respx.delete(f"{API_URL}/projects/p1/git_branch/tmp_ci_abc").mock(
            return_value=httpx.Response(204)
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            with structlog.testing.capture_logs() as captured:
                await _invoke_tool(
                    mcp,
                    "delete_git_branch",
                    {
                        "project_id": "p1",
                        "branch_name": "tmp_ci_abc",
                        "act_as_user": "42",
                    },
                )()

            audit = [line for line in captured if line.get("event") == "looker.audit.act_as_user"]
            assert len(audit) == 1, captured
            entry = audit[0]
            assert entry["target_user_id"] == "42"
            assert entry["triggered_by"] == "argument"
            assert entry["configured_user"] == "test-id"
            assert entry["tool"] == "delete_git_branch"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_format_rejected_up_front(self, config):
        # A bare username (no ``@``, not all-digits) must fail validation
        # rather than be forwarded to ``/login/alice`` where Looker would
        # respond with an opaque 400/404. No login or sudo route is
        # registered — if the validation isn't up-front, respx will
        # surface an unmatched-request RuntimeError.
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "delete_git_branch",
                {
                    "project_id": "p1",
                    "branch_name": "tmp_ci_abc",
                    "act_as_user": "alice",
                },
            )()
            # Surfaced as a clean validation error — no "Unexpected
            # error" wrapper, since format_api_error special-cases
            # ValueError.
            assert "error" in payload
            assert "Unexpected" not in payload["error"]
            assert "alice" in payload["error"]
            assert "numeric" in payload["error"].lower()
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_act_as_user_refused_when_sudo_disabled(self, config_sudo_disabled):
        # The gate: with ``LOOKER_SUDO_AS_USER=false`` the operator has
        # explicitly disabled sudo. ``act_as_user`` must fail loudly so
        # the misconfiguration surfaces at the call site instead of
        # silently routing the call under the configured identity.
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))

        mcp, looker_client = create_server(config_sudo_disabled, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "delete_git_branch",
                {
                    "project_id": "p1",
                    "branch_name": "tmp_ci_abc",
                    "act_as_user": "42",
                },
            )()
            assert "error" in payload
            assert "LOOKER_SUDO_AS_USER" in payload["error"]
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_sudo_disabled_without_act_as_user_works(self, config_sudo_disabled):
        # Companion to the gate test: without ``act_as_user``, the gate
        # is irrelevant — calls run normally under api_key mode.
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))
        respx.get(f"{API_URL}/projects/p1/git_branches").mock(
            return_value=httpx.Response(200, json=[{"name": "main"}])
        )

        mcp, looker_client = create_server(config_sudo_disabled, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(mcp, "list_git_branches", {"project_id": "p1"})()
            assert payload[0]["name"] == "main"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_audit_log_when_act_as_user_absent(self, config):
        respx.post(f"{API_URL}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-tok"})
        )
        respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))
        respx.get(f"{API_URL}/projects/p1/git_branches").mock(
            return_value=httpx.Response(200, json=[])
        )

        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            with structlog.testing.capture_logs() as captured:
                await _invoke_tool(mcp, "list_git_branches", {"project_id": "p1"})()

            audit = [line for line in captured if line.get("event") == "looker.audit.act_as_user"]
            assert audit == []
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


class TestResetGitBranchToRemote:
    """``reset_git_branch_to_remote`` is the force-push recovery primitive:
    hard-reset the current dev branch to its remote HEAD (fetch + reset).
    It discards the target user's uncommitted IDE edits, so it requires
    ``confirm=True`` and reports before/after branch state for verification."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_unconfirmed_returns_refusal_without_api_calls(self, config):
        _mock_login_logout()
        reset_route = respx.post(f"{API_URL}/projects/proj1/reset_to_remote").mock(
            return_value=httpx.Response(204)
        )
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp, "reset_git_branch_to_remote", {"project_id": "proj1"}
            )()
            assert "error" in payload
            assert not reset_route.called, "refusal must short-circuit before any API call"
            assert not respx.calls, (
                "refusal must short-circuit before ANY API traffic — "
                "including login and the dev-mode session PATCH"
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_confirmed_resets_and_returns_before_after(self, config):
        _mock_login_logout()
        stale = {"name": "feature-x", "ref": "a592f91", "remote_ref": "f60eb12"}
        fresh = {"name": "feature-x", "ref": "f60eb12", "remote_ref": "f60eb12"}
        branch_route = respx.get(f"{API_URL}/projects/proj1/git_branch").mock(
            side_effect=[httpx.Response(200, json=stale), httpx.Response(200, json=fresh)]
        )
        reset_route = respx.post(f"{API_URL}/projects/proj1/reset_to_remote").mock(
            return_value=httpx.Response(204)
        )
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "reset_git_branch_to_remote",
                {"project_id": "proj1", "confirm": True},
            )()
        finally:
            await looker_client.close()

        assert reset_route.called
        assert branch_route.call_count == 2, "must capture branch state before AND after"
        assert payload["reset"] is True
        assert payload["before"] == {
            "name": "feature-x",
            "ref": "a592f91",
            "remote_ref": "f60eb12",
        }
        assert payload["after"] == {
            "name": "feature-x",
            "ref": "f60eb12",
            "remote_ref": "f60eb12",
        }

        # The reset must run in the dev workspace — without the session
        # PATCH it would target production and 422.
        patched = [
            c
            for c in respx.calls
            if c.request.method == "PATCH" and "/session" in str(c.request.url)
        ]
        assert patched, "reset_git_branch_to_remote must switch the session to dev mode"


class TestUpdateGitBranch:
    """``update_git_branch`` pins a branch to a specific ref via
    ``PUT /projects/{id}/git_branch`` with ``{name, ref}`` — the
    deterministic-sync primitive (e.g. CI pinning to an event SHA).
    Equally destructive to the target user's local branch position, so it
    carries the same ``confirm=True`` guard."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_unconfirmed_returns_refusal_without_api_calls(self, config):
        _mock_login_logout()
        put_route = respx.put(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x", "ref": "f60eb12"})
        )
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_git_branch",
                {"project_id": "proj1", "branch_name": "feature-x", "ref": "f60eb12"},
            )()
            assert "error" in payload
            assert not put_route.called, "refusal must short-circuit before any API call"
            assert not respx.calls, (
                "refusal must short-circuit before ANY API traffic — "
                "including login and the dev-mode session PATCH"
            )
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_confirmed_puts_name_and_ref(self, config):
        _mock_login_logout()
        put_route = respx.put(f"{API_URL}/projects/proj1/git_branch").mock(
            return_value=httpx.Response(
                200, json={"name": "feature-x", "ref": "f60eb12", "remote_ref": "f60eb12"}
            )
        )
        mcp, looker_client = create_server(config, enabled_groups={"git"})
        try:
            payload = await _invoke_tool(
                mcp,
                "update_git_branch",
                {
                    "project_id": "proj1",
                    "branch_name": "feature-x",
                    "ref": "f60eb12",
                    "confirm": True,
                },
            )()
        finally:
            await looker_client.close()

        assert put_route.called
        body = json.loads(put_route.calls[0].request.content.decode())
        assert body == {"name": "feature-x", "ref": "f60eb12"}
        assert payload["name"] == "feature-x"
        assert payload["ref"] == "f60eb12"

        # The pin must run in the dev workspace — without the session PATCH
        # it would target production and 422.
        session_patches = [
            c
            for c in respx.calls
            if c.request.method == "PATCH" and "/session" in str(c.request.url)
        ]
        assert session_patches, "update_git_branch must switch the session to dev mode"
        patch_body = json.loads(session_patches[0].request.content.decode())
        assert patch_body == {"workspace_id": "dev"}
