"""Tests for git tool group — LookML version control, deploy keys, diagnostics."""

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
def server_and_client(config):
    mcp, client = create_server(config, enabled_groups={"git"})
    return mcp, client


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


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
    async def test_all_git_tools_register(self, server_and_client):
        mcp, _ = server_and_client
        names = {t.name for t in await mcp.list_tools()}
        missing = EXPECTED_GIT_TOOLS - names
        assert not missing, f"git tool group missing tools: {sorted(missing)}"

    @pytest.mark.asyncio
    async def test_deploy_to_production_accepts_branch_and_ref(self, server_and_client):
        # Looker spec: branch + ref are query params on POST
        # /projects/{id}/deploy_ref_to_production. Without them the tool can
        # only deploy the current dev ref.
        mcp, _ = server_and_client
        tools = {t.name: t for t in await mcp.list_tools()}
        props = tools["deploy_to_production"].parameters["properties"]
        assert {"project_id", "branch", "ref"} <= props.keys()


class TestDeleteGitBranch:
    @pytest.mark.asyncio
    @respx.mock
    async def test_issues_delete_to_branch_path(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/projects/proj1/git_branch/feature_x").mock(
            return_value=httpx.Response(204)
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_git_branch", "git", {"project_id": "proj1"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.delete(
                    f"/projects/{quote('proj1', safe='')}/git_branch/{quote('feature_x', safe='')}"
                )
        finally:
            await client.close()

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

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_git_branch", "git", {"project_id": "proj1"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.delete(
                    f"/projects/{quote('proj1', safe='')}"
                    f"/git_branch/{quote('feature/foo', safe='')}"
                )
                # The slash inside the branch name must be encoded as %2F so
                # 'feature/foo' is not interpreted as a sub-resource.
                assert "feature%2Ffoo" in captured["raw_path"]
                assert "git_branch/feature/foo" not in captured["raw_path"]
        finally:
            await client.close()


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

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("deploy_to_production", "git", {"project_id": "proj1"})
        try:
            async with client.session(ctx) as session:
                await session.post(
                    "/projects/proj1/deploy_ref_to_production",
                    params={"branch": "release/v2", "ref": "abc123"},
                )
                assert "branch=release" in captured["url"]
                assert "ref=abc123" in captured["url"]
        finally:
            await client.close()


class TestGitDeployKey:
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_public_key(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/projects/proj1/git/deploy_key").mock(
            return_value=httpx.Response(200, json="ssh-rsa AAAA...example")
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_git_deploy_key", "git", {"project_id": "proj1"})
        try:
            async with client.session(ctx) as session:
                key = await session.get("/projects/proj1/git/deploy_key")
                assert key == "ssh-rsa AAAA...example"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_rotates_key(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/projects/proj1/git/deploy_key").mock(
            return_value=httpx.Response(200, json="ssh-rsa NEW...rotated")
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_git_deploy_key", "git", {"project_id": "proj1"})
        try:
            async with client.session(ctx) as session:
                key = await session.post("/projects/proj1/git/deploy_key")
                assert "NEW" in key
        finally:
            await client.close()


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

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "run_git_connection_test",
            "git",
            {"project_id": "proj1", "test_id": "git_remote_check"},
        )
        try:
            async with client.session(ctx) as session:
                result = await session.get("/projects/proj1/git_connection_tests/git_remote_check")
                assert result["status"] == "fail"
                assert "publickey" in result["message"]
        finally:
            await client.close()

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

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "run_git_connection_test",
            "git",
            {"project_id": "proj1", "test_id": "remote_dep"},
        )
        try:
            async with client.session(ctx) as session:
                await session.get(
                    "/projects/proj1/git_connection_tests/remote_dep",
                    params={
                        "remote_url": "git@github.com:org/repo.git",
                        "use_production": "true",
                    },
                )
                assert "remote_url=" in captured["url"]
                assert "use_production=true" in captured["url"]
        finally:
            await client.close()
