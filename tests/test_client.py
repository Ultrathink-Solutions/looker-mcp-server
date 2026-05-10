"""Tests for LookerClient session management."""

import json

import httpx
import pytest
import respx

from looker_mcp_server.client import LookerApiError, LookerClient, LookerSession, format_api_error
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import (
    ApiKeyIdentityProvider,
    OAuthIdentityProvider,
    RequestContext,
    SudoIdentityProvider,
)


@pytest.fixture
def config():
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        _env_file=None,  # type: ignore[call-arg]
    )


class TestLookerSession:
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_request(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/lookml_models").mock(
            return_value=httpx.Response(200, json=[{"name": "model1"}])
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.get("/lookml_models")
            assert result == [{"name": "model1"}]

    @pytest.mark.asyncio
    @respx.mock
    async def test_post_request(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.post(f"{api_url}/queries").mock(return_value=httpx.Response(200, json={"id": 123}))
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.post("/queries", body={"model": "m", "view": "v"})
            assert result["id"] == 123

    @pytest.mark.asyncio
    @respx.mock
    async def test_raises_on_error(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/bad").mock(
            return_value=httpx.Response(404, json={"message": "Not found"})
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            with pytest.raises(LookerApiError) as exc_info:
                await session.get("/bad")
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_returns_none(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.delete(f"{api_url}/resource/1").mock(return_value=httpx.Response(204))
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.delete("/resource/1")
            assert result is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_with_params(self):
        api_url = "https://test.looker.com/api/4.0"
        route = respx.get(f"{api_url}/projects/myproj/files").mock(
            return_value=httpx.Response(200, json=[{"id": "test.lkml"}])
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.get("/projects/myproj/files", params={"workspace_id": "dev"})
            assert result == [{"id": "test.lkml"}]
            assert "workspace_id=dev" in str(route.calls[0].request.url)

    @pytest.mark.asyncio
    @respx.mock
    async def test_post_with_params(self):
        api_url = "https://test.looker.com/api/4.0"
        route = respx.post(f"{api_url}/projects/myproj/files/new.lkml").mock(
            return_value=httpx.Response(200, json={"id": "new.lkml"})
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.post(
                "/projects/myproj/files/new.lkml",
                body={"content": "view: test {}"},
                params={"workspace_id": "dev"},
            )
            assert result["id"] == "new.lkml"
            assert "workspace_id=dev" in str(route.calls[0].request.url)

    @pytest.mark.asyncio
    @respx.mock
    async def test_patch_with_params(self):
        api_url = "https://test.looker.com/api/4.0"
        route = respx.patch(f"{api_url}/projects/myproj/files/test.lkml").mock(
            return_value=httpx.Response(200, json={"id": "test.lkml"})
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.patch(
                "/projects/myproj/files/test.lkml",
                body={"content": "updated"},
                params={"workspace_id": "dev"},
            )
            assert result["id"] == "test.lkml"
            assert "workspace_id=dev" in str(route.calls[0].request.url)

    @pytest.mark.asyncio
    @respx.mock
    async def test_put_with_params(self):
        api_url = "https://test.looker.com/api/4.0"
        route = respx.put(f"{api_url}/projects/myproj/files/test.lkml").mock(
            return_value=httpx.Response(200, json={"id": "test.lkml"})
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            result = await session.put(
                "/projects/myproj/files/test.lkml",
                body={"content": "updated"},
                params={"workspace_id": "dev"},
            )
            assert result["id"] == "test.lkml"
            assert "workspace_id=dev" in str(route.calls[0].request.url)

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_with_params(self):
        api_url = "https://test.looker.com/api/4.0"
        route = respx.delete(f"{api_url}/projects/myproj/files/old.lkml").mock(
            return_value=httpx.Response(204)
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            await session.delete("/projects/myproj/files/old.lkml", params={"workspace_id": "dev"})
            assert "workspace_id=dev" in str(route.calls[0].request.url)


class TestLookerClientApiKey:
    @pytest.mark.asyncio
    @respx.mock
    async def test_login_logout_lifecycle(self, config):
        api_url = config.api_url
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "sess-token"})
        )
        respx.get(f"{api_url}/lookml_models").mock(
            return_value=httpx.Response(200, json=[{"name": "m1"}])
        )
        respx.delete(f"{api_url}/logout").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = RequestContext(tool_name="list_models", tool_group="explore")

        async with client.session(ctx) as session:
            result = await session.get("/lookml_models")
            assert result == [{"name": "m1"}]

        await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_dev_mode_patches_session_workspace(self, config):
        api_url = config.api_url
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "sess-token"})
        )
        # The contract: when ``dev_mode=True``, PATCH /session is called
        # with workspace_id=dev before the body runs. Without it, branch
        # operations would fail with "Developer mode required".
        patch_route = respx.patch(f"{api_url}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        respx.delete(f"{api_url}/logout").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = RequestContext(tool_name="t", tool_group="g")

        async with client.session(ctx, dev_mode=True):
            pass

        assert patch_route.called
        body = json.loads(patch_route.calls[0].request.content.decode())
        assert body == {"workspace_id": "dev"}
        await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_default_session_does_not_patch_workspace(self, config):
        api_url = config.api_url
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "sess-token"})
        )
        patch_route = respx.patch(f"{api_url}/session").mock(
            return_value=httpx.Response(200, json={"workspace_id": "dev"})
        )
        respx.delete(f"{api_url}/logout").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = RequestContext(tool_name="t", tool_group="g")

        async with client.session(ctx):
            pass

        # Default ``dev_mode=False`` must NOT issue PATCH /session — Looker
        # sessions default to production workspace and that's the right
        # behavior for non-dev-mode tools.
        assert not patch_route.called
        await client.close()


class TestUseBranch:
    """``LookerSession.use_branch`` performs an in-session save → PUT
    target → yield → PUT saved cycle. The save+restore is what makes
    ``query(branch=…)`` safe for one-shot CI validation: even if the
    body raises, the workspace state is restored to whatever was checked
    out before the call."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_save_swap_restore_on_success(self):
        api_url = "https://test.looker.com/api/4.0"
        # GET current branch returns the saved name
        respx.get(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        # Two PUTs: one to feature-x (the swap), one back to main (the restore)
        put_route = respx.put(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )

        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "tok")
            async with session.use_branch("myproj", "feature-x"):
                pass

        # Two PUT calls: swap then restore.
        assert put_route.call_count == 2
        bodies = [json.loads(c.request.content.decode()) for c in put_route.calls]
        assert bodies == [{"name": "feature-x"}, {"name": "main"}]

    @pytest.mark.asyncio
    @respx.mock
    async def test_restore_runs_in_finally_when_body_raises(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        put_route = respx.put(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )

        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "tok")
            with pytest.raises(RuntimeError, match="boom"):
                async with session.use_branch("myproj", "feature-x"):
                    raise RuntimeError("boom")

        # Both swap AND restore happened despite the exception.
        assert put_route.call_count == 2
        bodies = [json.loads(c.request.content.decode()) for c in put_route.calls]
        assert bodies[1] == {"name": "main"}, "restore must run in finally"

    @pytest.mark.asyncio
    @respx.mock
    async def test_raises_when_current_branch_name_missing(self):
        # Defense against a Looker payload that omits ``name``: the swap
        # would silently put the workspace on the target branch, then
        # restore with ``{"name": None}`` (which Looker rejects), leaving
        # the workspace stuck on the caller-supplied branch. We fail fast
        # before the swap so atomic semantics aren't quietly broken.
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={})  # no "name" key
        )
        # PUT should NEVER be called — failure must precede any state mutation.
        put_route = respx.put(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )

        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "tok")
            with pytest.raises(LookerApiError) as exc_info:
                async with session.use_branch("myproj", "feature-x"):
                    pass
            assert "no current branch name" in exc_info.value.detail
        assert put_route.call_count == 0

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_op_when_already_on_target_branch(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )
        put_route = respx.put(f"{api_url}/projects/myproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )

        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "tok")
            async with session.use_branch("myproj", "feature-x"):
                pass

        # No swap, no restore — the branch was already where we wanted it.
        assert put_route.call_count == 0

    @pytest.mark.asyncio
    @respx.mock
    async def test_project_id_with_slash_is_path_encoded(self):
        api_url = "https://test.looker.com/api/4.0"
        # ``my/proj`` must be percent-encoded as ``my%2Fproj`` so it doesn't
        # misroute as a sub-path.
        respx.get(f"{api_url}/projects/my%2Fproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "main"})
        )
        respx.put(f"{api_url}/projects/my%2Fproj/git_branch").mock(
            return_value=httpx.Response(200, json={"name": "feature-x"})
        )

        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "tok")
            async with session.use_branch("my/proj", "feature-x"):
                pass


class TestLookerClientSudo:
    @pytest.mark.asyncio
    @respx.mock
    async def test_sudo_session(self, config):
        api_url = config.api_url
        # Admin login
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "admin-token"})
        )
        # Sudo login
        respx.post(f"{api_url}/login/42").mock(
            return_value=httpx.Response(200, json={"access_token": "sudo-token"})
        )
        # API call
        respx.get(f"{api_url}/lookml_models").mock(
            return_value=httpx.Response(200, json=[{"name": "m1"}])
        )
        # Two logouts (sudo + admin)
        respx.delete(f"{api_url}/logout").mock(return_value=httpx.Response(204))

        async def lookup(email: str) -> str | None:
            return "42"

        provider = SudoIdentityProvider("admin-id", "admin-secret", user_lookup_fn=lookup)
        client = LookerClient(config, provider)
        ctx = RequestContext(
            headers={"x-user-email": "user@example.com"},
            tool_name="query",
            tool_group="query",
        )

        async with client.session(ctx) as session:
            result = await session.get("/lookml_models")
            assert result == [{"name": "m1"}]

        await client.close()


class TestLookerClientOAuth:
    @pytest.mark.asyncio
    @respx.mock
    async def test_oauth_no_login_logout(self, config):
        api_url = config.api_url
        # No login/logout — just direct API call with OAuth token
        respx.get(f"{api_url}/lookml_models").mock(
            return_value=httpx.Response(200, json=[{"name": "m1"}])
        )

        provider = OAuthIdentityProvider(
            fallback_client_id="fb-id",
            fallback_client_secret="fb-secret",
        )
        client = LookerClient(config, provider)
        ctx = RequestContext(
            headers={"x-user-token": "oauth-token"},
            tool_name="list_models",
            tool_group="explore",
        )

        async with client.session(ctx) as session:
            result = await session.get("/lookml_models")
            assert result == [{"name": "m1"}]

        await client.close()


class TestFormatApiError:
    def test_format_looker_api_error_401(self):
        err = LookerApiError(401, "Unauthorized", "Bad credentials")
        result = json.loads(format_api_error("test_tool", err))
        assert "expired or invalid" in result["error"]
        assert result["status"] == 401
        assert result["detail"] == "Bad credentials"

    def test_format_looker_api_error_429(self):
        err = LookerApiError(429, "Too Many Requests")
        result = json.loads(format_api_error("test_tool", err))
        assert "Rate limited" in result["error"]

    def test_format_generic_error(self):
        err = RuntimeError("something broke")
        result = json.loads(format_api_error("test_tool", err))
        assert "something broke" in result["error"]


class TestCheckConnectivity:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_true_on_success(self, config):
        api_url = config.api_url
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(200, json={"access_token": "tok"})
        )
        respx.delete(f"{api_url}/logout").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("id", "secret")
        client = LookerClient(config, provider)
        assert await client.check_connectivity() is True
        await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_false_on_failure(self, config):
        api_url = config.api_url
        respx.post(f"{api_url}/login").mock(
            return_value=httpx.Response(401, json={"message": "bad creds"})
        )

        provider = ApiKeyIdentityProvider("id", "secret")
        client = LookerClient(config, provider)
        assert await client.check_connectivity() is False
        await client.close()

    @pytest.mark.asyncio
    async def test_returns_false_without_credentials(self):
        config = LookerConfig(
            base_url="https://test.looker.com",
            _env_file=None,  # type: ignore[call-arg]
        )
        provider = ApiKeyIdentityProvider("", "")
        client = LookerClient(config, provider)
        assert await client.check_connectivity() is False
        await client.close()
