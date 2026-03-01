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
