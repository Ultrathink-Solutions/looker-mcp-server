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
    async def test_error_carries_full_json_body(self):
        api_url = "https://test.looker.com/api/4.0"
        # Looker returns the compiled SQL and LookML evaluator errors in
        # the 400 body for query failures. Callers need access to these.
        full_body = {
            "message": "Query failed with unexpected exception …",
            "sql": "SELECT 1 FROM dual",
            "errors": [{"message": "type mismatch"}],
            "applied_filters": {"orders.created_date": "today"},
        }
        respx.get(f"{api_url}/queries/1/run/json").mock(
            return_value=httpx.Response(400, json=full_body)
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            with pytest.raises(LookerApiError) as exc_info:
                await session.get("/queries/1/run/json")
            assert exc_info.value.status_code == 400
            assert exc_info.value.detail == full_body["message"]
            assert exc_info.value.body == full_body

    @pytest.mark.asyncio
    @respx.mock
    async def test_error_with_plain_text_body_leaves_body_none(self):
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/projects/p/git/deploy_key").mock(
            return_value=httpx.Response(404, text="No deploy key configured")
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            with pytest.raises(LookerApiError) as exc_info:
                await session.get_text("/projects/p/git/deploy_key")
            assert exc_info.value.status_code == 404
            assert "No deploy key configured" in exc_info.value.detail
            assert exc_info.value.body is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_error_with_array_body_leaves_body_none(self):
        # Some Looker endpoints occasionally return JSON arrays on error.
        # The contract is "body is a dict or it's None" — array responses
        # populate detail (truncated repr) but not body.
        api_url = "https://test.looker.com/api/4.0"
        respx.get(f"{api_url}/something").mock(
            return_value=httpx.Response(400, json=["error1", "error2"])
        )
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            with pytest.raises(LookerApiError) as exc_info:
                await session.get("/something")
            assert exc_info.value.body is None
            assert "error1" in exc_info.value.detail

    @pytest.mark.asyncio
    @respx.mock
    async def test_error_with_non_string_message_coerces_detail_to_string(self):
        # If Looker ever returns a non-string ``message`` (e.g. a nested
        # error object), ``detail`` must remain a string for downstream
        # consumers. The full payload is still preserved on ``body``.
        api_url = "https://test.looker.com/api/4.0"
        full_body = {
            "message": {"code": "X1", "text": "structured error"},
            "errors": [{"message": "downstream"}],
        }
        respx.get(f"{api_url}/something").mock(return_value=httpx.Response(400, json=full_body))
        async with httpx.AsyncClient(base_url=api_url) as http:
            session = LookerSession(http, "test-token")
            with pytest.raises(LookerApiError) as exc_info:
                await session.get("/something")
            assert isinstance(exc_info.value.detail, str)
            assert "structured error" in exc_info.value.detail
            # Full structured payload still accessible via body.
            assert exc_info.value.body == full_body

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

    def test_format_includes_full_body_when_present(self):
        # Mimics a Looker query failure: detail message is generic but the
        # body carries the compiled SQL, the LookML evaluator errors[], and
        # applied_filters — the high-signal debugging payload that callers
        # otherwise have to re-fetch via raw REST.
        body = {
            "message": "Query failed with unexpected exception …",
            "sql": "SELECT region, SUM(total) FROM orders GROUP BY 1",
            "errors": [
                {"message": "wrong argument type NilClass (expected Integer)"},
            ],
            "applied_filters": {"orders.created_date": "90 days"},
        }
        err = LookerApiError(400, "Bad Request", "Query failed …", body=body)
        result = json.loads(format_api_error("query", err))
        assert result["status"] == 400
        assert result["body"] == body
        assert result["body"]["sql"].startswith("SELECT region")
        assert result["body"]["errors"][0]["message"].startswith("wrong argument type")

    def test_format_omits_body_when_absent(self):
        err = LookerApiError(401, "Unauthorized", "Bad creds")
        result = json.loads(format_api_error("test_tool", err))
        assert "body" not in result


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
