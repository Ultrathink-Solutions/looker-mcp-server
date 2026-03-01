"""Tests for identity providers."""

import pytest

from looker_mcp_server.identity import (
    ApiKeyIdentityProvider,
    DualModeIdentityProvider,
    OAuthIdentityProvider,
    RequestContext,
    SudoIdentityProvider,
)


class TestApiKeyIdentityProvider:
    @pytest.mark.asyncio
    async def test_always_returns_api_key_mode(self):
        provider = ApiKeyIdentityProvider("id", "secret")
        ctx = RequestContext(tool_name="list_models", tool_group="explore")
        identity = await provider.resolve(ctx)

        assert identity.mode == "api_key"
        assert identity.client_id == "id"
        assert identity.client_secret == "secret"

    @pytest.mark.asyncio
    async def test_ignores_headers(self):
        provider = ApiKeyIdentityProvider("id", "secret")
        ctx = RequestContext(
            headers={"x-user-email": "user@example.com", "x-user-token": "tok"},
            tool_name="query",
            tool_group="query",
        )
        identity = await provider.resolve(ctx)
        assert identity.mode == "api_key"


class TestSudoIdentityProvider:
    @pytest.mark.asyncio
    async def test_falls_back_to_api_key_without_header(self):
        provider = SudoIdentityProvider("admin-id", "admin-secret")
        ctx = RequestContext(headers={}, tool_name="list_models", tool_group="explore")
        identity = await provider.resolve(ctx)

        assert identity.mode == "api_key"
        assert identity.client_id == "admin-id"

    @pytest.mark.asyncio
    async def test_falls_back_when_user_not_found(self):
        provider = SudoIdentityProvider("admin-id", "admin-secret")
        ctx = RequestContext(
            headers={"x-user-email": "unknown@example.com"},
            tool_name="query",
            tool_group="query",
        )
        identity = await provider.resolve(ctx)
        # No lookup function set and no cache hit → falls back
        assert identity.mode == "api_key"

    @pytest.mark.asyncio
    async def test_resolves_sudo_with_lookup(self):
        async def lookup(email: str) -> str | None:
            return "42" if email == "user@example.com" else None

        provider = SudoIdentityProvider("admin-id", "admin-secret", user_lookup_fn=lookup)
        ctx = RequestContext(
            headers={"x-user-email": "user@example.com"},
            tool_name="query",
            tool_group="query",
        )
        identity = await provider.resolve(ctx)

        assert identity.mode == "sudo"
        assert identity.target_user_id == "42"
        assert identity.user_email == "user@example.com"
        assert identity.client_id == "admin-id"

    @pytest.mark.asyncio
    async def test_caches_user_id(self):
        call_count = 0

        async def lookup(email: str) -> str | None:
            nonlocal call_count
            call_count += 1
            return "42"

        provider = SudoIdentityProvider("admin-id", "admin-secret", user_lookup_fn=lookup)
        ctx = RequestContext(
            headers={"x-user-email": "user@example.com"},
            tool_name="query",
            tool_group="query",
        )

        await provider.resolve(ctx)
        await provider.resolve(ctx)

        assert call_count == 1  # Second call should use cache


class TestOAuthIdentityProvider:
    @pytest.mark.asyncio
    async def test_returns_oauth_with_token(self):
        provider = OAuthIdentityProvider(
            token_header="X-User-Token",
            fallback_client_id="fb-id",
            fallback_client_secret="fb-secret",
        )
        ctx = RequestContext(
            headers={"x-user-token": "my-oauth-token"},
            tool_name="query",
            tool_group="query",
        )
        identity = await provider.resolve(ctx)

        assert identity.mode == "oauth"
        assert identity.access_token == "my-oauth-token"

    @pytest.mark.asyncio
    async def test_falls_back_to_api_key(self):
        provider = OAuthIdentityProvider(
            fallback_client_id="fb-id",
            fallback_client_secret="fb-secret",
        )
        ctx = RequestContext(headers={}, tool_name="query", tool_group="query")
        identity = await provider.resolve(ctx)

        assert identity.mode == "api_key"
        assert identity.client_id == "fb-id"

    @pytest.mark.asyncio
    async def test_raises_without_token_or_fallback(self):
        provider = OAuthIdentityProvider()
        ctx = RequestContext(headers={}, tool_name="query", tool_group="query")
        with pytest.raises(PermissionError):
            await provider.resolve(ctx)


class TestDualModeIdentityProvider:
    @pytest.mark.asyncio
    async def test_self_hosted_uses_sudo_path(self):
        provider = DualModeIdentityProvider(
            client_id="id",
            client_secret="secret",
            deployment_type="self_hosted",
        )
        # No email header → falls back to api_key (sudo provider fallback)
        ctx = RequestContext(headers={}, tool_name="query", tool_group="query")
        identity = await provider.resolve(ctx)
        assert identity.mode == "api_key"

    @pytest.mark.asyncio
    async def test_gc_core_uses_oauth_path(self):
        provider = DualModeIdentityProvider(
            client_id="id",
            client_secret="secret",
            deployment_type="google_cloud_core",
        )
        ctx = RequestContext(
            headers={"x-user-token": "gc-token"},
            tool_name="query",
            tool_group="query",
        )
        identity = await provider.resolve(ctx)
        assert identity.mode == "oauth"
        assert identity.access_token == "gc-token"

    @pytest.mark.asyncio
    async def test_gc_core_falls_back_without_token(self):
        provider = DualModeIdentityProvider(
            client_id="id",
            client_secret="secret",
            deployment_type="google_cloud_core",
        )
        ctx = RequestContext(headers={}, tool_name="query", tool_group="query")
        identity = await provider.resolve(ctx)
        assert identity.mode == "api_key"


class TestRequestContext:
    def test_defaults(self):
        ctx = RequestContext()
        assert ctx.headers == {}
        assert ctx.tool_name == ""
        assert ctx.tool_group == ""
        assert ctx.arguments == {}

    def test_frozen(self):
        ctx = RequestContext(tool_name="query")
        with pytest.raises(AttributeError):
            ctx.tool_name = "other"  # type: ignore[misc]
