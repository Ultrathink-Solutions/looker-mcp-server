"""Tests for identity providers."""

import pytest

from looker_mcp_server.identity import (
    ApiKeyIdentityProvider,
    ArgumentSudoIdentityProvider,
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
        # Header-driven sudo is tagged so audit logs can distinguish it
        # from argument-driven sudo (admin per-call impersonation).
        assert identity.triggered_by == "header"

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


class TestArgumentSudoIdentityProvider:
    """Per-call admin impersonation via the ``act_as_user`` argument."""

    @pytest.mark.asyncio
    async def test_passthrough_when_arg_absent(self):
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner, client_id="admin-id", client_secret="admin-secret"
        )
        ctx = RequestContext(tool_name="list_git_branches", tool_group="git")
        identity = await provider.resolve(ctx)
        # Inner identity is returned unchanged, including credentials —
        # we did NOT swap to admin-id when the argument is absent.
        assert identity.mode == "api_key"
        assert identity.client_id == "inner-id"
        assert identity.triggered_by is None

    @pytest.mark.asyncio
    async def test_passthrough_when_arg_empty_string(self):
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner, client_id="admin-id", client_secret="admin-secret"
        )
        # Whitespace-only is treated as absent — protects against the
        # common mistake of passing "" or "  " through a UI / config layer.
        ctx = RequestContext(
            tool_name="list_git_branches",
            tool_group="git",
            arguments={"act_as_user": "   "},
        )
        identity = await provider.resolve(ctx)
        assert identity.mode == "api_key"
        assert identity.client_id == "inner-id"

    @pytest.mark.asyncio
    async def test_resolves_sudo_with_numeric_user_id(self):
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner, client_id="admin-id", client_secret="admin-secret"
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "42"},
        )
        identity = await provider.resolve(ctx)

        # Numeric values bypass email lookup entirely.
        assert identity.mode == "sudo"
        assert identity.target_user_id == "42"
        assert identity.user_email is None
        assert identity.client_id == "admin-id"  # admin creds, not inner's
        assert identity.client_secret == "admin-secret"
        assert identity.triggered_by == "argument"

    @pytest.mark.asyncio
    async def test_resolves_sudo_via_email_lookup(self):
        async def lookup(email: str) -> str | None:
            return "99" if email == "ci-bot@example.com" else None

        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
            user_lookup_fn=lookup,
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "ci-bot@example.com"},
        )
        identity = await provider.resolve(ctx)

        assert identity.mode == "sudo"
        assert identity.target_user_id == "99"
        assert identity.user_email == "ci-bot@example.com"
        assert identity.triggered_by == "argument"

    @pytest.mark.asyncio
    async def test_email_lookup_miss_raises_value_error(self):
        # Fail-loud is deliberate: silently falling back to the inner
        # identity would let a typo'd email perform an action as the
        # *configured* identity instead of refusing.
        async def lookup(_email: str) -> str | None:
            return None

        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
            user_lookup_fn=lookup,
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "ghost@example.com"},
        )
        with pytest.raises(ValueError, match="no Looker user found for email"):
            await provider.resolve(ctx)

    @pytest.mark.asyncio
    async def test_email_lookup_caches(self):
        call_count = 0

        async def lookup(_email: str) -> str | None:
            nonlocal call_count
            call_count += 1
            return "7"

        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
            user_lookup_fn=lookup,
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "ci-bot@example.com"},
        )
        await provider.resolve(ctx)
        await provider.resolve(ctx)
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_set_user_lookup_propagates_to_inner(self):
        # The realistic chain at server bootstrap is
        # ArgumentSudo → DualMode → Sudo. A single call from the bootstrap
        # must wire the lookup_fn through every layer that needs it
        # (the wrapper itself, plus the inner SudoIdentityProvider that
        # DualMode delegates to).
        async def lookup(_email: str) -> str | None:
            return None

        inner = DualModeIdentityProvider(
            client_id="inner-id",
            client_secret="inner-secret",
        )
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
        )
        provider.set_user_lookup(lookup)

        # Wrapper got the function for its own email-resolution path.
        assert provider._user_lookup_fn is lookup
        # Propagated through DualMode to its underlying SudoIdentityProvider.
        assert inner._sudo._user_lookup_fn is lookup

    @pytest.mark.asyncio
    async def test_set_user_lookup_skips_inner_without_support(self):
        # When the inner provider doesn't expose set_user_lookup
        # (e.g. a third-party custom provider), propagation is a no-op
        # rather than raising — the wrapper still gets the function for
        # its own resolution path.
        async def lookup(_email: str) -> str | None:
            return None

        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
        )
        provider.set_user_lookup(lookup)
        assert provider._user_lookup_fn is lookup

    @pytest.mark.asyncio
    async def test_rejects_invalid_format(self):
        # A bare username (no ``@``, not all-digits) must be rejected
        # up front rather than forwarded to Looker's
        # ``/login/{value}`` endpoint where it would surface as an
        # opaque HTTP 400/404. The error message tells the caller the
        # accepted forms.
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner, client_id="admin-id", client_secret="admin-secret"
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "alice"},
        )
        with pytest.raises(ValueError, match="numeric Looker user ID"):
            await provider.resolve(ctx)

    @pytest.mark.asyncio
    async def test_refuses_when_sudo_disabled(self):
        # Honors the ``LOOKER_SUDO_AS_USER`` kill switch. The wrapper
        # is still installed at bootstrap so the request fails loudly
        # rather than silently routing under the configured identity.
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
            sudo_enabled=False,
        )
        ctx = RequestContext(
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "42"},
        )
        with pytest.raises(ValueError, match="LOOKER_SUDO_AS_USER"):
            await provider.resolve(ctx)

    @pytest.mark.asyncio
    async def test_sudo_disabled_passthrough_without_arg(self):
        # The gate only fires when the argument is present — absent
        # ``act_as_user`` it's transparent so existing flows aren't
        # affected by the wrapper being installed.
        inner = ApiKeyIdentityProvider("inner-id", "inner-secret")
        provider = ArgumentSudoIdentityProvider(
            inner=inner,
            client_id="admin-id",
            client_secret="admin-secret",
            sudo_enabled=False,
        )
        ctx = RequestContext(tool_name="list_git_branches", tool_group="git")
        identity = await provider.resolve(ctx)
        assert identity.mode == "api_key"
        assert identity.client_id == "inner-id"

    @pytest.mark.asyncio
    async def test_overrides_oauth_inner(self):
        # When the inner provider would resolve to OAuth (gateway pattern),
        # an explicit act_as_user still wins. Looker enforces whether the
        # configured admin credentials may impersonate.
        inner = OAuthIdentityProvider(
            fallback_client_id="fb-id", fallback_client_secret="fb-secret"
        )
        provider = ArgumentSudoIdentityProvider(
            inner=inner, client_id="admin-id", client_secret="admin-secret"
        )
        ctx = RequestContext(
            headers={"x-user-token": "gateway-token"},
            tool_name="delete_git_branch",
            tool_group="git",
            arguments={"act_as_user": "42"},
        )
        identity = await provider.resolve(ctx)
        assert identity.mode == "sudo"
        assert identity.target_user_id == "42"
        assert identity.triggered_by == "argument"
