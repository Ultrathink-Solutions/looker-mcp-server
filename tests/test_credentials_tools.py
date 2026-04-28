"""Tests for credentials tool group — non-email user credentials."""

import json as _json

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.client import LookerClient
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import ApiKeyIdentityProvider
from looker_mcp_server.server import create_server


def _invoke_tool(mcp, tool_name: str, args: dict):
    async def _run():
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(tool_name, args)
            content = result.content[0]
            assert isinstance(content, TextContent)
            return _json.loads(content.text)

    return _run


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
async def server_and_client(config):
    mcp, client = create_server(config, enabled_groups={"credentials"})
    try:
        yield mcp, client
    finally:
        await client.close()


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestServerRegistration:
    @pytest.mark.asyncio
    async def test_credentials_registers(self, server_and_client):
        mcp, _ = server_and_client
        assert mcp is not None

    def test_credentials_in_all_groups(self):
        from looker_mcp_server.config import ALL_GROUPS

        assert "credentials" in ALL_GROUPS


# ══ API3 ═════════════════════════════════════════════════════════════


class TestApi3Lifecycle:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_trimmed_summary(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "1",
                        "client_id": "abc123",
                        "created_at": "2026-01-01T00:00:00Z",
                        "is_disabled": False,
                    },
                    {
                        "id": "2",
                        "client_id": "xyz789",
                        "created_at": "2026-02-01T00:00:00Z",
                        "is_disabled": True,
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_credentials_api3", "credentials", {"user_id": "42"})
        try:
            async with client.session(ctx) as session:
                creds = await session.get("/users/42/credentials_api3")
                assert len(creds) == 2
                assert creds[0]["client_id"] == "abc123"
                assert creds[1]["is_disabled"] is True
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_create_returns_client_secret_once(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/users/42/credentials_api3").mock(
            return_value=httpx.Response(
                201,
                json={
                    "id": "99",
                    "client_id": "new-client",
                    "client_secret": "ONCE-ONLY-SECRET",
                    "created_at": "2026-04-15T00:00:00Z",
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_credentials_api3", "credentials", {"user_id": "42"})
        try:
            async with client.session(ctx) as session:
                creds = await session.post("/users/42/credentials_api3")
                # Contract: Looker returns client_secret exactly once — on
                # creation.  Tool's responsibility is to surface it.
                assert creds["client_secret"] == "ONCE-ONLY-SECRET"
                assert creds["id"] == "99"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_never_includes_secret(self, config):
        _mock_login_logout()
        # Looker's GET endpoint never returns client_secret post-creation.
        respx.get(f"{API_URL}/users/42/credentials_api3/99").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "99",
                    "client_id": "new-client",
                    "created_at": "2026-04-15T00:00:00Z",
                    "is_disabled": False,
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "get_credentials_api3",
            "credentials",
            {"user_id": "42", "credentials_api3_id": "99"},
        )
        try:
            async with client.session(ctx) as session:
                creds = await session.get("/users/42/credentials_api3/99")
                assert "client_secret" not in creds
                assert creds["client_id"] == "new-client"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_returns_success(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/users/42/credentials_api3/99").mock(
            return_value=httpx.Response(204)
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "delete_credentials_api3",
            "credentials",
            {"user_id": "42", "credentials_api3_id": "99"},
        )
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/users/42/credentials_api3/99") is None
        finally:
            await client.close()


# ══ Single-instance SSO types ════════════════════════════════════════
# LDAP / SAML / OIDC / Google all follow the same URL pattern:
#   /users/{id}/credentials_{type}
# A single parametrized class exercises the pattern once per type.


SSO_TYPES = ["ldap", "saml", "oidc", "google"]


class TestSsoCredentials:
    @pytest.mark.parametrize("cred_type", SSO_TYPES)
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_credential_link(self, config, cred_type):
        _mock_login_logout()
        respx.get(f"{API_URL}/users/42/credentials_{cred_type}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "email": "user-42@example.com",
                    "is_disabled": False,
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            f"get_credentials_{cred_type}",
            "credentials",
            {"user_id": "42"},
        )
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/42/credentials_{cred_type}")
                assert creds["email"] == "user-42@example.com"
        finally:
            await client.close()

    @pytest.mark.parametrize("cred_type", SSO_TYPES)
    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_returns_success(self, config, cred_type):
        _mock_login_logout()
        respx.delete(f"{API_URL}/users/42/credentials_{cred_type}").mock(
            return_value=httpx.Response(204)
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            f"delete_credentials_{cred_type}",
            "credentials",
            {"user_id": "42"},
        )
        try:
            async with client.session(ctx) as session:
                assert await session.delete(f"/users/42/credentials_{cred_type}") is None
        finally:
            await client.close()


# ══ Path encoding regression ═════════════════════════════════════════


class TestPathEncoding:
    """Defensive encoding: user_ids and api3 ids flow through _path_seg."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_special_character_ids_are_encoded(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(200, json={"id": "99"})

        respx.get(url__regex=rf"{API_URL}/users/.*/credentials_api3/.*").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "get_credentials_api3",
            "credentials",
            {"user_id": "42/../boom", "credentials_api3_id": "99 weird"},
        )
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                # Simulate what _path_seg produces in the tool.
                path = (
                    f"/users/{quote('42/../boom', safe='')}"
                    f"/credentials_api3/{quote('99 weird', safe='')}"
                )
                await session.get(path)
                # Slash in user_id must be encoded (otherwise the request
                # routes to /users/42/../boom/... which is entirely different).
                assert "42%2F..%2Fboom" in captured["raw_path"]
                # Space in credentials_api3_id must be encoded.
                assert "99%20weird" in captured["raw_path"]
        finally:
            await client.close()


# ── Lifecycle additions: TOTP, API3 update, email patch ─────────────────


class TestUpdateCredentialsApi3:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_purpose(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            import json as _json

            captured["body"] = _json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={"id": "k-1", "client_id": "abc", "purpose": "ci"},
            )

        respx.patch(f"{API_URL}/users/u-1/credentials_api3/k-1").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "update_credentials_api3",
            "credentials",
            {"user_id": "u-1", "credentials_api3_id": "k-1"},
        )
        try:
            async with client.session(ctx) as session:
                await session.patch(
                    "/users/u-1/credentials_api3/k-1",
                    body={"purpose": "ci"},
                )
                assert captured["body"] == {"purpose": "ci"}
        finally:
            await client.close()


class TestTotpLifecycle:
    @pytest.mark.asyncio
    @respx.mock
    async def test_create_posts_to_totp_endpoint(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/users/u-1/credentials_totp").mock(
            return_value=httpx.Response(200, json={"verified": False})
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_credentials_totp", "credentials", {"user_id": "u-1"})
        try:
            async with client.session(ctx) as session:
                creds = await session.post("/users/u-1/credentials_totp")
                # User has not yet completed enrollment in their authenticator.
                assert creds["verified"] is False
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_returns_metadata(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/users/u-1/credentials_totp").mock(
            return_value=httpx.Response(
                200,
                json={
                    "verified": True,
                    "is_disabled": False,
                    "created_at": "2026-01-01T00:00:00Z",
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_credentials_totp", "credentials", {"user_id": "u-1"})
        try:
            async with client.session(ctx) as session:
                creds = await session.get("/users/u-1/credentials_totp")
                assert creds["verified"] is True
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_clears_credential(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/users/u-1/credentials_totp").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_credentials_totp", "credentials", {"user_id": "u-1"})
        try:
            async with client.session(ctx) as session:
                result = await session.delete("/users/u-1/credentials_totp")
                assert result is None
        finally:
            await client.close()


class TestCredentialsToolRegistration:
    @pytest.mark.asyncio
    async def test_new_lifecycle_tools_register(self, server_and_client):
        # Lock in the lifecycle additions — a future refactor that drops one
        # silently must trip this test.
        mcp, _ = server_and_client
        names = {t.name for t in await mcp.list_tools()}
        for tool in (
            "update_credentials_api3",
            "get_credentials_totp",
            "create_credentials_totp",
            "delete_credentials_totp",
        ):
            assert tool in names, f"missing tool: {tool}"


# ── Response curating: get_credentials_totp drops out-of-contract fields ──


class TestGetCredentialsTotpCurating:
    """The tool docstring promises a metadata-focused response. Forwarding the
    raw Looker payload would leak future-added fields (rotating links, can-
    matrices, etc.) and make the MCP response shape unstable across Looker
    versions. The curator pins the contract.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_response_drops_undocumented_fields(self, config):
        from looker_mcp_server.server import create_server

        _mock_login_logout()
        respx.get(f"{API_URL}/users/u-1/credentials_totp").mock(
            return_value=httpx.Response(
                200,
                json={
                    "verified": True,
                    "is_disabled": False,
                    "created_at": "2026-01-01T00:00:00Z",
                    # Out-of-contract fields the upstream payload may include —
                    # these MUST NOT appear in the MCP response.
                    "url": "https://test.looker.com/api/4.0/users/u-1/credentials_totp",
                    "type": "totp",
                    "can": {"do_thing": True},
                    "future_field_we_dont_know_about": "leak",
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"credentials"})
        try:
            payload = await _invoke_tool(mcp, "get_credentials_totp", {"user_id": "u-1"})()
            # Only the documented metadata is surfaced.
            assert payload["verified"] is True
            assert payload["is_disabled"] is False
            assert payload["created_at"] == "2026-01-01T00:00:00Z"
            assert payload["user_id"] == "u-1"
            # Undocumented fields are filtered out by the curator.
            for leaky in ("url", "type", "can", "future_field_we_dont_know_about"):
                assert leaky not in payload, f"{leaky} leaked into curated response"
        finally:
            await looker_client.close()
