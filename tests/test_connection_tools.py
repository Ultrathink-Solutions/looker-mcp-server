"""Tests for connection tool group — database connection management."""

import json

import httpx
import pytest
import respx

from looker_mcp_server.client import LookerApiError, LookerClient, format_api_error
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
    mcp, client = create_server(config, enabled_groups={"connection"})
    return mcp, client


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    """Set up login/logout mocks for API-key auth sessions."""
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestServerRegistration:
    def test_connection_group_registers(self, server_and_client):
        mcp, _ = server_and_client
        assert mcp is not None

    def test_connection_is_in_all_groups(self):
        from looker_mcp_server.config import ALL_GROUPS

        assert "connection" in ALL_GROUPS


class TestGetConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_connection_details(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/connections/warehouse").mock(
            return_value=httpx.Response(
                200,
                json={
                    "name": "warehouse",
                    "dialect_name": "snowflake",
                    "host": "db.example.com",
                    "database": "analytics",
                    "schema": "public",
                    "pdts_enabled": True,
                },
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                conn = await session.get("/connections/warehouse")
                assert conn["name"] == "warehouse"
                assert conn["dialect_name"] == "snowflake"
                assert conn["pdts_enabled"] is True
        finally:
            await client.close()


class TestListConnectionDialects:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_dialects(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/dialect_info").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "name": "snowflake",
                        "label": "Snowflake",
                        "default_max_connections": 50,
                        "supported_options": {"pdts": True, "oauth": True},
                    },
                    {
                        "name": "bigquery_standard_sql",
                        "label": "Google BigQuery Standard SQL",
                        "default_max_connections": 10,
                        "supported_options": {"pdts": True, "oauth": False},
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_connection_dialects", "connection")
        try:
            async with client.session(ctx) as session:
                dialects = await session.get("/dialect_info")
                assert len(dialects) == 2
                assert dialects[0]["name"] == "snowflake"
        finally:
            await client.close()


class TestCreateConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_sends_only_provided_fields(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                201,
                json={"name": "warehouse", "dialect_name": "snowflake"},
            )

        respx.post(f"{API_URL}/connections").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                body = {
                    "name": "warehouse",
                    "dialect_name": "snowflake",
                    "host": "db.example.com",
                    "database": "analytics",
                }
                conn = await session.post("/connections", body=body)
                assert conn["name"] == "warehouse"

                # Unprovided optional fields must NOT be serialized as null —
                # Looker interprets null as "clear this field."
                assert captured["body"] == {
                    "name": "warehouse",
                    "dialect_name": "snowflake",
                    "host": "db.example.com",
                    "database": "analytics",
                }
                assert "port" not in captured["body"]
                assert "password" not in captured["body"]
        finally:
            await client.close()


class TestUpdateConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_only_provided_fields(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"name": "warehouse"})

        respx.patch(f"{API_URL}/connections/warehouse").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("update_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                await session.patch(
                    "/connections/warehouse",
                    body={"password": "new-secret"},
                )
                assert captured["body"] == {"password": "new-secret"}
        finally:
            await client.close()


class TestDeleteConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_success_envelope(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/connections/warehouse").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("delete_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                result = await session.delete("/connections/warehouse")
                assert result is None
        finally:
            await client.close()


class TestTestConnection:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_per_check_breakdown(self, config):
        _mock_login_logout()
        respx.put(f"{API_URL}/connections/warehouse/test").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"name": "connect", "status": "success", "message": "Can connect"},
                    {"name": "query", "status": "success", "message": "Can run queries"},
                    {
                        "name": "tmp_table",
                        "status": "error",
                        "message": "User lacks CREATE on scratch schema",
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("test_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                results = await session.put("/connections/warehouse/test")
                statuses = {r["name"]: r["status"] for r in results}
                assert statuses["connect"] == "success"
                assert statuses["tmp_table"] == "error"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_passes_tests_subset_param(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(
                200,
                json=[{"name": "connect", "status": "success", "message": "OK"}],
            )

        respx.put(f"{API_URL}/connections/warehouse/test").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("test_connection", "connection", {"name": "warehouse"})
        try:
            async with client.session(ctx) as session:
                await session.put(
                    "/connections/warehouse/test",
                    params={"tests": "connect,query"},
                )
                assert "tests=connect" in captured["url"]
                assert "query" in captured["url"]
        finally:
            await client.close()


class TestPathEncoding:
    """Connection names are free-text strings and must be URL-encoded."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_encodes_special_characters(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            # raw_path preserves percent-encoding; .path decodes it.
            captured["raw_path"] = request.url.raw_path.decode("ascii")
            return httpx.Response(200, json={"name": "data warehouse"})

        respx.get(url__regex=rf"{API_URL}/connections/.*").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("get_connection", "connection", {"name": "data warehouse"})
        try:
            from urllib.parse import quote

            async with client.session(ctx) as session:
                await session.get(f"/connections/{quote('data warehouse', safe='')}")
                assert "data%20warehouse" in captured["raw_path"]
                assert " " not in captured["raw_path"]
        finally:
            await client.close()


class TestErrorFormatting:
    def test_404_returns_actionable_hint(self):
        error = LookerApiError(404, "Not Found", "Connection 'warehouse' does not exist.")
        result = json.loads(format_api_error("get_connection", error))
        assert result["status"] == 404
        assert "not found" in result["error"].lower()
        assert "warehouse" in result["detail"]

    def test_400_returns_invalid_params_hint(self):
        error = LookerApiError(400, "Bad Request", "dialect_name is required")
        result = json.loads(format_api_error("create_connection", error))
        assert result["status"] == 400
        assert "invalid" in result["error"].lower()
