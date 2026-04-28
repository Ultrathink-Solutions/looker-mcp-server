"""Tests for connection tool group — database connection management."""

import json

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.client import LookerApiError, LookerClient, format_api_error
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import ApiKeyIdentityProvider
from looker_mcp_server.server import create_server
from looker_mcp_server.tools.connection import WRITABLE_DBCONNECTION_FIELDS


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


# WRITABLE_DBCONNECTION_FIELDS is imported from
# ``looker_mcp_server.tools.connection`` — a single source of truth so the
# runtime ``clear_fields`` validator and the schema-contract tests can never
# drift. A regression (a field silently dropped on a refactor) must fail
# loudly in the tests below.

# Fields the Looker spec marks readOnly. Sending them is a no-op on Looker's
# side, so we must NOT advertise them as writable inputs (avoids false-success
# user expectations like "I set pdts_enabled=true but nothing happened").
READONLY_DBCONNECTION_FIELDS = {
    "pdts_enabled",
    "uses_oauth",
    "has_password",
    "uses_instance_oauth",
    "uses_service_auth",
    "snippets",
    "managed",
    "example",
    "supports_data_studio_link",
    "named_driver_version_actual",
    "created_at",
    "user_id",
    "last_regen_at",
    "last_reap_at",
    "default_bq_connection",
    "p4sa_name",
}


class TestCreateConnectionToolSchema:
    """The MCP tool surface is the contract — verify all DBConnection writable
    fields are reachable, and no read-only fields are mistakenly accepted as
    inputs. The surface is what an agent sees; tests on the HTTP layer alone
    don't catch a missing tool parameter.
    """

    @pytest.mark.asyncio
    async def test_create_connection_exposes_exact_writable_fields(self, server_and_client):
        # Exact-equality assertion: a refactor that *adds* an unexpected param
        # (typo, leaked internal, mistake) must fail just as loudly as a
        # refactor that drops a documented one.
        mcp, _ = server_and_client
        tools = {t.name: t for t in await mcp.list_tools()}
        assert "create_connection" in tools

        props = tools["create_connection"].parameters["properties"]
        expected = WRITABLE_DBCONNECTION_FIELDS | {"name", "dialect_name"}
        assert props.keys() == expected, (
            f"create_connection surface drift — "
            f"missing: {sorted(expected - props.keys())}, "
            f"unexpected: {sorted(props.keys() - expected)}"
        )
        # Read-only fields must never leak (subsumed by the exact-equality
        # check above, but kept as a more readable failure message).
        leaked = READONLY_DBCONNECTION_FIELDS & props.keys()
        assert not leaked, f"create_connection exposes read-only fields: {sorted(leaked)}"

    @pytest.mark.asyncio
    async def test_update_connection_exposes_exact_writable_fields(self, server_and_client):
        mcp, _ = server_and_client
        tools = {t.name: t for t in await mcp.list_tools()}
        assert "update_connection" in tools

        props = tools["update_connection"].parameters["properties"]
        # update has: name (URL key) + every writable field + clear_fields
        # (the field-clearing escape hatch for nulling previously-set values).
        # No dialect_name — it's write-once at create.
        expected = WRITABLE_DBCONNECTION_FIELDS | {"name", "clear_fields"}
        assert props.keys() == expected, (
            f"update_connection surface drift — "
            f"missing: {sorted(expected - props.keys())}, "
            f"unexpected: {sorted(props.keys() - expected)}"
        )
        assert "dialect_name" not in props, (
            "dialect_name is write-once at create — exposing it on update implies "
            "it can be changed, which Looker does not support."
        )
        leaked = READONLY_DBCONNECTION_FIELDS & props.keys()
        assert not leaked, f"update_connection exposes read-only fields: {sorted(leaked)}"


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


# ── Field-clearing semantics on update_connection ───────────────────────


def _invoke_tool(mcp, tool_name: str, args: dict):
    """Call a tool through the MCP server and return the parsed payload."""

    async def _run():
        async with Client(mcp) as mcp_client:
            result = await mcp_client.call_tool(tool_name, args)
            content = result.content[0]
            assert isinstance(content, TextContent)
            return json.loads(content.text)

    return _run


class TestUpdateConnectionClearFields:
    """The `_set_if` helper drops `None`, which means a plain
    update_connection(host=None) call is "no-op" rather than "clear host."
    `clear_fields` is the explicit escape hatch that puts a JSON `null` on
    the wire so Looker reverts the field to its dialect default. Without
    this, agents have no way to undo a previously-set oauth_application_id
    / service_name / tunnel_id / etc.
    """

    @pytest.mark.asyncio
    @respx.mock
    async def test_clear_fields_serialize_as_json_null(self, server_and_client):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            # Use the raw bytes so we can verify `null` survives serialization
            # (json.loads decodes it back to Python None, which is the wire shape).
            captured["body_bytes"] = request.content.decode()
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"name": "warehouse"})

        respx.patch(f"{API_URL}/connections/warehouse").mock(side_effect=capture)

        mcp, _ = server_and_client
        payload = await _invoke_tool(
            mcp,
            "update_connection",
            {
                "name": "warehouse",
                "host": "newhost.example.com",
                "clear_fields": ["oauth_application_id", "service_name"],
            },
        )()
        assert payload["updated"] is True
        # The set field carries its value.
        assert captured["body"]["host"] == "newhost.example.com"
        # Cleared fields are present in the body as JSON null (not absent).
        assert captured["body"]["oauth_application_id"] is None
        assert captured["body"]["service_name"] is None
        # Confirm null is on the wire — guards against silent stripping by
        # any future serialization layer. (httpx serializes JSON compactly,
        # so no space between key and value.)
        assert '"oauth_application_id":null' in captured["body_bytes"]
        # fields_changed in the response covers both set and cleared keys.
        assert "oauth_application_id" in payload["fields_changed"]
        assert "service_name" in payload["fields_changed"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_clear_fields_rejects_unknown_field(self, server_and_client):
        # Catches typos before they reach Looker (which would 400 with a less
        # actionable message). The error includes the offending names so the
        # caller can see what to fix.
        _mock_login_logout()
        # No PATCH mock — validation must short-circuit before any PATCH.

        mcp, _ = server_and_client
        payload = await _invoke_tool(
            mcp,
            "update_connection",
            {"name": "warehouse", "clear_fields": ["not_a_field", "host"]},
        )()
        assert "Invalid field name" in payload["error"]
        assert "not_a_field" in payload["invalid"]
        # 'host' is a valid writable field so it should NOT be flagged.
        assert "host" not in payload["invalid"]
        patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
        assert patch_calls == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_clear_fields_rejects_set_and_clear_conflict(self, server_and_client):
        # Setting host="x" AND asking to clear "host" is contradictory — the
        # tool returns an actionable error rather than picking one and
        # silently doing the wrong thing.
        _mock_login_logout()

        mcp, _ = server_and_client
        payload = await _invoke_tool(
            mcp,
            "update_connection",
            {"name": "warehouse", "host": "x.example.com", "clear_fields": ["host"]},
        )()
        assert "Cannot both set and clear" in payload["error"]
        assert "host" in payload["conflicts"]
        patch_calls = [c for c in respx.calls if c.request.method == "PATCH"]
        assert patch_calls == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_clear_fields_alone_is_a_valid_update(self, server_and_client):
        # An update that ONLY clears (no value parameters set) is still a
        # legitimate operation — must NOT trip the "no fields" guard.
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"name": "warehouse"})

        respx.patch(f"{API_URL}/connections/warehouse").mock(side_effect=capture)

        mcp, _ = server_and_client
        payload = await _invoke_tool(
            mcp,
            "update_connection",
            {"name": "warehouse", "clear_fields": ["tunnel_id"]},
        )()
        assert payload["updated"] is True
        assert captured["body"] == {"tunnel_id": None}

    def test_writable_fields_constant_matches_test_expectation(self):
        # If the source-of-truth set drifts, the test contract drifts with it.
        # This guard makes the drift explicit.
        assert "oauth_application_id" in WRITABLE_DBCONNECTION_FIELDS
        assert "name" not in WRITABLE_DBCONNECTION_FIELDS  # write-once, URL key
        assert "dialect_name" not in WRITABLE_DBCONNECTION_FIELDS  # write-once
        assert "pdts_enabled" not in WRITABLE_DBCONNECTION_FIELDS  # readOnly
        assert "uses_oauth" not in WRITABLE_DBCONNECTION_FIELDS  # readOnly
