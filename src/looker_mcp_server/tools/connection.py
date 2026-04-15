"""Connection tool group — database connection management.

Covers full CRUD for Looker database connections plus the ``test`` endpoint
that validates connectivity and PDT permissions.  These tools require admin
permission in Looker and are disabled by default.  Enable with
``--groups connection`` (or ``all``).
"""

from __future__ import annotations

import json
from typing import Annotated, Any
from urllib.parse import quote

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _set_if


def register_connection_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Read ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get full configuration for a specific database connection, including "
            "dialect, host, database, PDT settings, and usage attributes. For a "
            "trimmed summary of all connections, use ``list_connections`` in the "
            "explore group instead."
        ),
    )
    async def get_connection(
        name: Annotated[str, "Connection name"],
    ) -> str:
        ctx = client.build_context("get_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                conn = await session.get(f"/connections/{quote(name, safe='')}")
                return json.dumps(conn, indent=2)
        except Exception as e:
            return format_api_error("get_connection", e)

    @server.tool(
        description=(
            "List the database dialects supported by this Looker instance along with "
            "the fields each one accepts. Useful before calling ``create_connection`` "
            "to discover valid ``dialect_name`` values and dialect-specific options."
        ),
    )
    async def list_connection_dialects() -> str:
        ctx = client.build_context("list_connection_dialects", "connection")
        try:
            async with client.session(ctx) as session:
                dialects = await session.get("/dialect_info")
                result = [
                    {
                        "name": d.get("name"),
                        "label": d.get("label"),
                        "default_max_connections": d.get("default_max_connections"),
                        "supported_options": d.get("supported_options"),
                    }
                    for d in (dialects or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_connection_dialects", e)

    # ── Create ───────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Create a new database connection. After creation, call "
            "``test_connection`` to verify reachability and PDT permissions. "
            "Call ``list_connection_dialects`` first if you need valid dialect names."
        ),
    )
    async def create_connection(
        name: Annotated[str, "Unique connection name (used by LookML ``connection:`` param)"],
        dialect_name: Annotated[str, "Database dialect, e.g. 'snowflake', 'bigquery_standard_sql'"],
        host: Annotated[str | None, "Database host or endpoint"] = None,
        port: Annotated[int | None, "Database port"] = None,
        database: Annotated[str | None, "Default database name"] = None,
        username: Annotated[str | None, "Database username"] = None,
        password: Annotated[str | None, "Database password (write-only)"] = None,
        schema: Annotated[str | None, "Default schema"] = None,
        tmp_db_name: Annotated[str | None, "Scratch schema/database for PDTs"] = None,
        jdbc_additional_params: Annotated[
            str | None, "Extra JDBC parameters, semicolon-separated"
        ] = None,
        ssl: Annotated[bool | None, "Use SSL/TLS for the connection"] = None,
        verify_ssl: Annotated[bool | None, "Verify the server's SSL certificate"] = None,
        max_connections: Annotated[int | None, "Maximum pool size"] = None,
        pool_timeout: Annotated[int | None, "Pool checkout timeout in seconds"] = None,
        pdts_enabled: Annotated[bool | None, "Enable persistent derived tables"] = None,
        uses_oauth: Annotated[bool | None, "Connection authenticates via OAuth"] = None,
    ) -> str:
        ctx = client.build_context("create_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "dialect_name": dialect_name}
                _set_if(body, "host", host)
                _set_if(body, "port", port)
                _set_if(body, "database", database)
                _set_if(body, "username", username)
                _set_if(body, "password", password)
                _set_if(body, "schema", schema)
                _set_if(body, "tmp_db_name", tmp_db_name)
                _set_if(body, "jdbc_additional_params", jdbc_additional_params)
                _set_if(body, "ssl", ssl)
                _set_if(body, "verify_ssl", verify_ssl)
                _set_if(body, "max_connections", max_connections)
                _set_if(body, "pool_timeout", pool_timeout)
                _set_if(body, "pdts_enabled", pdts_enabled)
                _set_if(body, "uses_oauth", uses_oauth)

                conn = await session.post("/connections", body=body)
                return json.dumps(
                    {
                        "name": conn.get("name"),
                        "dialect_name": conn.get("dialect_name"),
                        "created": True,
                        "next_step": (
                            "Run test_connection to verify the new connection is usable."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_connection", e)

    # ── Update ───────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Update an existing database connection.  Only provided fields are "
            "changed — omitted fields are preserved.  After updating credentials "
            "or host, call ``test_connection`` to verify the change is healthy."
        ),
    )
    async def update_connection(
        name: Annotated[str, "Connection name to update"],
        host: Annotated[str | None, "New host"] = None,
        port: Annotated[int | None, "New port"] = None,
        database: Annotated[str | None, "New database name"] = None,
        username: Annotated[str | None, "New username"] = None,
        password: Annotated[str | None, "New password (write-only)"] = None,
        schema: Annotated[str | None, "New default schema"] = None,
        tmp_db_name: Annotated[str | None, "New scratch schema/database for PDTs"] = None,
        jdbc_additional_params: Annotated[str | None, "New extra JDBC parameters"] = None,
        ssl: Annotated[bool | None, "Toggle SSL/TLS"] = None,
        verify_ssl: Annotated[bool | None, "Toggle SSL certificate verification"] = None,
        max_connections: Annotated[int | None, "New maximum pool size"] = None,
        pool_timeout: Annotated[int | None, "New pool checkout timeout"] = None,
        pdts_enabled: Annotated[bool | None, "Toggle persistent derived tables"] = None,
    ) -> str:
        ctx = client.build_context("update_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "host", host)
                _set_if(body, "port", port)
                _set_if(body, "database", database)
                _set_if(body, "username", username)
                _set_if(body, "password", password)
                _set_if(body, "schema", schema)
                _set_if(body, "tmp_db_name", tmp_db_name)
                _set_if(body, "jdbc_additional_params", jdbc_additional_params)
                _set_if(body, "ssl", ssl)
                _set_if(body, "verify_ssl", verify_ssl)
                _set_if(body, "max_connections", max_connections)
                _set_if(body, "pool_timeout", pool_timeout)
                _set_if(body, "pdts_enabled", pdts_enabled)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one of: host, port, database, username, "
                                "password, schema, tmp_db_name, jdbc_additional_params, "
                                "ssl, verify_ssl, max_connections, pool_timeout, pdts_enabled."
                            ),
                        },
                        indent=2,
                    )

                conn = await session.patch(f"/connections/{quote(name, safe='')}", body=body)
                return json.dumps(
                    {
                        "name": conn.get("name"),
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_connection", e)

    # ── Delete ───────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Delete a database connection. LookML models referencing this "
            "connection will fail to build until they are pointed at another "
            "connection. This action cannot be undone."
        ),
    )
    async def delete_connection(
        name: Annotated[str, "Connection name to delete"],
    ) -> str:
        ctx = client.build_context("delete_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/connections/{quote(name, safe='')}")
                return json.dumps({"deleted": True, "name": name}, indent=2)
        except Exception as e:
            return format_api_error("delete_connection", e)

    # ── Test ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Run Looker's built-in health checks against a database connection "
            "(connect, query, temp table, CDT/PDT, kill). Each check returns its "
            "own status — the connection is only fully healthy when every "
            "requested check passes. Use the per-check breakdown to decide which "
            "connection parameter to correct (e.g. if ``tmp_table`` fails but "
            "``connect`` passes, the credentials work but ``tmp_db_name`` is "
            "missing or the user lacks CREATE permission)."
        ),
    )
    async def test_connection(
        name: Annotated[str, "Connection name to test"],
        tests: Annotated[
            list[str] | None,
            (
                "Subset of checks to run. Common values: 'connect', 'kill', "
                "'query', 'tmp_table', 'cdt', 'pdt'. Omit to run all checks "
                "supported by the dialect."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("test_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                params = {"tests": ",".join(tests)} if tests else None
                results = await session.put(
                    f"/connections/{quote(name, safe='')}/test", params=params
                )
                summary = [
                    {
                        "check": r.get("name"),
                        "status": r.get("status"),
                        "message": r.get("message"),
                    }
                    for r in (results or [])
                ]
                all_ok = bool(summary) and all(r["status"] == "success" for r in summary)
                return json.dumps(
                    {
                        "connection": name,
                        "healthy": all_ok,
                        "checks": summary,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("test_connection", e)
