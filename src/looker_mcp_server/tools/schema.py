"""Schema tool group — database introspection via Looker connections.

Tools for browsing the underlying database schema through Looker's
connection abstraction.  Useful for understanding the raw tables and
columns that LookML models are built on.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_schema_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description="List all databases accessible through a Looker connection.",
    )
    async def list_databases(
        connection_name: Annotated[str, "Name of the Looker database connection"],
    ) -> str:
        ctx = client.build_context("list_databases", "schema", {"connection_name": connection_name})
        try:
            async with client.session(ctx) as session:
                databases = await session.get(f"/connections/{connection_name}/databases")
                return json.dumps(databases, indent=2)
        except Exception as e:
            return format_api_error("list_databases", e)

    @server.tool(
        description="List schemas in a database accessible through a Looker connection.",
    )
    async def list_schemas(
        connection_name: Annotated[str, "Name of the Looker database connection"],
        database: Annotated[
            str | None, "Database name (required for multi-database connections)"
        ] = None,
    ) -> str:
        ctx = client.build_context("list_schemas", "schema", {"connection_name": connection_name})
        try:
            async with client.session(ctx) as session:
                params = {}
                if database:
                    params["database"] = database
                schemas = await session.get(
                    f"/connections/{connection_name}/schemas", params=params
                )
                return json.dumps(schemas, indent=2)
        except Exception as e:
            return format_api_error("list_schemas", e)

    @server.tool(
        description="List tables in a schema accessible through a Looker connection.",
    )
    async def list_tables(
        connection_name: Annotated[str, "Name of the Looker database connection"],
        database: Annotated[str | None, "Database name"] = None,
        schema_name: Annotated[str | None, "Schema name to filter tables"] = None,
    ) -> str:
        ctx = client.build_context("list_tables", "schema", {"connection_name": connection_name})
        try:
            async with client.session(ctx) as session:
                params = {}
                if database:
                    params["database"] = database
                if schema_name:
                    params["schema_name"] = schema_name
                tables = await session.get(f"/connections/{connection_name}/tables", params=params)
                return json.dumps(tables, indent=2)
        except Exception as e:
            return format_api_error("list_tables", e)

    @server.tool(
        description=(
            "List columns for specific tables in a Looker connection. "
            "Returns column names, data types, and other metadata."
        ),
    )
    async def list_columns(
        connection_name: Annotated[str, "Name of the Looker database connection"],
        database: Annotated[str | None, "Database name"] = None,
        schema_name: Annotated[str | None, "Schema name"] = None,
        table_name: Annotated[str | None, "Table name to get columns for"] = None,
    ) -> str:
        ctx = client.build_context("list_columns", "schema", {"connection_name": connection_name})
        try:
            async with client.session(ctx) as session:
                params = {}
                if database:
                    params["database"] = database
                if schema_name:
                    params["schema_name"] = schema_name
                if table_name:
                    params["table_name"] = table_name
                columns = await session.get(
                    f"/connections/{connection_name}/columns", params=params
                )
                return json.dumps(columns, indent=2)
        except Exception as e:
            return format_api_error("list_columns", e)
