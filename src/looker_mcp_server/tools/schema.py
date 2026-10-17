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

ConnectionName = Annotated[
    str,
    "Name of the Looker database connection, for example 'warehouse'. "
    "list_connections returns connection names.",
]


def register_schema_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "List the databases that a Looker connection can reach. Use it as the first "
            "step when exploring the raw warehouse behind a connection; an empty list "
            "means the connection's dialect has a single database, so go on to "
            "list_schemas with the connection's default database. It returns database "
            "names only. To answer a business question, query the semantic model with "
            "query instead of reading raw tables."
        ),
    )
    async def list_databases(
        connection_name: ConnectionName,
    ) -> str:
        ctx = client.build_context("list_databases", "schema", {"connection_name": connection_name})
        try:
            async with client.session(ctx) as session:
                databases = await session.get(f"/connections/{connection_name}/databases")
                return json.dumps(databases, indent=2)
        except Exception as e:
            return format_api_error("list_databases", e)

    @server.tool(
        description=(
            "List the schemas in a database that a Looker connection can reach. Use it "
            "after list_databases to find where the raw tables live. It returns schema "
            "names only; use list_tables for the tables in a schema."
        ),
    )
    async def list_schemas(
        connection_name: ConnectionName,
        database: Annotated[
            str | None,
            "Database name, for example 'analytics'. Required for a connection that "
            "reaches more than one database. Default: the connection's database.",
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
        description=(
            "List the tables in a database schema that a Looker connection can reach. "
            "Use it to see which raw tables a LookML model could be built on. It returns "
            "table names, not columns or data; use list_columns for the columns."
        ),
    )
    async def list_tables(
        connection_name: ConnectionName,
        database: Annotated[
            str | None,
            "Database name, for example 'analytics'. Default: the connection's database.",
        ] = None,
        schema_name: Annotated[
            str | None,
            "Schema to list, for example 'public'. Default: every schema the connection can see.",
        ] = None,
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
            "List the columns of raw tables that a Looker connection can reach, with "
            "each column's name and data type. Use it to check the warehouse columns "
            "behind a LookML field. Name a table with table_name to keep the response "
            "short; without it the response covers every table in the schema."
        ),
    )
    async def list_columns(
        connection_name: ConnectionName,
        database: Annotated[
            str | None,
            "Database name, for example 'analytics'. Default: the connection's database.",
        ] = None,
        schema_name: Annotated[
            str | None,
            "Schema of the tables, for example 'public'. Default: not set, which leaves "
            "the choice of schema to Looker.",
        ] = None,
        table_name: Annotated[
            str | None,
            "Table to list columns for, for example 'orders'. Default: every table in the schema.",
        ] = None,
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
                    # Looker's columns endpoint filters on ``table_names``, a
                    # comma-separated list; one table is a list of one.
                    params["table_names"] = table_name
                columns = await session.get(
                    f"/connections/{connection_name}/columns", params=params
                )
                return json.dumps(columns, indent=2)
        except Exception as e:
            return format_api_error("list_columns", e)
