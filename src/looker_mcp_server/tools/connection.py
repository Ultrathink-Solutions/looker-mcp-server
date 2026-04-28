"""Connection tool group — database connection management.

Covers full CRUD for Looker database connections plus the ``test`` endpoint
that validates connectivity and PDT permissions.  These tools require admin
permission in Looker and are disabled by default.  Enable with
``--groups connection`` (or ``all``).

The ``create_connection`` and ``update_connection`` signatures expose every
writable field of Looker's ``DBConnection`` schema, so connections can be
configured end-to-end from an MCP client without falling back to the Looker
admin UI — including key-pair auth (Snowflake), OAuth / Application Default
Credentials (BigQuery), SSH tunnels, Oracle TNS, per-user attribute mapping,
PDT overrides, and BigQuery storage project routing.

Read-only fields returned by Looker (``has_password``, ``snippets``,
``pdts_enabled``, ``uses_oauth``, ``managed``, ``last_regen_at``, …) are
surfaced verbatim by ``get_connection`` and are not accepted as inputs:
sending them in a request body is a no-op on Looker's side.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg, _set_if


def register_connection_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Read ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get full configuration for a specific database connection, including "
            "dialect, host, database, PDT settings, usage attributes, and all "
            "read-only metadata (``has_password``, ``snippets``, ``pdts_enabled``, "
            "``uses_oauth``, ``managed``, ``last_regen_at``, ``last_reap_at``, "
            "``created_at``, ``user_id``, ``p4sa_name``, ``default_bq_connection``, "
            "etc.). Write-only fields like ``password`` and ``certificate`` are "
            "never returned by Looker. For a trimmed summary of all connections, "
            "use ``list_connections`` in the explore group instead."
        ),
    )
    async def get_connection(
        name: Annotated[str, "Connection name"],
    ) -> str:
        ctx = client.build_context("get_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                conn = await session.get(f"/connections/{_path_seg(name)}")
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
            "Call ``list_connection_dialects`` first if you need valid dialect names. "
            "All optional fields default to Looker's dialect-level defaults when "
            "omitted; pass only the fields you intend to set."
        ),
    )
    async def create_connection(
        # ── Identity ────────────────────────────────────────────────
        name: Annotated[str, "Unique connection name (used by LookML ``connection:`` param)"],
        dialect_name: Annotated[str, "Database dialect, e.g. 'snowflake', 'bigquery_standard_sql'"],
        # ── Connection target ───────────────────────────────────────
        host: Annotated[str | None, "Database host or endpoint"] = None,
        port: Annotated[int | None, "Database port"] = None,
        database: Annotated[str | None, "Default database name"] = None,
        schema: Annotated[str | None, "Default schema"] = None,
        service_name: Annotated[
            str | None,
            "Oracle TNS service name (only meaningful when ``uses_tns`` is true)",
        ] = None,
        uses_tns: Annotated[
            bool | None,
            "Enable Oracle Transparent Network Substrate (TNS) connections",
        ] = None,
        named_driver_version_requested: Annotated[
            str | None,
            "Pin a specific JDBC driver version name (omit to use the dialect default)",
        ] = None,
        # ── Authentication: username / password ─────────────────────
        username: Annotated[str | None, "Database username"] = None,
        password: Annotated[str | None, "Database password (write-only)"] = None,
        # ── Authentication: key-pair (Snowflake, BigQuery service-account keys) ──
        uses_key_pair_auth: Annotated[
            bool | None,
            "Enable key-pair authentication (Snowflake et al.)",
        ] = None,
        certificate: Annotated[
            str | None,
            "Base64-encoded keyfile/certificate body (write-only)",
        ] = None,
        file_type: Annotated[
            str | None,
            "Keyfile type for ``certificate``: '.json', '.p8', or '.p12'",
        ] = None,
        # ── Authentication: OAuth / Application Default Credentials ─
        oauth_application_id: Annotated[
            str | None,
            "External OAuth Application id used for authenticating to the database",
        ] = None,
        uses_application_default_credentials: Annotated[
            bool | None,
            "Authenticate using the host environment's Application Default Credentials (GCP)",
        ] = None,
        impersonated_service_account: Annotated[
            str | None,
            "Service account email to impersonate when querying datasets (used with ADC)",
        ] = None,
        # ── Per-user / user-attribute scoping ───────────────────────
        user_db_credentials: Annotated[
            bool | None,
            (
                "Enable per-user database credentials. Enabling clears any previously "
                "set ``username`` / ``password`` on the connection. Limited-access feature."
            ),
        ] = None,
        user_attribute_fields: Annotated[
            list[str] | None,
            (
                "Connection fields whose values are sourced from Looker user attributes "
                "(e.g. ['username', 'database', 'schema']). Acts as the per-user "
                "allowlist for runtime substitution."
            ),
        ] = None,
        # ── Connection pool / SSL ───────────────────────────────────
        ssl: Annotated[bool | None, "Use SSL/TLS for the connection"] = None,
        verify_ssl: Annotated[bool | None, "Verify the server's SSL certificate"] = None,
        max_connections: Annotated[int | None, "Maximum pool size"] = None,
        max_queries: Annotated[
            int | None,
            "Maximum number of concurrent queries to begin on this connection",
        ] = None,
        max_queries_per_user: Annotated[
            int | None,
            "Maximum number of concurrent queries per user on this connection",
        ] = None,
        pool_timeout: Annotated[int | None, "Pool checkout timeout in seconds"] = None,
        connection_pooling: Annotated[bool | None, "Enable database connection pooling"] = None,
        # ── SQL governance ──────────────────────────────────────────
        max_billing_gigabytes: Annotated[
            str | None,
            (
                "Maximum query size in GB (BigQuery only). May be a literal number "
                "or the name of a Looker user attribute."
            ),
        ] = None,
        cost_estimate_enabled: Annotated[
            bool | None,
            "Show query cost estimate in Explore",
        ] = None,
        query_holding_disabled: Annotated[
            bool | None,
            "Disable query holding for this connection",
        ] = None,
        disable_context_comment: Annotated[
            bool | None,
            "Suppress Looker's context comment on emitted SQL",
        ] = None,
        query_timezone: Annotated[str | None, "Timezone applied at query time"] = None,
        db_timezone: Annotated[str | None, "Timezone of the database"] = None,
        after_connect_statements: Annotated[
            str | None,
            (
                "SQL statements (semicolon-separated) to run after connecting. "
                "Requires the ``custom_after_connect_statements`` Looker permission."
            ),
        ] = None,
        jdbc_additional_params: Annotated[
            str | None,
            "Extra JDBC parameters, semicolon-separated",
        ] = None,
        sql_runner_precache_tables: Annotated[
            bool | None,
            "Pre-cache tables in the SQL Runner",
        ] = None,
        sql_writing_with_info_schema: Annotated[
            bool | None,
            "Use information_schema when authoring SQL",
        ] = None,
        # ── PDTs (persistent derived tables) ────────────────────────
        tmp_db_name: Annotated[str | None, "Scratch schema/database for PDTs"] = None,
        tmp_db_host: Annotated[
            str | None,
            "Scratch host for PDTs (when separate from the primary host)",
        ] = None,
        maintenance_cron: Annotated[
            str | None,
            "Cron expression scheduling PDT trigger checks and drops",
        ] = None,
        pdt_concurrency: Annotated[
            int | None,
            "Maximum number of threads used to build PDTs in parallel",
        ] = None,
        pdt_api_control_enabled: Annotated[
            bool | None,
            "Allow PDT builds to be kicked off and cancelled via API",
        ] = None,
        always_retry_failed_builds: Annotated[
            bool | None,
            "Retry errored PDTs every regenerator cycle",
        ] = None,
        pdt_context_override: Annotated[
            dict[str, Any] | None,
            (
                "Per-PDT-context override block (DBConnectionOverride). Accepts the "
                "keys: ``context`` (must be 'pdt'), plus any of ``host``, ``port``, "
                "``username``, ``password``, ``certificate``, ``file_type``, "
                "``database``, ``schema``, ``jdbc_additional_params``, "
                "``after_connect_statements``, ``service_name`` (each may also be "
                "given with a ``pdt_`` prefix). Lets PDT builds run against a "
                "separate write-capable role/host without affecting query traffic."
            ),
        ] = None,
        # ── SSH tunnel ──────────────────────────────────────────────
        tunnel_id: Annotated[
            str | None,
            "Id of an existing Looker SSH tunnel definition to route the connection through",
        ] = None,
        custom_local_port: Annotated[
            int | None,
            "Local port mapped by the SSH tunnel (only meaningful when ``tunnel_id`` is set)",
        ] = None,
        # ── BigQuery ────────────────────────────────────────────────
        bq_storage_project_id: Annotated[
            str | None,
            "Default BigQuery storage project id (BigQuery dialects only)",
        ] = None,
        bq_roles_verified: Annotated[
            bool | None,
            "Mark all BigQuery project roles as verified",
        ] = None,
    ) -> str:
        ctx = client.build_context("create_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "dialect_name": dialect_name}
                # Connection target
                _set_if(body, "host", host)
                _set_if(body, "port", port)
                _set_if(body, "database", database)
                _set_if(body, "schema", schema)
                _set_if(body, "service_name", service_name)
                _set_if(body, "uses_tns", uses_tns)
                _set_if(body, "named_driver_version_requested", named_driver_version_requested)
                # Username / password
                _set_if(body, "username", username)
                _set_if(body, "password", password)
                # Key-pair
                _set_if(body, "uses_key_pair_auth", uses_key_pair_auth)
                _set_if(body, "certificate", certificate)
                _set_if(body, "file_type", file_type)
                # OAuth / ADC
                _set_if(body, "oauth_application_id", oauth_application_id)
                _set_if(
                    body,
                    "uses_application_default_credentials",
                    uses_application_default_credentials,
                )
                _set_if(body, "impersonated_service_account", impersonated_service_account)
                # Per-user / user-attribute scoping
                _set_if(body, "user_db_credentials", user_db_credentials)
                _set_if(body, "user_attribute_fields", user_attribute_fields)
                # Pool / SSL
                _set_if(body, "ssl", ssl)
                _set_if(body, "verify_ssl", verify_ssl)
                _set_if(body, "max_connections", max_connections)
                _set_if(body, "max_queries", max_queries)
                _set_if(body, "max_queries_per_user", max_queries_per_user)
                _set_if(body, "pool_timeout", pool_timeout)
                _set_if(body, "connection_pooling", connection_pooling)
                # SQL governance
                _set_if(body, "max_billing_gigabytes", max_billing_gigabytes)
                _set_if(body, "cost_estimate_enabled", cost_estimate_enabled)
                _set_if(body, "query_holding_disabled", query_holding_disabled)
                _set_if(body, "disable_context_comment", disable_context_comment)
                _set_if(body, "query_timezone", query_timezone)
                _set_if(body, "db_timezone", db_timezone)
                _set_if(body, "after_connect_statements", after_connect_statements)
                _set_if(body, "jdbc_additional_params", jdbc_additional_params)
                _set_if(body, "sql_runner_precache_tables", sql_runner_precache_tables)
                _set_if(body, "sql_writing_with_info_schema", sql_writing_with_info_schema)
                # PDTs
                _set_if(body, "tmp_db_name", tmp_db_name)
                _set_if(body, "tmp_db_host", tmp_db_host)
                _set_if(body, "maintenance_cron", maintenance_cron)
                _set_if(body, "pdt_concurrency", pdt_concurrency)
                _set_if(body, "pdt_api_control_enabled", pdt_api_control_enabled)
                _set_if(body, "always_retry_failed_builds", always_retry_failed_builds)
                _set_if(body, "pdt_context_override", pdt_context_override)
                # SSH tunnel
                _set_if(body, "tunnel_id", tunnel_id)
                _set_if(body, "custom_local_port", custom_local_port)
                # BigQuery
                _set_if(body, "bq_storage_project_id", bq_storage_project_id)
                _set_if(body, "bq_roles_verified", bq_roles_verified)

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
            "Update an existing database connection. Only provided fields are "
            "changed — omitted fields are preserved. After updating credentials, "
            "host, or auth mode, call ``test_connection`` to verify the change. "
            "``name`` and ``dialect_name`` are write-once at create time and are "
            "intentionally not accepted here. ``pdts_enabled`` and ``uses_oauth`` "
            "are read-only on the API and are derived from other fields "
            "(``tmp_db_name`` + permissions, and ``oauth_application_id`` "
            "respectively)."
        ),
    )
    async def update_connection(
        name: Annotated[str, "Connection name to update"],
        # ── Connection target ───────────────────────────────────────
        host: Annotated[str | None, "New host"] = None,
        port: Annotated[int | None, "New port"] = None,
        database: Annotated[str | None, "New database name"] = None,
        schema: Annotated[str | None, "New default schema"] = None,
        service_name: Annotated[
            str | None,
            "New Oracle TNS service name (only meaningful when ``uses_tns`` is true)",
        ] = None,
        uses_tns: Annotated[bool | None, "Toggle Oracle TNS connection mode"] = None,
        named_driver_version_requested: Annotated[
            str | None,
            "Change the requested JDBC driver version name",
        ] = None,
        # ── Authentication: username / password ─────────────────────
        username: Annotated[str | None, "New username"] = None,
        password: Annotated[str | None, "New password (write-only)"] = None,
        # ── Authentication: key-pair ────────────────────────────────
        uses_key_pair_auth: Annotated[
            bool | None,
            "Toggle key-pair authentication",
        ] = None,
        certificate: Annotated[
            str | None,
            "New base64-encoded keyfile/certificate body (write-only)",
        ] = None,
        file_type: Annotated[
            str | None,
            "Keyfile type for ``certificate``: '.json', '.p8', or '.p12'",
        ] = None,
        # ── Authentication: OAuth / ADC ─────────────────────────────
        oauth_application_id: Annotated[
            str | None,
            "External OAuth Application id (set/clear). Drives the read-only ``uses_oauth`` flag.",
        ] = None,
        uses_application_default_credentials: Annotated[
            bool | None,
            "Toggle Application Default Credentials authentication (GCP)",
        ] = None,
        impersonated_service_account: Annotated[
            str | None,
            "Service account email to impersonate (used with ADC)",
        ] = None,
        # ── Per-user / user-attribute scoping ───────────────────────
        user_db_credentials: Annotated[
            bool | None,
            (
                "Toggle per-user database credentials. Enabling clears any "
                "previously set ``username`` / ``password``."
            ),
        ] = None,
        user_attribute_fields: Annotated[
            list[str] | None,
            "Connection fields sourced from Looker user attributes (replaces the existing list)",
        ] = None,
        # ── Connection pool / SSL ───────────────────────────────────
        ssl: Annotated[bool | None, "Toggle SSL/TLS"] = None,
        verify_ssl: Annotated[bool | None, "Toggle SSL certificate verification"] = None,
        max_connections: Annotated[int | None, "New maximum pool size"] = None,
        max_queries: Annotated[int | None, "New max concurrent queries on this connection"] = None,
        max_queries_per_user: Annotated[
            int | None,
            "New max concurrent queries per user",
        ] = None,
        pool_timeout: Annotated[int | None, "New pool checkout timeout"] = None,
        connection_pooling: Annotated[bool | None, "Toggle database connection pooling"] = None,
        # ── SQL governance ──────────────────────────────────────────
        max_billing_gigabytes: Annotated[
            str | None,
            "New BigQuery query-size cap (literal GB or user-attribute name)",
        ] = None,
        cost_estimate_enabled: Annotated[
            bool | None,
            "Toggle Explore cost estimates",
        ] = None,
        query_holding_disabled: Annotated[bool | None, "Toggle query holding"] = None,
        disable_context_comment: Annotated[bool | None, "Toggle context comments on SQL"] = None,
        query_timezone: Annotated[str | None, "New query timezone"] = None,
        db_timezone: Annotated[str | None, "New database timezone"] = None,
        after_connect_statements: Annotated[
            str | None,
            (
                "SQL statements (semicolon-separated) to run after connecting. "
                "Requires the ``custom_after_connect_statements`` Looker permission."
            ),
        ] = None,
        jdbc_additional_params: Annotated[str | None, "New extra JDBC parameters"] = None,
        sql_runner_precache_tables: Annotated[
            bool | None,
            "Toggle SQL Runner table precaching",
        ] = None,
        sql_writing_with_info_schema: Annotated[
            bool | None,
            "Toggle use of information_schema when authoring SQL",
        ] = None,
        # ── PDTs ────────────────────────────────────────────────────
        tmp_db_name: Annotated[str | None, "New scratch schema/database for PDTs"] = None,
        tmp_db_host: Annotated[str | None, "New scratch host for PDTs"] = None,
        maintenance_cron: Annotated[
            str | None,
            "New cron expression for PDT maintenance",
        ] = None,
        pdt_concurrency: Annotated[int | None, "New maximum PDT-build thread count"] = None,
        pdt_api_control_enabled: Annotated[bool | None, "Toggle API control of PDT builds"] = None,
        always_retry_failed_builds: Annotated[
            bool | None,
            "Toggle automatic retry of errored PDTs",
        ] = None,
        pdt_context_override: Annotated[
            dict[str, Any] | None,
            (
                "Replace the per-PDT-context override block (DBConnectionOverride). "
                "See ``create_connection`` for the accepted shape."
            ),
        ] = None,
        # ── SSH tunnel ──────────────────────────────────────────────
        tunnel_id: Annotated[str | None, "New SSH tunnel id (or empty string to clear)"] = None,
        custom_local_port: Annotated[int | None, "New SSH-tunnel local port"] = None,
        # ── BigQuery ────────────────────────────────────────────────
        bq_storage_project_id: Annotated[
            str | None,
            "New default BigQuery storage project id",
        ] = None,
        bq_roles_verified: Annotated[
            bool | None,
            "Mark all BigQuery project roles as verified",
        ] = None,
    ) -> str:
        ctx = client.build_context("update_connection", "connection", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                # Connection target
                _set_if(body, "host", host)
                _set_if(body, "port", port)
                _set_if(body, "database", database)
                _set_if(body, "schema", schema)
                _set_if(body, "service_name", service_name)
                _set_if(body, "uses_tns", uses_tns)
                _set_if(body, "named_driver_version_requested", named_driver_version_requested)
                # Username / password
                _set_if(body, "username", username)
                _set_if(body, "password", password)
                # Key-pair
                _set_if(body, "uses_key_pair_auth", uses_key_pair_auth)
                _set_if(body, "certificate", certificate)
                _set_if(body, "file_type", file_type)
                # OAuth / ADC
                _set_if(body, "oauth_application_id", oauth_application_id)
                _set_if(
                    body,
                    "uses_application_default_credentials",
                    uses_application_default_credentials,
                )
                _set_if(body, "impersonated_service_account", impersonated_service_account)
                # Per-user / user-attribute scoping
                _set_if(body, "user_db_credentials", user_db_credentials)
                _set_if(body, "user_attribute_fields", user_attribute_fields)
                # Pool / SSL
                _set_if(body, "ssl", ssl)
                _set_if(body, "verify_ssl", verify_ssl)
                _set_if(body, "max_connections", max_connections)
                _set_if(body, "max_queries", max_queries)
                _set_if(body, "max_queries_per_user", max_queries_per_user)
                _set_if(body, "pool_timeout", pool_timeout)
                _set_if(body, "connection_pooling", connection_pooling)
                # SQL governance
                _set_if(body, "max_billing_gigabytes", max_billing_gigabytes)
                _set_if(body, "cost_estimate_enabled", cost_estimate_enabled)
                _set_if(body, "query_holding_disabled", query_holding_disabled)
                _set_if(body, "disable_context_comment", disable_context_comment)
                _set_if(body, "query_timezone", query_timezone)
                _set_if(body, "db_timezone", db_timezone)
                _set_if(body, "after_connect_statements", after_connect_statements)
                _set_if(body, "jdbc_additional_params", jdbc_additional_params)
                _set_if(body, "sql_runner_precache_tables", sql_runner_precache_tables)
                _set_if(body, "sql_writing_with_info_schema", sql_writing_with_info_schema)
                # PDTs
                _set_if(body, "tmp_db_name", tmp_db_name)
                _set_if(body, "tmp_db_host", tmp_db_host)
                _set_if(body, "maintenance_cron", maintenance_cron)
                _set_if(body, "pdt_concurrency", pdt_concurrency)
                _set_if(body, "pdt_api_control_enabled", pdt_api_control_enabled)
                _set_if(body, "always_retry_failed_builds", always_retry_failed_builds)
                _set_if(body, "pdt_context_override", pdt_context_override)
                # SSH tunnel
                _set_if(body, "tunnel_id", tunnel_id)
                _set_if(body, "custom_local_port", custom_local_port)
                # BigQuery
                _set_if(body, "bq_storage_project_id", bq_storage_project_id)
                _set_if(body, "bq_roles_verified", bq_roles_verified)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one settable DBConnection field. "
                                "See the tool description for the full list."
                            ),
                        },
                        indent=2,
                    )

                conn = await session.patch(f"/connections/{_path_seg(name)}", body=body)
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
                await session.delete(f"/connections/{_path_seg(name)}")
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
                results = await session.put(f"/connections/{_path_seg(name)}/test", params=params)
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
