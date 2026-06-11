"""Query tool group — semantic layer queries and content search.

Tools for running queries through Looker's semantic model, executing
saved Looks and dashboards, and searching across all content.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, LookerSession, format_api_error
from ._helpers import (
    ActAsUser,
    IncludeHidden,
    _filter_hidden,
    _maybe_use_branch,
    _validate_branch_args,
)

# Looker returns these formats as ``text/plain`` rather than JSON; calling
# ``session.get`` on them would raise ``Expecting value`` from the JSON
# decoder. ``sql`` is the same trap that motivated #29 / ``get_text`` —
# kept here so any future format additions stay co-located.
_TEXT_PLAIN_FORMATS = frozenset({"csv", "txt", "sql"})


async def _execute_saved_query(
    session: LookerSession,
    query_id: str,
    result_format: str = "json",
    *,
    limit: int | None = None,
    apply_formatting: bool | None = None,
    apply_vis: bool | None = None,
    server_table_calcs: bool | None = None,
    cache: bool | None = None,
) -> Any:
    """Run an existing Looker ``Query`` via ``GET /queries/{id}/run/{format}``.

    Shared by ``run_query`` and ``run_dashboard``'s per-element loop so the
    two paths can't drift on path shape, param serialization, or the
    JSON-vs-text response routing.

    Booleans are serialized as lowercase ``true``/``false`` because httpx's
    default ``str(True)`` → ``"True"`` is not what Looker's query-string
    parser accepts. ``None`` values are omitted entirely so the caller sees
    Looker's documented defaults.
    """
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = str(limit)
    for key, value in (
        ("apply_formatting", apply_formatting),
        ("apply_vis", apply_vis),
        ("server_table_calcs", server_table_calcs),
        ("cache", cache),
    ):
        if value is not None:
            params[key] = "true" if value else "false"

    path = f"/queries/{query_id}/run/{result_format}"
    request_params = params or None
    if result_format in _TEXT_PLAIN_FORMATS:
        return await session.get_text(path, params=request_params)
    return await session.get(path, params=request_params)


def register_query_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "Run a query using the Looker semantic model. Specify the model, "
            "explore, fields, filters, and sorts. Looker generates the optimized "
            "SQL — you never write SQL directly. Returns data rows as JSON."
        ),
    )
    async def query(
        model: Annotated[str, "LookML model name (e.g. 'ecommerce')"],
        view: Annotated[str, "Explore/view name within the model (e.g. 'orders')"],
        fields: Annotated[
            list[str],
            "Fields to select — use fully-qualified names "
            "(e.g. ['orders.region', 'orders.total_revenue'])",
        ],
        filters: Annotated[
            dict[str, str] | None,
            "Filter expressions as field:value pairs (e.g. {'orders.created_date': '90 days'})",
        ] = None,
        sorts: Annotated[
            list[str] | None,
            "Sort expressions (e.g. ['orders.total_revenue desc'])",
        ] = None,
        limit: Annotated[int, "Maximum rows to return"] = 500,
        result_format: Annotated[
            str,
            "Output format: 'json' (default), 'json_detail', 'csv', 'txt'",
        ] = "json",
        dev_mode: Annotated[
            bool,
            "Run against the dev workspace's currently-checked-out LookML "
            "rather than production. Required when validating in-progress "
            "branch edits. Implied automatically when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call. The dev "
            "workspace's saved branch is restored when the call completes "
            "(success or failure). Implies dev_mode=True; requires project_id.",
        ] = None,
        project_id: Annotated[
            str | None,
            "LookML project ID — required when ``branch`` is set so the "
            "MCP knows which project's branch state to swap.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    body: dict[str, Any] = {
                        "model": model,
                        "view": view,
                        "fields": fields,
                        "limit": str(limit),
                    }
                    if filters:
                        body["filters"] = filters
                    if sorts:
                        body["sorts"] = sorts

                    query_def = await session.post("/queries", body=body)
                    query_id = query_def["id"]

                    result = await session.get(f"/queries/{query_id}/run/{result_format}")
                    if isinstance(result, list):
                        return json.dumps(
                            {"row_count": len(result), "data": result},
                            indent=2,
                        )
                    return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("query", e)

    @server.tool(
        description=(
            "Generate the SQL that Looker would execute for a query, without "
            "actually running it. Useful for reviewing or debugging queries."
        ),
    )
    async def query_sql(
        model: Annotated[str, "LookML model name"],
        view: Annotated[str, "Explore/view name"],
        fields: Annotated[list[str], "Fields to select"],
        filters: Annotated[dict[str, str] | None, "Filter expressions"] = None,
        sorts: Annotated[list[str] | None, "Sort expressions"] = None,
        limit: Annotated[int, "Maximum rows"] = 500,
        dev_mode: Annotated[
            bool,
            "Compile the SQL against the dev workspace's LookML rather "
            "than production. Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call (saved "
            "branch restored on exit). Requires project_id.",
        ] = None,
        project_id: Annotated[str | None, "LookML project ID — required with ``branch``"] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query_sql",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    body: dict[str, Any] = {
                        "model": model,
                        "view": view,
                        "fields": fields,
                        "limit": str(limit),
                    }
                    if filters:
                        body["filters"] = filters
                    if sorts:
                        body["sorts"] = sorts

                    query_def = await session.post("/queries", body=body)
                    query_id = query_def["id"]

                    # Looker returns the compiled SQL as text/plain; calling
                    # session.get would route through response.json() and raise
                    # ``Expecting value: line 1 column 1 (char 0)``. Mirrors the
                    # pattern used by the git deploy-key tools.
                    result = await session.get_text(f"/queries/{query_id}/run/sql")
                    return json.dumps({"sql": result}, indent=2)
        except Exception as e:
            return format_api_error("query_sql", e)

    @server.tool(
        description=(
            "Run the query associated with a saved Look and return its results. "
            "Looks are pre-built query configurations saved in Looker."
        ),
    )
    async def run_look(
        look_id: Annotated[str, "ID of the saved Look"],
        result_format: Annotated[str, "Output format: 'json', 'csv', 'txt'"] = "json",
        limit: Annotated[int, "Maximum rows to return"] = 500,
        dev_mode: Annotated[
            bool,
            "Resolve the Look's model+explore against the dev workspace's "
            "LookML rather than production. Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call (saved "
            "branch restored on exit). Requires project_id.",
        ] = None,
        project_id: Annotated[
            str | None,
            "LookML project ID owning the Look's model — required with ``branch``",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "run_look",
            "query",
            {
                "look_id": look_id,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    result = await session.get(
                        f"/looks/{look_id}/run/{result_format}",
                        params={"limit": limit},
                    )
                    if isinstance(result, list):
                        return json.dumps(
                            {"row_count": len(result), "data": result},
                            indent=2,
                        )
                    return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("run_look", e)

    @server.tool(
        description=(
            "Run an existing saved Looker ``Query`` by ID and return its "
            "results. Unlike ``query``, this does not re-spec the query "
            "body — any settings baked into the saved ``Query`` (e.g. "
            "``dynamic_fields`` / table calcs / vis config) are preserved. "
            "Useful for re-running a query whose ID you already have: a "
            "dashboard tile's ``query.id``, the id returned by "
            "``query_url``, or an id surfaced by other Looker tooling."
        ),
    )
    async def run_query(
        query_id: Annotated[str, "ID of the saved Query"],
        result_format: Annotated[
            str,
            "Output format: 'json' (default), 'json_detail', 'csv', 'txt'",
        ] = "json",
        limit: Annotated[
            int | None,
            "Row limit override. Omit to use the limit baked into the saved Query.",
        ] = None,
        apply_formatting: Annotated[
            bool,
            "Render values per LookML/Look formatting (currency symbols, "
            "date formats, etc.). Default false matches Looker's API default.",
        ] = False,
        apply_vis: Annotated[
            bool,
            "Apply visualization-config-driven rendering to the result. "
            "Default false matches Looker's API default.",
        ] = False,
        server_table_calcs: Annotated[
            bool,
            "Compute table calculations server-side so the response "
            "includes them. Required for tile-fidelity validation when "
            "the saved Query carries table calcs. Default false matches "
            "Looker's API default.",
        ] = False,
        cache: Annotated[
            bool,
            "Allow Looker to serve cached results. Set false to force a "
            "fresh run. Default true matches Looker's API default.",
        ] = True,
        dev_mode: Annotated[
            bool,
            "Resolve the Query against the dev workspace's LookML rather "
            "than production. Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call (saved "
            "branch restored on exit). Requires project_id.",
        ] = None,
        project_id: Annotated[
            str | None,
            "LookML project ID owning the Query's model — required with ``branch``",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "run_query",
            "query",
            {
                "query_id": query_id,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    result = await _execute_saved_query(
                        session,
                        query_id,
                        result_format,
                        limit=limit,
                        apply_formatting=apply_formatting,
                        apply_vis=apply_vis,
                        server_table_calcs=server_table_calcs,
                        cache=cache,
                    )
                    if isinstance(result, list):
                        return json.dumps(
                            {"row_count": len(result), "data": result},
                            indent=2,
                        )
                    if isinstance(result, str):
                        # text/plain formats (csv, txt) — wrap so the
                        # response shape is always JSON for MCP transport.
                        return json.dumps(
                            {"format": result_format, "data": result},
                            indent=2,
                        )
                    return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("run_query", e)

    @server.tool(
        description=(
            "Get a dashboard definition and run all its tile queries. "
            "Returns the dashboard metadata and the data for each element."
        ),
    )
    async def run_dashboard(
        dashboard_id: Annotated[str, "ID of the dashboard"],
    ) -> str:
        ctx = client.build_context("run_dashboard", "query", {"dashboard_id": dashboard_id})
        try:
            async with client.session(ctx) as session:
                dashboard = await session.get(f"/dashboards/{dashboard_id}")
                elements = dashboard.get("dashboard_elements") or []

                results: list[dict[str, Any]] = []
                for elem in elements:
                    elem_info: dict[str, Any] = {
                        "title": elem.get("title") or elem.get("title_text"),
                        "type": elem.get("type"),
                    }
                    query_id = (elem.get("query") or {}).get("id") if elem.get("query") else None
                    result_maker = elem.get("result_maker")
                    if result_maker:
                        query_id = query_id or (result_maker.get("query") or {}).get("id")

                    if query_id:
                        try:
                            data = await _execute_saved_query(session, query_id, "json")
                            elem_info["row_count"] = len(data) if isinstance(data, list) else 0
                            elem_info["data"] = data
                        except Exception:
                            elem_info["error"] = "Failed to execute element query"
                    results.append(elem_info)

                return json.dumps(
                    {
                        "title": dashboard.get("title"),
                        "description": dashboard.get("description"),
                        "element_count": len(results),
                        "elements": results,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("run_dashboard", e)

    @server.tool(
        description=(
            "Generate a URL to a Looker Explore with pre-populated query "
            "parameters. The URL opens the Explore UI in Looker."
        ),
    )
    async def query_url(
        model: Annotated[str, "LookML model name"],
        view: Annotated[str, "Explore/view name"],
        fields: Annotated[list[str], "Fields to select"],
        filters: Annotated[dict[str, str] | None, "Filter expressions"] = None,
        sorts: Annotated[list[str] | None, "Sort expressions"] = None,
        dev_mode: Annotated[
            bool,
            "Generate the URL against dev-workspace LookML. Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call. Requires project_id.",
        ] = None,
        project_id: Annotated[str | None, "LookML project ID — required with ``branch``"] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query_url",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    body: dict[str, Any] = {
                        "model": model,
                        "view": view,
                        "fields": fields,
                    }
                    if filters:
                        body["filters"] = filters
                    if sorts:
                        body["sorts"] = sorts

                    query_def = await session.post("/queries", body=body)
                    share_url = query_def.get("share_url") or query_def.get("url")
                    return json.dumps(
                        {"url": share_url, "query_id": query_def.get("id")},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("query_url", e)

    @server.tool(
        description=(
            "Search across all Looker content — dashboards, looks, explores, "
            "and more. Returns matching items with titles, descriptions, and IDs. "
            "Hidden items are excluded unless include_hidden=true."
        ),
    )
    async def search_content(
        query_string: Annotated[str, "Search query (full-text search)"],
        types: Annotated[
            list[str] | None,
            "Content types to search: 'dashboard', 'look', 'folder', etc.",
        ] = None,
        limit: Annotated[int, "Maximum results to return"] = 20,
        include_hidden: IncludeHidden = False,
    ) -> str:
        ctx = client.build_context("search_content", "query", {"query_string": query_string})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"terms": query_string, "limit": limit}
                if types:
                    params["types"] = ",".join(types)
                results = await session.get("/content_metadata_access", params=params)
                if isinstance(results, list):
                    results = _filter_hidden(results, include_hidden)
                return json.dumps(results, indent=2)
        except Exception as e:
            return format_api_error("search_content", e)
