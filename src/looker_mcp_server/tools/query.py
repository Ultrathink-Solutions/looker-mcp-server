"""Query tool group — semantic layer queries and content search.

Tools for running queries through Looker's semantic model, executing
saved Looks and dashboards, and searching across all content.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


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
    ) -> str:
        ctx = client.build_context("query", "query", {"model": model, "view": view})
        try:
            async with client.session(ctx) as session:
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
    ) -> str:
        ctx = client.build_context("query_sql", "query", {"model": model, "view": view})
        try:
            async with client.session(ctx) as session:
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

                result = await session.get(f"/queries/{query_id}/run/sql")
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
    ) -> str:
        ctx = client.build_context("run_look", "query", {"look_id": look_id})
        try:
            async with client.session(ctx) as session:
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
                            data = await session.get(f"/queries/{query_id}/run/json")
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
    ) -> str:
        ctx = client.build_context("query_url", "query", {"model": model, "view": view})
        try:
            async with client.session(ctx) as session:
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
                return json.dumps({"url": share_url, "query_id": query_def.get("id")}, indent=2)
        except Exception as e:
            return format_api_error("query_url", e)

    @server.tool(
        description=(
            "Search across all Looker content — dashboards, looks, explores, "
            "and more. Returns matching items with titles, descriptions, and IDs."
        ),
    )
    async def search_content(
        query_string: Annotated[str, "Search query (full-text search)"],
        types: Annotated[
            list[str] | None,
            "Content types to search: 'dashboard', 'look', 'folder', etc.",
        ] = None,
        limit: Annotated[int, "Maximum results to return"] = 20,
    ) -> str:
        ctx = client.build_context("search_content", "query", {"query_string": query_string})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"terms": query_string, "limit": limit}
                if types:
                    params["types"] = ",".join(types)
                results = await session.get("/content_metadata_access", params=params)
                return json.dumps(results, indent=2)
        except Exception as e:
            return format_api_error("search_content", e)
