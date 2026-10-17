"""Content tool group — Look and dashboard CRUD.

Tools for creating, reading, updating, and deleting Looks and dashboards,
adding dashboard elements and filters, and generating embed URLs.
"""

from __future__ import annotations

import json
import re
from typing import Annotated, Any, Literal

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._query_spec import (
    ColumnLimit,
    DynamicFields,
    FillFields,
    FilterExpression,
    Pivots,
    QueryExplore,
    QueryFields,
    QueryFilters,
    QueryLimit,
    QueryModel,
    QuerySorts,
    QueryTimezone,
    RowTotal,
    Subtotals,
    Total,
    VisConfig,
    build_query_body,
    create_query,
)

FolderFilter = Annotated[
    str | None,
    "Return only items in this folder, given as a folder ID, for example '7'. "
    "Default: every folder.",
]
SearchLimit = Annotated[int, "Maximum results to return, for example 20. Default 50."]
TitlePattern = Annotated[
    str | None,
    "Title to match, case-insensitive, with % as a wildcard: '%Revenue%' matches any "
    "title that contains Revenue. Default: any title.",
]


def _filter_name_from_title(title: str) -> str:
    """Derive a stable dashboard-filter ``name`` from its title.

    Lowercase, every run of non-alphanumeric characters becomes ``_``, and
    leading or trailing underscores are dropped: ``"Order Date (UTC)"``
    becomes ``"order_date_utc"``. A title with no alphanumerics falls back
    to ``"filter"``.
    """
    return re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_") or "filter"


def register_content_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Looks ────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Search saved Looks by title or folder and return each one's ID, title, "
            "description, folder, model and view count. Use it to find a Look to run "
            "with run_look. It does not return the Look's data and does not search "
            "dashboards; use list_dashboards for those."
        ),
    )
    async def list_looks(
        title: TitlePattern = None,
        folder_id: FolderFilter = None,
        limit: SearchLimit = 50,
    ) -> str:
        ctx = client.build_context("list_looks", "content")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if title:
                    params["title"] = title
                if folder_id:
                    params["folder_id"] = folder_id
                looks = await session.get("/looks/search", params=params)
                result = [
                    {
                        "id": lk.get("id"),
                        "title": lk.get("title"),
                        "description": lk.get("description"),
                        "folder_id": lk.get("folder_id"),
                        "model": (lk.get("query") or {}).get("model"),
                        "view_count": lk.get("view_count"),
                    }
                    for lk in (looks or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_looks", e)

    @server.tool(
        description=(
            "Save a query as a new Look in a folder and return the Look's ID, title, "
            "URL and query ID. Use it when a person asks to keep a query as a saved "
            "Look; it supports pivots, totals, visualization settings and a timezone as "
            "well as flat tables. To get data without saving anything, use query."
        ),
    )
    async def create_look(
        title: Annotated[str, "Title for the new Look, for example 'Weekly revenue by region'."],
        model: QueryModel,
        view: QueryExplore,
        fields: QueryFields,
        folder_id: Annotated[str, "ID of the folder to save the Look in, for example '7'."],
        filters: QueryFilters = None,
        sorts: QuerySorts = None,
        limit: QueryLimit = 500,
        description: Annotated[
            str | None, "Description shown with the Look. Default: none."
        ] = None,
        pivots: Pivots = None,
        fill_fields: FillFields = None,
        filter_expression: FilterExpression = None,
        column_limit: ColumnLimit = None,
        total: Total = None,
        row_total: RowTotal = None,
        subtotals: Subtotals = None,
        vis_config: VisConfig = None,
        query_timezone: QueryTimezone = None,
        dynamic_fields: DynamicFields = None,
    ) -> str:
        ctx = client.build_context("create_look", "content")
        try:
            async with client.session(ctx) as session:
                # Looker's POST /looks takes a query_id, not an inline query —
                # an inline query 422s with {"field": "query_id", "code":
                # "missing"}. Materialize first, then save.
                query_def = await create_query(
                    session,
                    build_query_body(
                        model=model,
                        view=view,
                        fields=fields,
                        filters=filters,
                        sorts=sorts,
                        limit=limit,
                        pivots=pivots,
                        fill_fields=fill_fields,
                        filter_expression=filter_expression,
                        column_limit=column_limit,
                        total=total,
                        row_total=row_total,
                        subtotals=subtotals,
                        vis_config=vis_config,
                        query_timezone=query_timezone,
                        dynamic_fields=dynamic_fields,
                    ),
                )

                body: dict[str, Any] = {
                    "title": title,
                    "folder_id": folder_id,
                    "query_id": query_def["id"],
                }
                if description:
                    body["description"] = description

                look = await session.post("/looks", body=body)
                return json.dumps(
                    {
                        "id": look.get("id"),
                        "title": look.get("title"),
                        "url": look.get("url"),
                        "query_id": query_def["id"],
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_look", e)

    @server.tool(
        description=(
            "Change a saved Look's title, description or folder, and return its ID and "
            "title. Use it to rename, describe or move a Look; arguments you leave out "
            "stay as they are. It cannot change the Look's query; save a new Look with "
            "create_look for that."
        ),
    )
    async def update_look(
        look_id: Annotated[str, "ID of the Look to update, for example '42'."],
        title: Annotated[str | None, "New title. Default: unchanged."] = None,
        description: Annotated[str | None, "New description. Default: unchanged."] = None,
        folder_id: Annotated[
            str | None, "ID of the folder to move the Look to, for example '7'. Default: unchanged."
        ] = None,
    ) -> str:
        ctx = client.build_context("update_look", "content", {"look_id": look_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if title is not None:
                    body["title"] = title
                if description is not None:
                    body["description"] = description
                if folder_id is not None:
                    body["folder_id"] = folder_id
                look = await session.patch(f"/looks/{look_id}", body=body)
                return json.dumps({"id": look.get("id"), "title": look.get("title")}, indent=2)
        except Exception as e:
            return format_api_error("update_look", e)

    @server.tool(
        description=(
            "Delete a saved Look permanently. Use it only when a person has asked for "
            "that Look to be removed, since the deletion cannot be undone. To rename or "
            "move a Look instead, use update_look."
        ),
    )
    async def delete_look(
        look_id: Annotated[str, "ID of the Look to delete, for example '42'."],
    ) -> str:
        ctx = client.build_context("delete_look", "content", {"look_id": look_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/looks/{look_id}")
                return json.dumps({"deleted": True, "look_id": look_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_look", e)

    # ── Dashboards ───────────────────────────────────────────────────

    @server.tool(
        description=(
            "Search dashboards built in the Looker UI by title or folder and return each "
            "one's ID, title, description, folder and view count. Use it to find a "
            "dashboard to run with run_dashboard. It does not return LookML dashboards, "
            "which are defined in a LookML project; use list_lookml_dashboards for those."
        ),
    )
    async def list_dashboards(
        title: TitlePattern = None,
        folder_id: FolderFilter = None,
        limit: SearchLimit = 50,
    ) -> str:
        ctx = client.build_context("list_dashboards", "content")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if title:
                    params["title"] = title
                if folder_id:
                    params["folder_id"] = folder_id
                dashboards = await session.get("/dashboards/search", params=params)
                result = [
                    {
                        "id": d.get("id"),
                        "title": d.get("title"),
                        "description": d.get("description"),
                        "folder_id": d.get("folder_id"),
                        "view_count": d.get("view_count"),
                    }
                    for d in (dashboards or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_dashboards", e)

    @server.tool(
        description=(
            "Search LookML dashboards, the ones defined in a LookML project rather than "
            "built in the UI, and return each one's ID, title, description and folder. "
            "Use it when list_dashboards does not find a dashboard, since it never "
            "returns these; their IDs look like 'model::slug', for example "
            "'analytics::executive_overview'. Only dashboards deployed to production "
            "appear, not ones on an unmerged branch."
        ),
    )
    async def list_lookml_dashboards(
        title: Annotated[
            str | None,
            "Dashboard name to match, case-insensitive, with % as a wildcard, for example "
            "'%revenue%'. This matches the dashboard_name in the LookML code, not the "
            "title shown to viewers. Default: any dashboard.",
        ] = None,
        folder_id: FolderFilter = None,
        limit: SearchLimit = 50,
    ) -> str:
        ctx = client.build_context("list_lookml_dashboards", "content")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if title:
                    params["title"] = title
                if folder_id:
                    params["folder_id"] = folder_id
                dashboards = await session.get("/dashboards/lookml/search", params=params)
                # The generated SDK types this response as a singular
                # DashboardLookml, but the endpoint returns a collection.
                # Tolerate both rather than trusting the annotation.
                if isinstance(dashboards, dict):
                    dashboards = [dashboards]
                result = [
                    {
                        "id": d.get("id"),
                        "title": d.get("title"),
                        "description": d.get("description"),
                        "folder_id": d.get("folder_id"),
                    }
                    for d in (dashboards or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_lookml_dashboards", e)

    @server.tool(
        description=(
            "Create an empty dashboard in a folder and return its ID, title and URL. "
            "Use it as the first step of building a dashboard, then add tiles with "
            "add_dashboard_element and filters with add_dashboard_filter. It does not "
            "copy an existing dashboard or add any content."
        ),
    )
    async def create_dashboard(
        title: Annotated[str, "Title for the new dashboard, for example 'Sales overview'."],
        folder_id: Annotated[str, "ID of the folder to create it in, for example '7'."],
        description: Annotated[
            str | None, "Description shown with the dashboard. Default: none."
        ] = None,
    ) -> str:
        ctx = client.build_context("create_dashboard", "content")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"title": title, "folder_id": folder_id}
                if description:
                    body["description"] = description
                dashboard = await session.post("/dashboards", body=body)
                return json.dumps(
                    {
                        "id": dashboard.get("id"),
                        "title": dashboard.get("title"),
                        "url": dashboard.get("url"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_dashboard", e)

    @server.tool(
        description=(
            "Change a dashboard's title, description or folder, and return its ID and "
            "title. Use it to rename, describe or move a dashboard; arguments you leave "
            "out stay as they are. It does not change tiles or filters; use "
            "add_dashboard_element and add_dashboard_filter for those."
        ),
    )
    async def update_dashboard(
        dashboard_id: Annotated[str, "ID of the dashboard to update, for example '17'."],
        title: Annotated[str | None, "New title. Default: unchanged."] = None,
        description: Annotated[str | None, "New description. Default: unchanged."] = None,
        folder_id: Annotated[
            str | None,
            "ID of the folder to move the dashboard to, for example '7'. Default: unchanged.",
        ] = None,
    ) -> str:
        ctx = client.build_context("update_dashboard", "content", {"dashboard_id": dashboard_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if title is not None:
                    body["title"] = title
                if description is not None:
                    body["description"] = description
                if folder_id is not None:
                    body["folder_id"] = folder_id
                dash = await session.patch(f"/dashboards/{dashboard_id}", body=body)
                return json.dumps({"id": dash.get("id"), "title": dash.get("title")}, indent=2)
        except Exception as e:
            return format_api_error("update_dashboard", e)

    @server.tool(
        description=(
            "Delete a dashboard permanently, with its tiles and filters. Use it only "
            "when a person has asked for that dashboard to be removed, since the "
            "deletion cannot be undone. To rename or move a dashboard instead, use "
            "update_dashboard."
        ),
    )
    async def delete_dashboard(
        dashboard_id: Annotated[str, "ID of the dashboard to delete, for example '17'."],
    ) -> str:
        ctx = client.build_context("delete_dashboard", "content", {"dashboard_id": dashboard_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/dashboards/{dashboard_id}")
                return json.dumps({"deleted": True, "dashboard_id": dashboard_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_dashboard", e)

    # ── Dashboard elements and filters ───────────────────────────────

    @server.tool(
        description=(
            "Add a tile to a dashboard, showing either a saved Look or a new query, and "
            "return the tile's ID and title. Use it after create_dashboard to fill a "
            "dashboard. Give look_id for a saved Look, or query_model, query_view and "
            "query_fields together for a new query; with neither, the tile has no query."
        ),
    )
    async def add_dashboard_element(
        dashboard_id: Annotated[str, "ID of the dashboard, for example '17'."],
        title: Annotated[str, "Title for the tile, for example 'Revenue by region'."],
        type: Annotated[
            str, "Tile type, for example 'vis' for a chart or table, or 'text'. Default 'vis'."
        ] = "vis",
        look_id: Annotated[
            str | None, "ID of a saved Look to show, for example '42'. Default: none."
        ] = None,
        query_model: Annotated[
            str | None, "LookML model for a new query, for example 'ecommerce'. Default: none."
        ] = None,
        query_view: Annotated[
            str | None, "Explore name for a new query, for example 'orders'. Default: none."
        ] = None,
        query_fields: Annotated[
            list[str] | None,
            "Fully qualified fields for a new query, for example "
            "['orders.region', 'orders.count']. Default: none.",
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "add_dashboard_element", "content", {"dashboard_id": dashboard_id}
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "dashboard_id": dashboard_id,
                    "title": title,
                    "type": type,
                }
                if look_id:
                    body["look_id"] = look_id
                elif query_model and query_view and query_fields:
                    body["query"] = {
                        "model": query_model,
                        "view": query_view,
                        "fields": query_fields,
                    }

                # Looker creates dashboard elements at ``POST /dashboard_elements``
                # with the dashboard named in the body; the per-dashboard path
                # only lists them.
                element = await session.post("/dashboard_elements", body=body)
                return json.dumps(
                    {"id": element.get("id"), "title": element.get("title")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("add_dashboard_element", e)

    @server.tool(
        description=(
            "Add a filter control to a dashboard and return the filter's ID, name and title. "
            "Use it to let viewers narrow a dashboard: a 'field' filter, the default, "
            "filters on one dimension and needs the dimension, model and explore, "
            "while 'date', 'number' and 'string' filters need none of them. It does "
            "not wire the filter to the dashboard's existing tiles."
        ),
    )
    async def add_dashboard_filter(
        dashboard_id: Annotated[str, "ID of the dashboard, for example '17'."],
        title: Annotated[str, "Title shown on the filter, for example 'Region'."],
        dimension: Annotated[
            str | None,
            "Fully qualified dimension to filter on, for example 'orders.region'. "
            "Required for a 'field' filter. Default: none.",
        ] = None,
        name: Annotated[
            str | None,
            "Filter name, which tiles use to listen to the filter, for example "
            "'region'. Default: derived from title, lowercased with other characters "
            "replaced by '_'.",
        ] = None,
        model: Annotated[
            str | None,
            "LookML model of the dimension, for example 'ecommerce'. Required for a "
            "'field' filter. Default: none.",
        ] = None,
        explore: Annotated[
            str | None,
            "Explore of the dimension, for example 'orders'. Required for a 'field' "
            "filter. Default: none.",
        ] = None,
        type: Annotated[
            Literal["field", "date", "number", "string"],
            "Filter type: 'field' (values of a dimension; the default), 'date', "
            "'number' or 'string'.",
        ] = "field",
        default_value: Annotated[
            str | None,
            "Starting value as a Looker filter expression, for example '90 days' or "
            "'West'. Default: no value.",
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "add_dashboard_filter", "content", {"dashboard_id": dashboard_id}
        )
        try:
            if type == "field" and not (dimension and model and explore):
                raise ValueError(
                    "add_dashboard_filter: a 'field' filter requires dimension=…, "
                    "model=… and explore=…, naming the dimension and where it comes "
                    "from."
                )
            async with client.session(ctx) as session:
                filter_name = name or _filter_name_from_title(title)
                body: dict[str, Any] = {
                    "dashboard_id": dashboard_id,
                    "name": filter_name,
                    "title": title,
                    "type": type,
                }
                if dimension:
                    body["dimension"] = dimension
                if model:
                    body["model"] = model
                if explore:
                    body["explore"] = explore
                if default_value:
                    body["default_value"] = default_value

                # Looker creates dashboard filters at ``POST /dashboard_filters``
                # with the dashboard named in the body; the per-dashboard path
                # only lists them.
                filt = await session.post("/dashboard_filters", body=body)
                return json.dumps(
                    {
                        "id": filt.get("id"),
                        "name": filt.get("name") or filter_name,
                        "title": filt.get("title"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("add_dashboard_filter", e)

    # ── Embed ────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Create an embed URL for Looker content, such as a dashboard, Look or "
            "explore, that carries its own authentication, so the viewer needs no Looker "
            "login. Use it when content has to appear inside another application. Treat "
            "the URL as a credential and share it only with the intended viewer."
        ),
    )
    async def generate_embed_url(
        target_url: Annotated[str, "Path of the content to embed, for example '/dashboards/17'."],
        session_length: Annotated[
            int, "Length of the embed session in seconds, for example 600. Default 3600."
        ] = 3600,
        force_logout_login: Annotated[
            bool,
            "Set true to end the viewer's existing Looker session before logging in. "
            "Default false.",
        ] = False,
    ) -> str:
        ctx = client.build_context("generate_embed_url", "content")
        try:
            async with client.session(ctx) as session:
                body = {
                    "target_url": target_url,
                    "session_length": session_length,
                    "force_logout_login": force_logout_login,
                }
                result = await session.post("/embed/sso_url", body=body)
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("generate_embed_url", e)

    @server.tool(
        description=(
            "Run Looker's content validator over every Look and dashboard and return "
            "the broken references, counted by kind, such as a missing explore or a "
            "renamed field. Use it after a LookML change to find content that will show "
            "errors before users see them. It can take minutes on a large instance, and "
            "the response lists every broken item, so it can be long."
        ),
    )
    async def validate_content() -> str:
        ctx = client.build_context("validate_content", "content")
        try:
            async with client.session(ctx) as session:
                result = await session.get("/content_validation")
                errors = result.get("content_with_errors") or [] if result else []
                summary: dict[str, int] = {}
                for entry in errors:
                    for e in entry.get("errors", []) or []:
                        kind = e.get("kind") or "unknown"
                        summary[kind] = summary.get(kind, 0) + 1
                return json.dumps(
                    {
                        "total_errors": (result or {}).get("total_errors"),
                        "total_looks_validated": (result or {}).get("total_looks_validated"),
                        "total_dashboards_validated": (result or {}).get(
                            "total_dashboards_validated"
                        ),
                        "total_dashboard_elements_validated": (result or {}).get(
                            "total_dashboard_elements_validated"
                        ),
                        "errors_by_kind": summary,
                        "broken_content": errors,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("validate_content", e)
