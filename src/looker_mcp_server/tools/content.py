"""Content tool group — Look and dashboard CRUD.

Tools for creating, reading, updating, and deleting Looks and dashboards,
adding dashboard elements and filters, and generating embed URLs.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_content_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Looks ────────────────────────────────────────────────────────

    @server.tool(
        description="Search for saved Looks by title, description, or other criteria.",
    )
    async def list_looks(
        title: Annotated[str | None, "Filter by title (partial match)"] = None,
        folder_id: Annotated[str | None, "Filter by folder ID"] = None,
        limit: Annotated[int, "Maximum results"] = 50,
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
            "Create a new saved Look with a query definition. "
            "The Look is saved in the specified folder."
        ),
    )
    async def create_look(
        title: Annotated[str, "Title for the new Look"],
        model: Annotated[str, "LookML model name"],
        view: Annotated[str, "Explore/view name"],
        fields: Annotated[list[str], "Fields to include in the query"],
        folder_id: Annotated[str, "Folder ID to save the Look in"],
        filters: Annotated[dict[str, str] | None, "Query filters"] = None,
        sorts: Annotated[list[str] | None, "Sort expressions"] = None,
        limit: Annotated[int, "Row limit for the query"] = 500,
        description: Annotated[str | None, "Description of the Look"] = None,
    ) -> str:
        ctx = client.build_context("create_look", "content")
        try:
            async with client.session(ctx) as session:
                query_body: dict[str, Any] = {
                    "model": model,
                    "view": view,
                    "fields": fields,
                    "limit": str(limit),
                }
                if filters:
                    query_body["filters"] = filters
                if sorts:
                    query_body["sorts"] = sorts

                body: dict[str, Any] = {
                    "title": title,
                    "folder_id": folder_id,
                    "query": query_body,
                }
                if description:
                    body["description"] = description

                look = await session.post("/looks", body=body)
                return json.dumps(
                    {"id": look.get("id"), "title": look.get("title"), "url": look.get("url")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_look", e)

    @server.tool(description="Update a Look's metadata (title, description, folder).")
    async def update_look(
        look_id: Annotated[str, "ID of the Look to update"],
        title: Annotated[str | None, "New title"] = None,
        description: Annotated[str | None, "New description"] = None,
        folder_id: Annotated[str | None, "Move to a different folder"] = None,
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

    @server.tool(description="Delete a saved Look. This action cannot be undone.")
    async def delete_look(
        look_id: Annotated[str, "ID of the Look to delete"],
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
        description="Search for dashboards by title, description, or other criteria.",
    )
    async def list_dashboards(
        title: Annotated[str | None, "Filter by title (partial match)"] = None,
        folder_id: Annotated[str | None, "Filter by folder ID"] = None,
        limit: Annotated[int, "Maximum results"] = 50,
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
        description="Create a new empty dashboard in the specified folder.",
    )
    async def create_dashboard(
        title: Annotated[str, "Title for the new dashboard"],
        folder_id: Annotated[str, "Folder ID to save the dashboard in"],
        description: Annotated[str | None, "Description"] = None,
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

    @server.tool(description="Update a dashboard's metadata (title, description, folder).")
    async def update_dashboard(
        dashboard_id: Annotated[str, "ID of the dashboard to update"],
        title: Annotated[str | None, "New title"] = None,
        description: Annotated[str | None, "New description"] = None,
        folder_id: Annotated[str | None, "Move to a different folder"] = None,
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

    @server.tool(description="Delete a dashboard. This action cannot be undone.")
    async def delete_dashboard(
        dashboard_id: Annotated[str, "ID of the dashboard to delete"],
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
            "Add a visualization tile to a dashboard. Requires a query "
            "definition (model, view, fields) or a saved Look ID."
        ),
    )
    async def add_dashboard_element(
        dashboard_id: Annotated[str, "ID of the dashboard"],
        title: Annotated[str, "Title for the tile"],
        type: Annotated[str, "Element type: 'vis' (visualization), 'text', 'filter'"] = "vis",
        look_id: Annotated[str | None, "ID of a saved Look to embed"] = None,
        query_model: Annotated[str | None, "LookML model for an inline query"] = None,
        query_view: Annotated[str | None, "Explore/view for an inline query"] = None,
        query_fields: Annotated[list[str] | None, "Fields for an inline query"] = None,
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

                element = await session.post(
                    f"/dashboards/{dashboard_id}/dashboard_elements", body=body
                )
                return json.dumps(
                    {"id": element.get("id"), "title": element.get("title")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("add_dashboard_element", e)

    @server.tool(description="Add a filter to a dashboard.")
    async def add_dashboard_filter(
        dashboard_id: Annotated[str, "ID of the dashboard"],
        title: Annotated[str, "Filter display title"],
        dimension: Annotated[str, "Fully-qualified dimension name (e.g. 'orders.region')"],
        type: Annotated[str, "Filter type: 'field_filter', 'date_filter'"] = "field_filter",
        default_value: Annotated[str | None, "Default filter value"] = None,
    ) -> str:
        ctx = client.build_context(
            "add_dashboard_filter", "content", {"dashboard_id": dashboard_id}
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "dashboard_id": dashboard_id,
                    "title": title,
                    "dimension": dimension,
                    "type": type,
                }
                if default_value:
                    body["default_value"] = default_value

                filt = await session.post(
                    f"/dashboards/{dashboard_id}/dashboard_filters", body=body
                )
                return json.dumps({"id": filt.get("id"), "title": filt.get("title")}, indent=2)
        except Exception as e:
            return format_api_error("add_dashboard_filter", e)

    # ── Embed ────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Generate an embeddable SSO URL for Looker content (dashboards, "
            "Looks, or explores). The URL includes authentication so the "
            "viewer does not need separate Looker credentials."
        ),
    )
    async def generate_embed_url(
        target_url: Annotated[str, "Looker content URL path (e.g. '/dashboards/123')"],
        session_length: Annotated[int, "Session duration in seconds"] = 3600,
        force_logout_login: Annotated[bool, "Force new login session"] = False,
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
