"""Folder tool group — Folder navigation and management.

Folders organise Looker content (dashboards, Looks) into a hierarchical tree.
These tools allow browsing, creating, updating, and deleting folders, as well
as inspecting a folder's children and ancestry.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_folder_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Folder listing & search ───────────────────────────────────────

    @server.tool(
        description="Search for folders by name or parent folder.",
    )
    async def list_folders(
        name: Annotated[str | None, "Filter by folder name (partial match)"] = None,
        parent_id: Annotated[str | None, "Filter by parent folder ID"] = None,
        limit: Annotated[int, "Maximum results"] = 50,
    ) -> str:
        ctx = client.build_context("list_folders", "folder")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if name:
                    params["name"] = name
                if parent_id:
                    params["parent_id"] = parent_id
                folders = await session.get("/folders/search", params=params)
                result = [
                    {
                        "id": f.get("id"),
                        "name": f.get("name"),
                        "parent_id": f.get("parent_id"),
                        "child_count": f.get("child_count"),
                        "dashboards_count": len(f.get("dashboards", [])),
                        "looks_count": len(f.get("looks", [])),
                    }
                    for f in (folders or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_folders", e)

    @server.tool(
        description="Get a folder by ID, including metadata and content counts.",
    )
    async def get_folder(
        folder_id: Annotated[str, "ID of the folder to retrieve"],
    ) -> str:
        ctx = client.build_context("get_folder", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                folder = await session.get(f"/folders/{folder_id}")
                return json.dumps(folder, indent=2)
        except Exception as e:
            return format_api_error("get_folder", e)

    @server.tool(
        description="Create a new folder inside a parent folder.",
    )
    async def create_folder(
        name: Annotated[str, "Name for the new folder"],
        parent_id: Annotated[str, "ID of the parent folder"],
    ) -> str:
        ctx = client.build_context("create_folder", "folder")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "parent_id": parent_id}
                folder = await session.post("/folders", body=body)
                return json.dumps(
                    {
                        "id": folder.get("id"),
                        "name": folder.get("name"),
                        "parent_id": folder.get("parent_id"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_folder", e)

    @server.tool(description="Update a folder's name or move it to a different parent.")
    async def update_folder(
        folder_id: Annotated[str, "ID of the folder to update"],
        name: Annotated[str | None, "New folder name"] = None,
        parent_id: Annotated[str | None, "Move to a different parent folder"] = None,
    ) -> str:
        ctx = client.build_context("update_folder", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if name is not None:
                    body["name"] = name
                if parent_id is not None:
                    body["parent_id"] = parent_id
                folder = await session.patch(f"/folders/{folder_id}", body=body)
                return json.dumps(
                    {
                        "id": folder.get("id"),
                        "name": folder.get("name"),
                        "parent_id": folder.get("parent_id"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_folder", e)

    @server.tool(
        description=(
            "Delete a folder. The folder must be empty (no child folders, "
            "dashboards, or Looks). This action cannot be undone."
        ),
    )
    async def delete_folder(
        folder_id: Annotated[str, "ID of the folder to delete"],
    ) -> str:
        ctx = client.build_context("delete_folder", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/folders/{folder_id}")
                return json.dumps({"deleted": True, "folder_id": folder_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_folder", e)

    # ── Folder tree navigation ────────────────────────────────────────

    @server.tool(
        description=("List a folder's direct child folders. Useful for browsing the folder tree."),
    )
    async def get_folder_children(
        folder_id: Annotated[str, "ID of the parent folder"],
        limit: Annotated[int, "Maximum child folders to return"] = 50,
    ) -> str:
        ctx = client.build_context("get_folder_children", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                children = await session.get(f"/folders/{folder_id}/children", params=params)
                result = [
                    {
                        "id": c.get("id"),
                        "name": c.get("name"),
                        "parent_id": c.get("parent_id"),
                        "child_count": c.get("child_count"),
                    }
                    for c in (children or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_folder_children", e)

    @server.tool(
        description=(
            "Get the full ancestry chain of a folder (from root to its parent). "
            "Useful for building breadcrumb navigation or understanding the "
            "folder hierarchy."
        ),
    )
    async def get_folder_ancestors(
        folder_id: Annotated[str, "ID of the folder"],
    ) -> str:
        ctx = client.build_context("get_folder_ancestors", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                ancestors = await session.get(f"/folders/{folder_id}/ancestors")
                result = [
                    {
                        "id": a.get("id"),
                        "name": a.get("name"),
                        "parent_id": a.get("parent_id"),
                    }
                    for a in (ancestors or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_folder_ancestors", e)

    # ── Folder content listing ────────────────────────────────────────

    @server.tool(
        description="List all Looks saved in a specific folder.",
    )
    async def get_folder_looks(
        folder_id: Annotated[str, "ID of the folder"],
    ) -> str:
        ctx = client.build_context("get_folder_looks", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                looks = await session.get(f"/folders/{folder_id}/looks")
                result = [
                    {
                        "id": lk.get("id"),
                        "title": lk.get("title"),
                        "description": lk.get("description"),
                    }
                    for lk in (looks or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_folder_looks", e)

    @server.tool(
        description="List all dashboards saved in a specific folder.",
    )
    async def get_folder_dashboards(
        folder_id: Annotated[str, "ID of the folder"],
    ) -> str:
        ctx = client.build_context("get_folder_dashboards", "folder", {"folder_id": folder_id})
        try:
            async with client.session(ctx) as session:
                dashboards = await session.get(f"/folders/{folder_id}/dashboards")
                result = [
                    {
                        "id": d.get("id"),
                        "title": d.get("title"),
                        "description": d.get("description"),
                    }
                    for d in (dashboards or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_folder_dashboards", e)
