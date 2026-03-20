"""Board tool group — Board, board section, and board item CRUD.

Boards are Looker's content curation surface (replacing the legacy Homepage
API).  A board contains *sections*, and each section contains *items* that
reference dashboards, Looks, or other content.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_board_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Boards ────────────────────────────────────────────────────────

    @server.tool(
        description="Search for boards by title or other criteria.",
    )
    async def list_boards(
        title: Annotated[str | None, "Filter by title (partial match)"] = None,
        limit: Annotated[int, "Maximum results"] = 50,
    ) -> str:
        ctx = client.build_context("list_boards", "board")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if title:
                    params["title"] = title
                boards = await session.get("/boards/search", params=params)
                result = [
                    {
                        "id": b.get("id"),
                        "title": b.get("title"),
                        "description": b.get("description"),
                        "created_at": b.get("created_at"),
                        "section_order": b.get("section_order"),
                    }
                    for b in (boards or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_boards", e)

    @server.tool(
        description=(
            "Get a board by ID, including its sections and items. Returns the full board structure."
        ),
    )
    async def get_board(
        board_id: Annotated[str, "ID of the board to retrieve"],
    ) -> str:
        ctx = client.build_context("get_board", "board", {"board_id": board_id})
        try:
            async with client.session(ctx) as session:
                board = await session.get(f"/boards/{board_id}")
                return json.dumps(board, indent=2)
        except Exception as e:
            return format_api_error("get_board", e)

    @server.tool(
        description="Create a new board with a title and optional description.",
    )
    async def create_board(
        title: Annotated[str, "Title for the new board"],
        description: Annotated[str | None, "Description of the board"] = None,
    ) -> str:
        ctx = client.build_context("create_board", "board")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"title": title}
                if description:
                    body["description"] = description
                board = await session.post("/boards", body=body)
                return json.dumps(
                    {
                        "id": board.get("id"),
                        "title": board.get("title"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_board", e)

    @server.tool(description="Update a board's title or description.")
    async def update_board(
        board_id: Annotated[str, "ID of the board to update"],
        title: Annotated[str | None, "New title"] = None,
        description: Annotated[str | None, "New description"] = None,
    ) -> str:
        ctx = client.build_context("update_board", "board", {"board_id": board_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if title is not None:
                    body["title"] = title
                if description is not None:
                    body["description"] = description
                board = await session.patch(f"/boards/{board_id}", body=body)
                return json.dumps(
                    {"id": board.get("id"), "title": board.get("title")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_board", e)

    @server.tool(description="Delete a board. This action cannot be undone.")
    async def delete_board(
        board_id: Annotated[str, "ID of the board to delete"],
    ) -> str:
        ctx = client.build_context("delete_board", "board", {"board_id": board_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/boards/{board_id}")
                return json.dumps({"deleted": True, "board_id": board_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_board", e)

    # ── Board sections ────────────────────────────────────────────────

    @server.tool(
        description="Get a board section by ID, including its items.",
    )
    async def get_board_section(
        board_section_id: Annotated[str, "ID of the board section"],
    ) -> str:
        ctx = client.build_context(
            "get_board_section", "board", {"board_section_id": board_section_id}
        )
        try:
            async with client.session(ctx) as session:
                section = await session.get(f"/board_sections/{board_section_id}")
                return json.dumps(section, indent=2)
        except Exception as e:
            return format_api_error("get_board_section", e)

    @server.tool(
        description="Create a new section within a board.",
    )
    async def create_board_section(
        board_id: Annotated[str, "ID of the parent board"],
        title: Annotated[str | None, "Section title"] = None,
        description: Annotated[str | None, "Section description"] = None,
    ) -> str:
        ctx = client.build_context("create_board_section", "board", {"board_id": board_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"board_id": board_id}
                if title:
                    body["title"] = title
                if description:
                    body["description"] = description
                section = await session.post("/board_sections", body=body)
                return json.dumps(
                    {
                        "id": section.get("id"),
                        "title": section.get("title"),
                        "board_id": section.get("board_id"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_board_section", e)

    @server.tool(description="Update a board section's title or description.")
    async def update_board_section(
        board_section_id: Annotated[str, "ID of the section to update"],
        title: Annotated[str | None, "New title"] = None,
        description: Annotated[str | None, "New description"] = None,
    ) -> str:
        ctx = client.build_context(
            "update_board_section", "board", {"board_section_id": board_section_id}
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if title is not None:
                    body["title"] = title
                if description is not None:
                    body["description"] = description
                section = await session.patch(f"/board_sections/{board_section_id}", body=body)
                return json.dumps(
                    {"id": section.get("id"), "title": section.get("title")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_board_section", e)

    @server.tool(description="Delete a board section and all its items.")
    async def delete_board_section(
        board_section_id: Annotated[str, "ID of the section to delete"],
    ) -> str:
        ctx = client.build_context(
            "delete_board_section", "board", {"board_section_id": board_section_id}
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/board_sections/{board_section_id}")
                return json.dumps(
                    {"deleted": True, "board_section_id": board_section_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_board_section", e)

    # ── Board items ───────────────────────────────────────────────────

    @server.tool(
        description="Get a board item by ID.",
    )
    async def get_board_item(
        board_item_id: Annotated[str, "ID of the board item"],
    ) -> str:
        ctx = client.build_context("get_board_item", "board", {"board_item_id": board_item_id})
        try:
            async with client.session(ctx) as session:
                item = await session.get(f"/board_items/{board_item_id}")
                return json.dumps(item, indent=2)
        except Exception as e:
            return format_api_error("get_board_item", e)

    @server.tool(
        description=(
            "Add an item (dashboard, Look, or URL) to a board section. "
            "Provide exactly one of dashboard_id, look_id, or url."
        ),
    )
    async def create_board_item(
        board_section_id: Annotated[str, "ID of the board section to add the item to"],
        dashboard_id: Annotated[str | None, "ID of a dashboard to add"] = None,
        look_id: Annotated[str | None, "ID of a Look to add"] = None,
        url: Annotated[str | None, "URL to add as a link item"] = None,
        title: Annotated[str | None, "Custom title for the item"] = None,
        description: Annotated[str | None, "Custom description for the item"] = None,
    ) -> str:
        ctx = client.build_context(
            "create_board_item", "board", {"board_section_id": board_section_id}
        )
        try:
            targets = [v for v in (dashboard_id, look_id, url) if v is not None]
            if len(targets) != 1:
                return json.dumps(
                    {
                        "error": "Provide exactly one of dashboard_id, look_id, or url.",
                        "status": 400,
                    },
                    indent=2,
                )
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"board_section_id": board_section_id}
                if dashboard_id:
                    body["dashboard_id"] = int(dashboard_id)
                if look_id:
                    body["look_id"] = int(look_id)
                if url:
                    body["url"] = url
                if title:
                    body["title"] = title
                if description:
                    body["description"] = description
                item = await session.post("/board_items", body=body)
                return json.dumps(
                    {
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "board_section_id": item.get("board_section_id"),
                        "dashboard_id": item.get("dashboard_id"),
                        "look_id": item.get("look_id"),
                        "url": item.get("url"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_board_item", e)

    @server.tool(description="Update a board item's properties.")
    async def update_board_item(
        board_item_id: Annotated[str, "ID of the board item to update"],
        title: Annotated[str | None, "New title"] = None,
        description: Annotated[str | None, "New description"] = None,
        board_section_id: Annotated[str | None, "Move to a different board section"] = None,
    ) -> str:
        ctx = client.build_context("update_board_item", "board", {"board_item_id": board_item_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if title is not None:
                    body["title"] = title
                if description is not None:
                    body["description"] = description
                if board_section_id is not None:
                    body["board_section_id"] = board_section_id
                item = await session.patch(f"/board_items/{board_item_id}", body=body)
                return json.dumps(
                    {"id": item.get("id"), "title": item.get("title")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_board_item", e)

    @server.tool(description="Remove an item from a board section.")
    async def delete_board_item(
        board_item_id: Annotated[str, "ID of the board item to delete"],
    ) -> str:
        ctx = client.build_context("delete_board_item", "board", {"board_item_id": board_item_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/board_items/{board_item_id}")
                return json.dumps(
                    {"deleted": True, "board_item_id": board_item_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_board_item", e)
