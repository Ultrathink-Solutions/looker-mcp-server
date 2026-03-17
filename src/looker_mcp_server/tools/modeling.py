"""Modeling tool group — LookML development.

Tools for browsing and editing LookML files and validating LookML syntax.
File operations automatically use dev-mode workspace context.
Does NOT include Git operations or deployment — those are in the ``git``
tool group.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error

# Looker's file endpoints are dev-mode-only and require an explicit
# workspace_id query parameter.  Sessions are ephemeral (per tool call),
# so PATCH /session workspace state does not persist across calls.
_DEV_PARAMS: dict[str, str] = {"workspace_id": "dev"}


def register_modeling_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(description="List all LookML projects in the Looker instance.")
    async def list_projects() -> str:
        ctx = client.build_context("list_projects", "modeling")
        try:
            async with client.session(ctx) as session:
                projects = await session.get("/projects")
                result = [
                    {
                        "id": p.get("id"),
                        "name": p.get("name"),
                        "git_remote_url": p.get("git_remote_url"),
                        "validation_required": p.get("validation_required"),
                        "is_example": p.get("is_example"),
                    }
                    for p in (projects or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_projects", e)

    @server.tool(description="List all LookML files in a project (dev workspace).")
    async def list_project_files(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("list_project_files", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                files = await session.get(f"/projects/{project_id}/files", params=_DEV_PARAMS)
                result = [
                    {
                        "id": f.get("id"),
                        "title": f.get("title"),
                        "type": f.get("type"),
                        "extension": f.get("extension"),
                        "editable": f.get("editable"),
                    }
                    for f in (files or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_project_files", e)

    @server.tool(
        description=(
            "Read the contents of a LookML file (dev workspace). Returns the full source code."
        ),
    )
    async def get_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[
            str, "File path within the project (e.g. 'models/ecommerce.model.lkml')"
        ],
    ) -> str:
        ctx = client.build_context("get_file", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                file_info = await session.get(
                    f"/projects/{project_id}/files/{file_id}", params=_DEV_PARAMS
                )
                return json.dumps(file_info, indent=2)
        except Exception as e:
            return format_api_error("get_file", e)

    @server.tool(
        description="Create a new LookML file in a project (dev workspace).",
    )
    async def create_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path (e.g. 'views/new_view.view.lkml')"],
        content: Annotated[str, "LookML source code for the file"],
    ) -> str:
        ctx = client.build_context("create_file", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                file_info = await session.post(
                    f"/projects/{project_id}/files/{file_id}",
                    body={"id": file_id, "content": content},
                    params=_DEV_PARAMS,
                )
                return json.dumps(
                    {"created": True, "id": file_info.get("id") if file_info else file_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_file", e)

    @server.tool(
        description="Update the content of an existing LookML file (dev workspace).",
    )
    async def update_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path within the project"],
        content: Annotated[str, "New LookML source code"],
    ) -> str:
        ctx = client.build_context("update_file", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                file_info = await session.patch(
                    f"/projects/{project_id}/files/{file_id}",
                    body={"content": content},
                    params=_DEV_PARAMS,
                )
                return json.dumps(
                    {"updated": True, "id": file_info.get("id") if file_info else file_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_file", e)

    @server.tool(
        description="Delete a LookML file from a project (dev workspace).",
    )
    async def delete_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path to delete"],
    ) -> str:
        ctx = client.build_context("delete_file", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/projects/{project_id}/files/{file_id}", params=_DEV_PARAMS)
                return json.dumps({"deleted": True, "file_id": file_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_file", e)

    @server.tool(
        description=(
            "Validate LookML syntax and semantics for a project. "
            "Returns any errors or warnings found."
        ),
    )
    async def validate_project(
        project_id: Annotated[str, "LookML project ID to validate"],
    ) -> str:
        ctx = client.build_context("validate_project", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                result = await session.post(f"/projects/{project_id}/lookml_validation")
                errors = result.get("errors") or [] if result else []
                warnings = result.get("warnings") or [] if result else []
                return json.dumps(
                    {
                        "valid": len(errors) == 0,
                        "error_count": len(errors),
                        "warning_count": len(warnings),
                        "errors": [
                            {
                                "severity": e.get("severity"),
                                "kind": e.get("kind"),
                                "message": e.get("message"),
                                "source_file": e.get("source_file"),
                                "line": e.get("line"),
                            }
                            for e in errors
                        ],
                        "warnings": [
                            {
                                "severity": w.get("severity"),
                                "message": w.get("message"),
                                "source_file": w.get("source_file"),
                            }
                            for w in warnings
                        ],
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("validate_project", e)
