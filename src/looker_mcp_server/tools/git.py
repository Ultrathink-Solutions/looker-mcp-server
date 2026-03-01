"""Git tool group — LookML version control and deployment.

Tools for managing Git branches within LookML projects and deploying
changes to production.  Separated from the ``modeling`` group so that
LookML editing can be enabled without granting deployment access.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_git_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description="Get the currently active Git branch for a LookML project.",
    )
    async def get_git_branch(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("get_git_branch", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                branch = await session.get(f"/projects/{project_id}/git_branch")
                return json.dumps(
                    {
                        "name": branch.get("name"),
                        "ref": branch.get("ref"),
                        "remote": branch.get("remote"),
                        "remote_ref": branch.get("remote_ref"),
                        "is_local": branch.get("is_local"),
                        "is_production": branch.get("is_production"),
                        "ahead_count": branch.get("ahead_count"),
                        "behind_count": branch.get("behind_count"),
                        "can_update": branch.get("can_update"),
                        "error": branch.get("error"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("get_git_branch", e)

    @server.tool(
        description="List all Git branches for a LookML project.",
    )
    async def list_git_branches(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("list_git_branches", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                branches = await session.get(f"/projects/{project_id}/git_branches")
                result = [
                    {
                        "name": b.get("name"),
                        "remote": b.get("remote"),
                        "is_local": b.get("is_local"),
                        "is_production": b.get("is_production"),
                        "ahead_count": b.get("ahead_count"),
                        "behind_count": b.get("behind_count"),
                    }
                    for b in (branches or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_git_branches", e)

    @server.tool(
        description=(
            "Create a new Git branch in a LookML project. "
            "The branch is created from the current ref."
        ),
    )
    async def create_git_branch(
        project_id: Annotated[str, "LookML project ID"],
        branch_name: Annotated[str, "Name for the new branch"],
        ref: Annotated[str | None, "Git ref to branch from (default: current HEAD)"] = None,
    ) -> str:
        ctx = client.build_context("create_git_branch", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": branch_name}
                if ref:
                    body["ref"] = ref
                branch = await session.post(f"/projects/{project_id}/git_branch", body=body)
                return json.dumps(
                    {"name": branch.get("name"), "ref": branch.get("ref")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_git_branch", e)

    @server.tool(
        description=(
            "Switch to a different Git branch in a LookML project. Requires dev mode to be enabled."
        ),
    )
    async def switch_git_branch(
        project_id: Annotated[str, "LookML project ID"],
        branch_name: Annotated[str, "Name of the branch to switch to"],
    ) -> str:
        ctx = client.build_context("switch_git_branch", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                branch = await session.put(
                    f"/projects/{project_id}/git_branch",
                    body={"name": branch_name},
                )
                return json.dumps(
                    {
                        "switched_to": branch.get("name") if branch else branch_name,
                        "ref": branch.get("ref") if branch else None,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("switch_git_branch", e)

    @server.tool(
        description=(
            "Deploy the current dev branch to production. This makes all "
            "LookML changes in dev mode visible to all users. "
            "Ensure the project validates cleanly before deploying."
        ),
    )
    async def deploy_to_production(
        project_id: Annotated[str, "LookML project ID to deploy"],
    ) -> str:
        ctx = client.build_context("deploy_to_production", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                result = await session.post(f"/projects/{project_id}/deploy_ref_to_production")
                return json.dumps(
                    {"deployed": True, "project_id": project_id, "detail": result},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("deploy_to_production", e)

    @server.tool(
        description=(
            "Reset the dev branch to match production. Discards all "
            "uncommitted LookML changes in dev mode."
        ),
    )
    async def reset_to_production(
        project_id: Annotated[str, "LookML project ID to reset"],
    ) -> str:
        ctx = client.build_context("reset_to_production", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                result = await session.post(f"/projects/{project_id}/reset_to_production")
                return json.dumps(
                    {"reset": True, "project_id": project_id, "detail": result},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("reset_to_production", e)
