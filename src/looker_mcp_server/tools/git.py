"""Git tool group — LookML version control and deployment.

Tools for managing Git branches within LookML projects, deploying
changes to production, rotating SSH deploy keys for the LookML repo's
git remote, and running Looker's built-in git-connection diagnostics.
Separated from the ``modeling`` group so that LookML editing can be
enabled without granting deployment access.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg


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
                branch = await session.get(f"/projects/{_path_seg(project_id)}/git_branch")
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
                branches = await session.get(f"/projects/{_path_seg(project_id)}/git_branches")
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
            "Get a specific Git branch by name for a LookML project, including "
            "its full ref/remote/ahead/behind/error state. Use ``list_git_branches`` "
            "to enumerate branches first; this tool is the right choice when you "
            "already know the branch you want to inspect."
        ),
    )
    async def get_git_branch_by_name(
        project_id: Annotated[str, "LookML project ID"],
        branch_name: Annotated[str, "Name of the branch to inspect"],
    ) -> str:
        ctx = client.build_context(
            "get_git_branch_by_name",
            "git",
            {"project_id": project_id, "branch_name": branch_name},
        )
        try:
            async with client.session(ctx) as session:
                branch = await session.get(
                    f"/projects/{_path_seg(project_id)}/git_branch/{_path_seg(branch_name)}"
                )
                return json.dumps(branch, indent=2)
        except Exception as e:
            return format_api_error("get_git_branch_by_name", e)

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
                branch = await session.post(
                    f"/projects/{_path_seg(project_id)}/git_branch", body=body
                )
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
                    f"/projects/{_path_seg(project_id)}/git_branch",
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
            "Delete a local Git branch in a LookML project. Does NOT delete the "
            "branch on the remote. Will fail if the branch is currently checked "
            "out — switch to a different branch first via ``switch_git_branch``. "
            "Useful for sweeping abandoned dev branches accumulated during "
            "iterative LookML work."
        ),
    )
    async def delete_git_branch(
        project_id: Annotated[str, "LookML project ID"],
        branch_name: Annotated[str, "Name of the branch to delete"],
    ) -> str:
        ctx = client.build_context(
            "delete_git_branch",
            "git",
            {"project_id": project_id, "branch_name": branch_name},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(
                    f"/projects/{_path_seg(project_id)}/git_branch/{_path_seg(branch_name)}"
                )
                return json.dumps(
                    {"deleted": True, "project_id": project_id, "branch_name": branch_name},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_git_branch", e)

    @server.tool(
        description=(
            "Deploy LookML to production. By default deploys the project's "
            "current dev ref; pass ``branch`` or ``ref`` to deploy a specific "
            "named branch or commit. Ensure the project validates cleanly before "
            "deploying — this makes all changes visible to all users."
        ),
    )
    async def deploy_to_production(
        project_id: Annotated[str, "LookML project ID to deploy"],
        branch: Annotated[
            str | None,
            "Specific branch to deploy (defaults to current dev ref)",
        ] = None,
        ref: Annotated[
            str | None,
            "Specific commit ref to deploy (mutually informative with ``branch``)",
        ] = None,
    ) -> str:
        ctx = client.build_context("deploy_to_production", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {}
                if branch is not None:
                    params["branch"] = branch
                if ref is not None:
                    params["ref"] = ref
                result = await session.post(
                    f"/projects/{_path_seg(project_id)}/deploy_ref_to_production",
                    params=params or None,
                )
                return json.dumps(
                    {
                        "deployed": True,
                        "project_id": project_id,
                        "branch": branch,
                        "ref": ref,
                        "detail": result,
                    },
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
                result = await session.post(
                    f"/projects/{_path_seg(project_id)}/reset_to_production"
                )
                return json.dumps(
                    {"reset": True, "project_id": project_id, "detail": result},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("reset_to_production", e)

    # ── Deploy keys ──────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get the public SSH deploy key Looker uses to authenticate to the "
            "LookML project's git remote. Add this key to GitHub / GitLab / "
            "Bitbucket as a deploy key (typically read+write) before configuring "
            "the project's git connection. Returns 404 if no deploy key has "
            "been generated yet — call ``create_git_deploy_key`` first."
        ),
    )
    async def get_git_deploy_key(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("get_git_deploy_key", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                # Looker returns the public key as text/plain, not JSON.
                # Using session.get_text avoids tripping on response.json().
                public_key = await session.get_text(
                    f"/projects/{_path_seg(project_id)}/git/deploy_key"
                )
                return json.dumps(
                    {"project_id": project_id, "public_key": public_key},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("get_git_deploy_key", e)

    @server.tool(
        description=(
            "Generate a new SSH deploy key for the LookML project's git remote. "
            "If a key already exists, this rotates it — invalidating the old "
            "public key on the remote until you re-register the new one. Use "
            "this for credential rotation; the new public key is returned in "
            "the response and must be added to the git host (GitHub / GitLab / "
            "Bitbucket) before the project's git connection will work again."
        ),
    )
    async def create_git_deploy_key(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("create_git_deploy_key", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                # Looker returns the rotated public key as text/plain.
                public_key = await session.post_text(
                    f"/projects/{_path_seg(project_id)}/git/deploy_key"
                )
                return json.dumps(
                    {
                        "project_id": project_id,
                        "rotated": True,
                        "public_key": public_key,
                        "next_step": (
                            "Add this public key to the git host as a deploy key "
                            "with read+write access, then run "
                            "``run_git_connection_test`` to verify connectivity."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_git_deploy_key", e)

    # ── Connection tests / diagnostics ───────────────────────────────

    @server.tool(
        description=(
            "List the git-connection diagnostic tests available for a LookML "
            "project. Each entry has an ``id`` (used with ``run_git_connection_test``) "
            "and a human-readable ``description``. Test types vary by remote git "
            "provider and are dynamically generated by Looker based on the project's "
            "git configuration."
        ),
    )
    async def list_git_connection_tests(
        project_id: Annotated[str, "LookML project ID"],
        remote_url: Annotated[
            str | None,
            (
                "Optional: remote URL for a remote dependency test. Leave blank to "
                "list tests for the root project."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("list_git_connection_tests", "git", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] | None = (
                    {"remote_url": remote_url} if remote_url is not None else None
                )
                tests = await session.get(
                    f"/projects/{_path_seg(project_id)}/git_connection_tests",
                    params=params,
                )
                result = [
                    {"id": t.get("id"), "description": t.get("description")} for t in (tests or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_git_connection_tests", e)

    @server.tool(
        description=(
            "Run a single git-connection diagnostic test against a LookML "
            "project's git remote. Returns ``status`` ('pass' or 'fail') with a "
            "``message`` describing the failure cause when applicable. Common "
            "diagnoses: missing deploy key registration on the remote, wrong "
            "remote URL, no network egress to the git host, or stale SSH known-"
            "hosts entry on the Looker instance. Call ``list_git_connection_tests`` "
            "to discover valid ``test_id`` values for this project."
        ),
    )
    async def run_git_connection_test(
        project_id: Annotated[str, "LookML project ID"],
        test_id: Annotated[str, "Test id from ``list_git_connection_tests``"],
        remote_url: Annotated[
            str | None,
            "Optional: remote URL to test against (for remote-dependency tests)",
        ] = None,
        use_production: Annotated[
            str | None,
            (
                "Optional: 'true' to use the project's production git credentials "
                "instead of dev. Pass as a string (Looker query-param convention)."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "run_git_connection_test",
            "git",
            {"project_id": project_id, "test_id": test_id},
        )
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {}
                if remote_url is not None:
                    params["remote_url"] = remote_url
                if use_production is not None:
                    params["use_production"] = use_production
                result = await session.get(
                    f"/projects/{_path_seg(project_id)}/git_connection_tests/{_path_seg(test_id)}",
                    params=params or None,
                )
                return json.dumps(
                    {
                        "test_id": result.get("id") if result else test_id,
                        "status": result.get("status") if result else None,
                        "passed": (result.get("status") == "pass") if result else False,
                        "message": result.get("message") if result else None,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("run_git_connection_test", e)
