"""Modeling tool group — LookML development.

Tools for browsing and editing LookML files and validating LookML syntax.
File operations automatically use dev-mode workspace context.
Does NOT include Git operations or deployment — those are in the ``git``
tool group.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg, _set_if

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
                files = await session.get(
                    f"/projects/{_path_seg(project_id)}/files", params=_DEV_PARAMS
                )
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
                    f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
                    params=_DEV_PARAMS,
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
                    f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
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
                    f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
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
                await session.delete(
                    f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
                    params=_DEV_PARAMS,
                )
                return json.dumps({"deleted": True, "file_id": file_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_file", e)

    @server.tool(
        description=(
            "Get full configuration for a single LookML project, including git "
            "remote settings, validation policy, and release management flags. "
            "For a trimmed summary across all projects, use ``list_projects``."
        ),
    )
    async def get_project(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("get_project", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                project = await session.get(f"/projects/{_path_seg(project_id)}")
                return json.dumps(project, indent=2)
        except Exception as e:
            return format_api_error("get_project", e)

    @server.tool(
        description=(
            "Create a new LookML project. The project starts with no git remote "
            "configured — call ``update_project`` to set ``git_remote_url`` and "
            "``create_project_deploy_key`` to generate a key that can be added "
            "as a deploy key on the git remote."
        ),
    )
    async def create_project(
        name: Annotated[str, "Project name (becomes the ID)"],
    ) -> str:
        ctx = client.build_context("create_project", "modeling", {"name": name})
        try:
            async with client.session(ctx) as session:
                project = await session.post("/projects", body={"name": name})
                return json.dumps(
                    {
                        "id": project.get("id") if project else name,
                        "name": project.get("name") if project else name,
                        "created": True,
                        "next_step": (
                            "Call update_project to configure the git remote, "
                            "then create_project_deploy_key if using deploy-key auth."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_project", e)

    @server.tool(
        description=(
            "Update a LookML project's configuration. Primary use is to "
            "connect a project to a git remote (set ``git_remote_url``, "
            "``git_service_name``, and either ``git_username`` with a deploy key "
            "or ``git_username_user_attribute`` for per-developer auth). Only "
            "fields you supply are modified; omitted fields are preserved."
        ),
    )
    async def update_project(
        project_id: Annotated[str, "LookML project ID"],
        name: Annotated[str | None, "New display name"] = None,
        git_remote_url: Annotated[str | None, "Git remote URL"] = None,
        git_username: Annotated[
            str | None, "Git username (use with deploy-key or basic auth)"
        ] = None,
        git_password: Annotated[str | None, "Git password (write-only)"] = None,
        git_service_name: Annotated[
            str | None,
            "Git service identifier, e.g. 'github', 'gitlab', 'bitbucket', 'custom'",
        ] = None,
        git_username_user_attribute: Annotated[
            str | None, "User attribute holding each developer's git username"
        ] = None,
        git_password_user_attribute: Annotated[
            str | None, "User attribute holding each developer's git password or token"
        ] = None,
        git_production_branch_name: Annotated[str | None, "Production branch name"] = None,
        pull_request_mode: Annotated[
            str | None, "'off', 'links', 'recommended', or 'required'"
        ] = None,
        validation_required: Annotated[
            bool | None, "Require LookML validation before deploy"
        ] = None,
        git_release_mgmt_enabled: Annotated[bool | None, "Enable Looker release management"] = None,
        allow_warnings: Annotated[
            bool | None, "Allow deploys even when LookML warnings are present"
        ] = None,
    ) -> str:
        ctx = client.build_context("update_project", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "name", name)
                _set_if(body, "git_remote_url", git_remote_url)
                _set_if(body, "git_username", git_username)
                _set_if(body, "git_password", git_password)
                _set_if(body, "git_service_name", git_service_name)
                _set_if(body, "git_username_user_attribute", git_username_user_attribute)
                _set_if(body, "git_password_user_attribute", git_password_user_attribute)
                _set_if(body, "git_production_branch_name", git_production_branch_name)
                _set_if(body, "pull_request_mode", pull_request_mode)
                _set_if(body, "validation_required", validation_required)
                _set_if(body, "git_release_mgmt_enabled", git_release_mgmt_enabled)
                _set_if(body, "allow_warnings", allow_warnings)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one of: name, git_remote_url, git_username, "
                                "git_password, git_service_name, git_username_user_attribute, "
                                "git_password_user_attribute, git_production_branch_name, "
                                "pull_request_mode, validation_required, "
                                "git_release_mgmt_enabled, allow_warnings."
                            ),
                        },
                        indent=2,
                    )

                project = await session.patch(f"/projects/{_path_seg(project_id)}", body=body)
                return json.dumps(
                    {
                        "id": project.get("id") if project else project_id,
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_project", e)

    @server.tool(
        description=(
            "Delete a LookML project. Any models that live in this project will "
            "stop working. This action cannot be undone."
        ),
    )
    async def delete_project(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("delete_project", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/projects/{_path_seg(project_id)}")
                return json.dumps({"deleted": True, "project_id": project_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_project", e)

    @server.tool(
        description=(
            "Read the parsed LookML manifest for a project, which declares the "
            "project's name, any LookML projects it depends on via ``local_dependency`` "
            "or ``remote_dependency``, and which database connections it references. "
            "Useful for auditing project dependencies before a change."
        ),
    )
    async def get_project_manifest(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("get_project_manifest", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                manifest = await session.get(f"/projects/{_path_seg(project_id)}/manifest")
                return json.dumps(manifest, indent=2)
        except Exception as e:
            return format_api_error("get_project_manifest", e)

    @server.tool(
        description=(
            "Read the existing SSH deploy key for a project (the public key that "
            "must be installed on the git remote as a deploy key for git operations "
            "to work). Returns 404 if no deploy key has been generated yet — call "
            "``create_project_deploy_key`` to generate one."
        ),
    )
    async def get_project_deploy_key(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("get_project_deploy_key", "modeling", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                # Looker returns the public key as a raw string, not JSON.
                key = await session.get(f"/projects/{_path_seg(project_id)}/git/deploy_key")
                return json.dumps(
                    {"project_id": project_id, "public_key": key},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("get_project_deploy_key", e)

    @server.tool(
        description=(
            "Generate a new SSH deploy key for a project and return its public "
            "key. Install the public key on the git remote (e.g. as a GitHub "
            "deploy key with write access) before the project can push to the "
            "remote. Calling this rotates any existing deploy key."
        ),
    )
    async def create_project_deploy_key(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context(
            "create_project_deploy_key", "modeling", {"project_id": project_id}
        )
        try:
            async with client.session(ctx) as session:
                key = await session.post(f"/projects/{_path_seg(project_id)}/git/deploy_key")
                return json.dumps(
                    {
                        "project_id": project_id,
                        "public_key": key,
                        "created": True,
                        "next_step": (
                            "Install this public key as a deploy key on the git "
                            "remote (with write access) before git push operations "
                            "will succeed."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_project_deploy_key", e)

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
                result = await session.post(f"/projects/{_path_seg(project_id)}/lookml_validation")
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
