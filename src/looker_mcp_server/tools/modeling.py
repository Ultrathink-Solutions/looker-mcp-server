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
from ._helpers import ActAsUser, _maybe_use_branch, _path_seg, _set_if, _validate_branch_args


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

    @server.tool(
        description=(
            "List all LookML files in a project. Defaults to the dev workspace's "
            "currently-checked-out branch — set ``branch=…`` to atomically swap "
            "the dev workspace to a feature branch for the call (saved branch "
            "restored on exit), or ``dev_mode=False`` to read production files."
        ),
    )
    async def list_project_files(
        project_id: Annotated[str, "LookML project ID"],
        dev_mode: Annotated[bool, "Read from the dev workspace (default) or production"] = True,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call (saved branch "
            "restored on exit). Implies dev_mode=True.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "list_project_files",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    files = await session.get(f"/projects/{_path_seg(project_id)}/files")
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
            "Read the contents of a LookML file. Defaults to the dev workspace's "
            "currently-checked-out branch — set ``branch=…`` to atomically swap "
            "to a feature branch for the call, or ``dev_mode=False`` to read the "
            "production version."
        ),
    )
    async def get_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[
            str, "File path within the project (e.g. 'models/ecommerce.model.lkml')"
        ],
        dev_mode: Annotated[bool, "Read from dev workspace (default) or production"] = True,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call. Implies dev_mode=True.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "get_file",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    file_info = await session.get(
                        f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
                    )
                    return json.dumps(file_info, indent=2)
        except Exception as e:
            return format_api_error("get_file", e)

    @server.tool(
        description=(
            "Create a new LookML file in a project. Always operates on the dev "
            "workspace — Looker rejects writes to production LookML. Pair with "
            "``branch=…`` to scope the create to a specific feature branch."
        ),
    )
    async def create_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path (e.g. 'views/new_view.view.lkml')"],
        content: Annotated[str, "LookML source code for the file"],
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored on exit).",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "create_file",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            async with client.session(ctx, dev_mode=True) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    file_info = await session.post(
                        f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
                        body={"id": file_id, "content": content},
                    )
                    return json.dumps(
                        {"created": True, "id": file_info.get("id") if file_info else file_id},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("create_file", e)

    @server.tool(
        description=(
            "Update the content of an existing LookML file. Always operates on "
            "the dev workspace — production is read-only. Pair with ``branch=…`` "
            "to scope the edit to a specific feature branch."
        ),
    )
    async def update_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path within the project"],
        content: Annotated[str, "New LookML source code"],
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored on exit).",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "update_file",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            async with client.session(ctx, dev_mode=True) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    file_info = await session.patch(
                        f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
                        body={"content": content},
                    )
                    return json.dumps(
                        {"updated": True, "id": file_info.get("id") if file_info else file_id},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("update_file", e)

    @server.tool(
        description=(
            "Delete a LookML file from a project. Always operates on the dev "
            "workspace — production is read-only. Pair with ``branch=…`` to "
            "scope the deletion to a specific feature branch."
        ),
    )
    async def delete_file(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[str, "File path to delete"],
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored on exit).",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "delete_file",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            async with client.session(ctx, dev_mode=True) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    await session.delete(
                        f"/projects/{_path_seg(project_id)}/files/{_path_seg(file_id)}",
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
            "Validate LookML syntax and semantics for a project. By default "
            "validates production LookML. Pass ``branch=…`` (or ``dev_mode=True``) "
            "to validate the dev workspace's LookML — required for any CI flow "
            "checking that a PR doesn't introduce LookML errors. Returns "
            "errors[], warnings[], and ``valid`` (boolean) summary."
        ),
    )
    async def validate_project(
        project_id: Annotated[str, "LookML project ID to validate"],
        dev_mode: Annotated[
            bool,
            "Validate the dev workspace's LookML rather than production. "
            "Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored "
            "on exit). Implies dev_mode=True.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "validate_project",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    result = await session.post(
                        f"/projects/{_path_seg(project_id)}/lookml_validation"
                    )
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

    # ── LookML data tests ────────────────────────────────────────────

    @server.tool(
        description=(
            "List the LookML/data tests defined in a project. Each entry has a "
            "``name``, ``model``, ``explore``, ``file``, ``line``, and the "
            "``query_url_params`` Looker uses to run it. By default lists tests "
            "from production LookML; set ``branch=…`` (or ``dev_mode=True``) to "
            "list tests from a feature branch under review."
        ),
    )
    async def list_lookml_tests(
        project_id: Annotated[str, "LookML project ID"],
        file_id: Annotated[
            str | None, "Optional: restrict to tests defined in a single file"
        ] = None,
        dev_mode: Annotated[
            bool,
            "List tests from the dev workspace rather than production. "
            "Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored "
            "on exit). Implies dev_mode=True.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "list_lookml_tests",
            "modeling",
            {"project_id": project_id, "branch": branch, "act_as_user": act_as_user},
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    params: dict[str, Any] = {}
                    _set_if(params, "file_id", file_id)
                    tests = await session.get(
                        f"/projects/{_path_seg(project_id)}/lookml_tests",
                        params=params or None,
                    )
                    result = [
                        {
                            "name": t.get("name"),
                            "model": t.get("model_name"),
                            "explore": t.get("explore_name"),
                            "file": t.get("file"),
                            "line": t.get("line"),
                            "query_url_params": t.get("query_url_params"),
                        }
                        for t in (tests or [])
                    ]
                    return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_lookml_tests", e)

    @server.tool(
        description=(
            "Run LookML/data tests for a project and return the assertion "
            "results. Optionally filter by ``model``, ``test`` (by name), or "
            "``file_id``. By default runs against production LookML — set "
            "``branch=…`` (or ``dev_mode=True``) to validate a PR's tests "
            "against the warehouse data without deploying. This is the "
            "primary primitive for catching data-regression bugs in CI."
            "\n\nLooker compiles each test's ``explore_source`` query, runs "
            "it against the warehouse, and evaluates the assertion expression "
            "against the result rows. A failed assert returns a non-empty "
            "``errors[]`` array. Tests can take many minutes — the default "
            "per-call timeout is 1800s (30 min), matching what Spectacles "
            "uses for the same endpoint."
        ),
    )
    async def run_lookml_tests(
        project_id: Annotated[str, "LookML project ID"],
        model: Annotated[str | None, "Restrict to tests in a specific model"] = None,
        test: Annotated[str | None, "Restrict to a single test by name"] = None,
        file_id: Annotated[str | None, "Restrict to tests in a single file"] = None,
        dev_mode: Annotated[
            bool,
            "Run tests against the dev workspace's LookML rather than production. "
            "Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Atomically swap to this branch for the call (saved branch restored "
            "on exit). Implies dev_mode=True.",
        ] = None,
        act_as_user: ActAsUser = None,
        timeout: Annotated[
            float,
            "Per-call timeout in seconds. Defaults to 1800 (30 min) because "
            "data tests run real warehouse queries with assertions and can "
            "take a long time on large tables.",
        ] = 1800.0,
    ) -> str:
        ctx = client.build_context(
            "run_lookml_tests",
            "modeling",
            {
                "project_id": project_id,
                "model": model,
                "test": test,
                "branch": branch,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    params: dict[str, Any] = {}
                    _set_if(params, "model", model)
                    _set_if(params, "test", test)
                    _set_if(params, "file_id", file_id)
                    results = await session.get(
                        f"/projects/{_path_seg(project_id)}/lookml_tests/run",
                        params=params or None,
                        timeout=timeout,
                    )
                    # Pass through raw results — failures carry assertion-level
                    # detail (model_name, test_name, query_url, success,
                    # errors[]) that's exactly what a regression report needs.
                    failure_count = sum(1 for r in (results or []) if not r.get("success", True))
                    return json.dumps(
                        {
                            "passed": failure_count == 0,
                            "test_count": len(results or []),
                            "failure_count": failure_count,
                            "results": results or [],
                        },
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("run_lookml_tests", e)

    # ── Datagroups (cache management) ────────────────────────────────

    @server.tool(
        description=(
            "List all datagroups (LookML cache policies) defined in the "
            "instance. Returns each datagroup with its model, last trigger "
            "time, and current stale_before marker. Datagroups are how "
            "LookML controls when Looker's query cache is invalidated."
        ),
    )
    async def list_datagroups() -> str:
        ctx = client.build_context("list_datagroups", "modeling")
        try:
            async with client.session(ctx) as session:
                datagroups = await session.get("/datagroups")
                result = [
                    {
                        "id": d.get("id"),
                        "model_name": d.get("model_name"),
                        "name": d.get("name"),
                        "trigger_check_at": d.get("trigger_check_at"),
                        "triggered_at": d.get("triggered_at"),
                        "stale_before": d.get("stale_before"),
                    }
                    for d in (datagroups or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_datagroups", e)

    @server.tool(
        description=(
            "Invalidate the cache for a datagroup by setting its "
            "``stale_before`` to the current time. All cached query results "
            "associated with this datagroup will be considered stale on the "
            "next read, forcing a fresh query. Use after a manual data "
            "correction or to flush a stuck PDT."
        ),
    )
    async def reset_datagroup(
        datagroup_id: Annotated[str, "Datagroup ID (from list_datagroups)"],
    ) -> str:
        import time

        ctx = client.build_context("reset_datagroup", "modeling", {"datagroup_id": datagroup_id})
        try:
            async with client.session(ctx) as session:
                # stale_before is a unix timestamp; setting it to now()
                # invalidates the cache for all queries tagged with this
                # datagroup.
                body = {"stale_before": int(time.time())}
                updated = await session.patch(f"/datagroups/{_path_seg(datagroup_id)}", body=body)
                return json.dumps(
                    {
                        "id": updated.get("id") if updated else datagroup_id,
                        "reset": True,
                        "stale_before": body["stale_before"],
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("reset_datagroup", e)

    @server.tool(
        description=(
            "Get detail for a single datagroup, including its model, name, "
            "interval/sql triggers, last ``triggered_at`` time, and current "
            "``stale_before`` marker. Use to inspect a datagroup before "
            "calling ``trigger_datagroup`` or ``reset_datagroup``."
        ),
    )
    async def get_datagroup(
        datagroup_id: Annotated[str, "Datagroup ID (from list_datagroups)"],
    ) -> str:
        ctx = client.build_context("get_datagroup", "modeling", {"datagroup_id": datagroup_id})
        try:
            async with client.session(ctx) as session:
                d = await session.get(f"/datagroups/{_path_seg(datagroup_id)}")
                if not d:
                    return json.dumps({"id": datagroup_id, "found": False}, indent=2)
                return json.dumps(
                    {
                        "id": d.get("id"),
                        "model_name": d.get("model_name"),
                        "name": d.get("name"),
                        "trigger_check_at": d.get("trigger_check_at"),
                        "triggered_at": d.get("triggered_at"),
                        "stale_before": d.get("stale_before"),
                        "trigger_value": d.get("trigger_value"),
                        "trigger_error": d.get("trigger_error"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("get_datagroup", e)

    @server.tool(
        description=(
            "Trigger a datagroup by setting its ``triggered_at`` to the "
            "current time. Forces a PDT rebuild AND cache invalidation for "
            "everything tagged with the datagroup — distinct from "
            "``reset_datagroup``, which only updates ``stale_before`` (cache "
            "bust without a rebuild). Use this to manually pre-warm a PDT or "
            "to force-rebuild after upstream data corrections."
        ),
    )
    async def trigger_datagroup(
        datagroup_id: Annotated[str, "Datagroup ID (from list_datagroups)"],
    ) -> str:
        import time

        ctx = client.build_context("trigger_datagroup", "modeling", {"datagroup_id": datagroup_id})
        try:
            async with client.session(ctx) as session:
                body = {"triggered_at": int(time.time())}
                updated = await session.patch(f"/datagroups/{_path_seg(datagroup_id)}", body=body)
                return json.dumps(
                    {
                        "id": updated.get("id") if updated else datagroup_id,
                        "triggered": True,
                        "triggered_at": body["triggered_at"],
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("trigger_datagroup", e)

    # ── PDT (Persistent Derived Table) build administration ──────────

    @server.tool(
        description=(
            "Enqueue materialization of a PDT identified by ``model`` + "
            "``view``. Returns a ``materialization_id`` you can poll via "
            "``check_pdt_build`` and cancel via ``stop_pdt_build``. By "
            "default builds in production; pass ``workspace='dev'`` to "
            "build the dev-workspace's version of the PDT (useful when "
            "validating LookML changes that affect derived-table SQL)."
        ),
    )
    async def start_pdt_build(
        model_name: Annotated[str, "LookML model name owning the PDT"],
        view_name: Annotated[str, "View name of the PDT to build"],
        force_rebuild: Annotated[
            bool,
            "Force rebuild of dependent PDTs even if already materialized.",
        ] = False,
        force_full_incremental: Annotated[
            bool,
            "Force any incremental PDTs in the dependency chain to fully re-materialize.",
        ] = False,
        workspace: Annotated[
            str,
            "Workspace for materialization: ``'production'`` (default) or ``'dev'``.",
        ] = "production",
        source: Annotated[
            str | None,
            "Optional caller tag — surfaces in Looker's PDT build logs.",
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "start_pdt_build",
            "modeling",
            {"model_name": model_name, "view_name": view_name, "workspace": workspace},
        )
        try:
            async with client.session(ctx) as session:
                # Looker's start_pdt_build is GET (per OpenAPI spec); booleans
                # ride as the strings 'true'/'false' in the query string.
                params: dict[str, Any] = {"workspace": workspace}
                if force_rebuild:
                    params["force_rebuild"] = "true"
                if force_full_incremental:
                    params["force_full_incremental"] = "true"
                _set_if(params, "source", source)
                result = await session.get(
                    f"/derived_table/{_path_seg(model_name)}/{_path_seg(view_name)}/start",
                    params=params,
                )
                return json.dumps(
                    {
                        "materialization_id": (result or {}).get("materialization_id"),
                        "resp_text": (result or {}).get("resp_text"),
                        "status": (result or {}).get("status"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("start_pdt_build", e)

    @server.tool(
        description=(
            "Check the status of a PDT materialization started via "
            "``start_pdt_build``. Returns ``status`` (e.g. ``running``, "
            "``complete``, ``error``), progress ratio, and any "
            "``resource_usage`` Looker has emitted. Poll until status "
            "is terminal."
        ),
    )
    async def check_pdt_build(
        materialization_id: Annotated[str, "ID returned by ``start_pdt_build``"],
    ) -> str:
        ctx = client.build_context(
            "check_pdt_build", "modeling", {"materialization_id": materialization_id}
        )
        try:
            async with client.session(ctx) as session:
                result = await session.get(f"/derived_table/{_path_seg(materialization_id)}/status")
                return json.dumps(
                    {
                        "materialization_id": (result or {}).get("materialization_id"),
                        "status": (result or {}).get("status"),
                        "ratio": (result or {}).get("ratio"),
                        "resp_text": (result or {}).get("resp_text"),
                        "resource_usage": (result or {}).get("resource_usage"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("check_pdt_build", e)

    @server.tool(
        description=(
            "Cancel an in-flight PDT materialization. Use to free a stuck "
            "build or recover after a runaway query. Note: Looker's API "
            "implements stop as a GET (not DELETE) — this tool wraps that "
            "convention. The materialization_id comes from "
            "``start_pdt_build`` or from the PDT build log in "
            "``system__activity``."
        ),
    )
    async def stop_pdt_build(
        materialization_id: Annotated[str, "ID returned by ``start_pdt_build``"],
        source: Annotated[
            str | None,
            "Optional caller tag — surfaces in Looker's PDT build logs.",
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "stop_pdt_build", "modeling", {"materialization_id": materialization_id}
        )
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {}
                _set_if(params, "source", source)
                result = await session.get(
                    f"/derived_table/{_path_seg(materialization_id)}/stop",
                    params=params or None,
                )
                status = (result or {}).get("status")
                return json.dumps(
                    {
                        "materialization_id": (result or {}).get("materialization_id"),
                        # Derive ``stopped`` from the actual response state.
                        # A no-op stop (e.g., the materialization already
                        # finished) returns a non-stopped status, and we
                        # must not falsely report cancellation success.
                        "stopped": status == "stopped",
                        "status": status,
                        "resp_text": (result or {}).get("resp_text"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("stop_pdt_build", e)

    @server.tool(
        description=(
            "Get the PDT dependency graph for a single derived-table view. "
            "Returns a DOT-language description of the subgraph (the view "
            "and everything it transitively depends on). Useful before "
            "kicking off a force-rebuild — you can see what other PDTs "
            "will be regenerated."
        ),
    )
    async def graph_derived_tables_for_view(
        view: Annotated[str, "Derived-table view name"],
        models: Annotated[
            str | None,
            "Optional comma-separated model names to scope the search.",
        ] = None,
        workspace: Annotated[
            str,
            "Workspace to query: ``'production'`` (default) or ``'dev'``.",
        ] = "production",
    ) -> str:
        ctx = client.build_context(
            "graph_derived_tables_for_view",
            "modeling",
            {"view": view, "workspace": workspace},
        )
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"workspace": workspace}
                _set_if(params, "models", models)
                graph = await session.get(
                    f"/derived_table/graph/view/{_path_seg(view)}",
                    params=params,
                )
                return json.dumps(graph, indent=2)
        except Exception as e:
            return format_api_error("graph_derived_tables_for_view", e)

    @server.tool(
        description=(
            "Get the full PDT dependency graph for a model. Returns DOT-"
            "language graph description with optional color coding by "
            "build state (grey=not built, green=built, yellow=building, "
            "red=error). Use to audit PDT freshness across a whole model."
        ),
    )
    async def graph_derived_tables_for_model(
        model: Annotated[str, "LookML model name"],
        format: Annotated[str, "Graph format. Currently only 'dot' is supported."] = "dot",
        color: Annotated[
            bool,
            "Color the graph nodes by current build status.",
        ] = False,
    ) -> str:
        ctx = client.build_context("graph_derived_tables_for_model", "modeling", {"model": model})
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"format": format}
                if color:
                    params["color"] = "true"
                graph = await session.get(
                    f"/derived_table/graph/model/{_path_seg(model)}",
                    params=params,
                )
                return json.dumps(graph, indent=2)
        except Exception as e:
            return format_api_error("graph_derived_tables_for_model", e)
