"""Workflows tool group — task-oriented compositions over atomic tools.

These tools each orchestrate several Looker API calls to complete a full
admin task (provision a connection, bootstrap a LookML project, onboard a
user). They return a structured response that tells the caller what
happened at each step, so partial failures surface clearly rather than
bubbling up as one opaque error.

Everything these tools can do is also doable by calling the underlying
atomic tools (``connection``, ``modeling``, ``admin``, ``credentials``,
``user_attributes``) in sequence. The value is in the orchestration:
correct ordering, structured partial-failure reporting, and one-call
ergonomics for common jobs.

Admin-only surface; disabled by default. Enable with ``--groups workflows``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerApiError, LookerClient, format_api_error
from ._helpers import _path_seg, _set_if


def register_workflow_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Day 1: data layer ────────────────────────────────────────────

    @server.tool(
        description=(
            "Create a database connection and run Looker's built-in test "
            "suite against it in one call. Returns the created connection "
            "summary plus the per-check test breakdown (connect, query, "
            "tmp_table, cdt, pdt). If any check fails, the connection is "
            "still left in place — caller can update or delete it based on "
            "which specific check failed. Replaces calling "
            "``create_connection`` then ``test_connection`` separately."
        ),
    )
    async def provision_connection(
        name: Annotated[str, "Unique connection name"],
        dialect_name: Annotated[
            str, "Database dialect (e.g. 'snowflake', 'bigquery_standard_sql')"
        ],
        host: Annotated[str | None, "Database host"] = None,
        port: Annotated[int | None, "Database port"] = None,
        database: Annotated[str | None, "Default database name"] = None,
        username: Annotated[str | None, "Database username"] = None,
        password: Annotated[str | None, "Database password"] = None,
        schema: Annotated[str | None, "Default schema"] = None,
        tmp_db_name: Annotated[str | None, "Scratch schema for PDTs"] = None,
        ssl: Annotated[bool | None, "Use SSL/TLS"] = None,
        pdts_enabled: Annotated[bool | None, "Enable PDTs"] = None,
    ) -> str:
        ctx = client.build_context("provision_connection", "workflows", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "dialect_name": dialect_name}
                _set_if(body, "host", host)
                _set_if(body, "port", port)
                _set_if(body, "database", database)
                _set_if(body, "username", username)
                _set_if(body, "password", password)
                _set_if(body, "schema", schema)
                _set_if(body, "tmp_db_name", tmp_db_name)
                _set_if(body, "ssl", ssl)
                _set_if(body, "pdts_enabled", pdts_enabled)

                conn = await session.post("/connections", body=body)

                # Run Looker's test suite against the new connection.
                try:
                    results = await session.put(f"/connections/{_path_seg(name)}/test")
                    checks = [
                        {
                            "check": r.get("name"),
                            "status": r.get("status"),
                            "message": r.get("message"),
                        }
                        for r in (results or [])
                    ]
                    all_ok = bool(checks) and all(c["status"] == "success" for c in checks)
                    test_section = {"ran": True, "healthy": all_ok, "checks": checks}
                except Exception as test_err:
                    test_section = {
                        "ran": False,
                        "error": format_api_error("test_connection", test_err),
                    }

                return json.dumps(
                    {
                        "created": True,
                        "name": conn.get("name"),
                        "dialect_name": conn.get("dialect_name"),
                        "test": test_section,
                        "next_step": (
                            "If any test check failed, call update_connection "
                            "to correct the specific field (e.g. tmp_db_name "
                            "for tmp_table failures) then test_connection "
                            "again. Connection is left registered so LookML "
                            "can reference it regardless of test state."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("provision_connection", e)

    @server.tool(
        description=(
            "Stand up a new LookML project wired to a git remote. Creates "
            "the project, sets its git configuration, and generates an SSH "
            "deploy key. The response includes the deploy key's public half "
            "— the caller must install it on the git remote (e.g. as a "
            "GitHub deploy key with write access) before git push operations "
            "will succeed. Replaces calling ``create_project``, "
            "``update_project``, and ``create_project_deploy_key`` in "
            "sequence."
        ),
    )
    async def bootstrap_lookml_project(
        name: Annotated[str, "Project name (becomes the ID)"],
        git_remote_url: Annotated[str, "Git remote URL to clone/push to"],
        git_service_name: Annotated[
            str, "Git host — 'github', 'gitlab', 'bitbucket', or 'custom'"
        ] = "github",
        git_production_branch_name: Annotated[
            str | None, "Production branch (default is main/master depending on git service)"
        ] = None,
        pull_request_mode: Annotated[str, "'off', 'links', 'recommended', or 'required'"] = "links",
    ) -> str:
        ctx = client.build_context("bootstrap_lookml_project", "workflows", {"name": name})
        try:
            async with client.session(ctx) as session:
                # Step 1: create the project.
                project = await session.post("/projects", body={"name": name})
                project_id = (project or {}).get("id", name)

                # Step 2: attach the git configuration.
                update_body: dict[str, Any] = {
                    "git_remote_url": git_remote_url,
                    "git_service_name": git_service_name,
                    "pull_request_mode": pull_request_mode,
                }
                _set_if(update_body, "git_production_branch_name", git_production_branch_name)
                await session.patch(f"/projects/{_path_seg(project_id)}", body=update_body)

                # Step 3: generate the SSH deploy key.
                deploy_key = await session.post(f"/projects/{_path_seg(project_id)}/git/deploy_key")

                return json.dumps(
                    {
                        "created": True,
                        "project_id": project_id,
                        "git_remote_url": git_remote_url,
                        "git_service_name": git_service_name,
                        "deploy_key_public": deploy_key,
                        "next_step": (
                            "Install deploy_key_public on the git remote as "
                            "a deploy key with write access. Then create a "
                            "dev branch with create_git_branch and start "
                            "authoring LookML with create_file."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("bootstrap_lookml_project", e)

    # ── Day 2: LookML workflow ───────────────────────────────────────

    @server.tool(
        description=(
            "Commit a set of LookML file edits and deploy them to "
            "production. Writes each file in the dev workspace, runs LookML "
            "validation (unless ``validate=False``), and — only if "
            "validation passes — deploys to production. Returns a "
            "step-by-step report of what happened. If validation fails, "
            "no deploy is attempted and the dev-workspace edits remain "
            "for the caller to fix."
        ),
    )
    async def deploy_lookml_changes(
        project_id: Annotated[str, "LookML project ID"],
        files: Annotated[
            dict[str, str],
            "Mapping of file path (e.g. 'views/orders.view.lkml') to new content",
        ],
        validate: Annotated[
            bool, "Run LookML validation before deploy; skip deploy on errors"
        ] = True,
    ) -> str:
        if not files:
            return json.dumps(
                {
                    "error": "No files provided.",
                    "hint": (
                        "Pass a dict mapping file paths to file contents, e.g. "
                        "{'views/orders.view.lkml': '<lkml source>'}."
                    ),
                },
                indent=2,
            )

        ctx = client.build_context("deploy_lookml_changes", "workflows", {"project_id": project_id})
        dev_params = {"workspace_id": "dev"}
        per_file: list[dict[str, Any]] = []
        try:
            async with client.session(ctx) as session:
                # Step 1: write each file (PATCH if it exists, POST if not).
                # Looker's API returns 404 for PATCH on a nonexistent file, so
                # we try PATCH first and fall back to POST on that signal.
                for path, content in files.items():
                    pseg = _path_seg(project_id)
                    fseg = _path_seg(path)
                    # Only fall back to POST on a confirmed 404 (the file
                    # doesn't exist yet). Other errors — auth, permissions,
                    # 5xx, network — must propagate rather than being
                    # silently retried as a create, which would double
                    # the side effects and mask the real cause.
                    try:
                        await session.patch(
                            f"/projects/{pseg}/files/{fseg}",
                            body={"content": content},
                            params=dev_params,
                        )
                        per_file.append({"path": path, "action": "updated"})
                    except LookerApiError as e:
                        if e.status_code != 404:
                            raise
                        await session.post(
                            f"/projects/{pseg}/files/{fseg}",
                            body={"id": path, "content": content},
                            params=dev_params,
                        )
                        per_file.append({"path": path, "action": "created"})

                # Step 2: optional validation.
                validation_section: dict[str, Any] | None = None
                if validate:
                    result = await session.post(
                        f"/projects/{_path_seg(project_id)}/lookml_validation"
                    )
                    errors = (result or {}).get("errors") or []
                    warnings = (result or {}).get("warnings") or []
                    validation_section = {
                        "valid": len(errors) == 0,
                        "error_count": len(errors),
                        "warning_count": len(warnings),
                        "errors": errors,
                    }
                    if errors:
                        return json.dumps(
                            {
                                "files": per_file,
                                "validation": validation_section,
                                "deployed": False,
                                "reason": "Validation errors — skipped deploy.",
                                "next_step": (
                                    "Fix the reported errors with another "
                                    "deploy_lookml_changes call (or "
                                    "update_file directly), then retry."
                                ),
                            },
                            indent=2,
                        )

                # Step 3: deploy to production.
                await session.post(f"/projects/{_path_seg(project_id)}/deploy_to_production")

                return json.dumps(
                    {
                        "files": per_file,
                        "validation": validation_section,
                        "deployed": True,
                        "project_id": project_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("deploy_lookml_changes", e)

    @server.tool(
        description=(
            "Abandon dev-workspace changes and reset the project's dev "
            "branch to match production. Wraps ``reset_to_production`` with "
            "a required ``confirm=True`` guard because the operation cannot "
            "be undone. Use when LookML edits have gotten into a stuck state."
        ),
    )
    async def rollback_to_production(
        project_id: Annotated[str, "LookML project ID"],
        confirm: Annotated[
            bool, "Must be True; prevents accidental data loss from stale tool calls"
        ] = False,
    ) -> str:
        if not confirm:
            return json.dumps(
                {
                    "error": "Confirmation required.",
                    "hint": (
                        "This operation discards all dev-workspace changes on "
                        "the project. Re-issue with confirm=True to proceed."
                    ),
                },
                indent=2,
            )

        ctx = client.build_context(
            "rollback_to_production", "workflows", {"project_id": project_id}
        )
        try:
            async with client.session(ctx) as session:
                await session.post(
                    f"/projects/{_path_seg(project_id)}/reset_to_production",
                    params={"workspace_id": "dev"},
                )
                return json.dumps(
                    {
                        "reset": True,
                        "project_id": project_id,
                        "workspace": "dev",
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("rollback_to_production", e)

    # ── Day 3: onboarding + access ───────────────────────────────────

    @server.tool(
        description=(
            "Onboard a new user end-to-end in one call. Creates the user, "
            "attaches email credentials, assigns direct roles, adds to "
            "groups, sets per-user attribute values, and (optionally) sends "
            "the welcome email. The response reports per-step status so a "
            "partial failure (e.g. group assignment failed) is visible "
            "without the user record being rolled back. Replaces calling "
            "create_user + create_credentials_email + set_user_roles + "
            "add_group_user (×N) + set_user_attribute_user_value (×N) + "
            "send_password_reset in sequence."
        ),
    )
    async def provision_user(
        email: Annotated[str, "Email address (also login identifier)"],
        first_name: Annotated[str, "First name"],
        last_name: Annotated[str, "Last name"],
        role_ids: Annotated[list[int] | None, "Role IDs to assign directly"] = None,
        group_ids: Annotated[list[int] | None, "Group IDs to add the user to"] = None,
        user_attribute_values: Annotated[
            dict[str, str] | None,
            "Per-user attribute overrides, keyed by user_attribute_id",
        ] = None,
        send_invite: Annotated[
            bool, "Send the welcome / password-reset email after provisioning"
        ] = True,
    ) -> str:
        ctx = client.build_context("provision_user", "workflows", {"email": email})
        steps: list[dict[str, Any]] = []
        try:
            async with client.session(ctx) as session:
                # Step 1: create the user record.
                user_body: dict[str, Any] = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                }
                if role_ids:
                    user_body["role_ids"] = role_ids
                if group_ids:
                    user_body["group_ids"] = group_ids
                user = await session.post("/users", body=user_body)
                user_id = str((user or {}).get("id", ""))
                steps.append({"step": "create_user", "user_id": user_id})

                # Short-circuit if the create call didn't return a usable
                # id. Downstream endpoints take a user_id path param;
                # continuing with an empty id would issue malformed URLs
                # like /users//credentials_email and surface Looker 404s
                # that obscure the original root cause.
                if not user_id:
                    return json.dumps(
                        {
                            "error": "User creation returned no id.",
                            "hint": (
                                "Looker's POST /users responded without an "
                                "'id' field. Check the response body for an "
                                "API-level error before retrying."
                            ),
                            "user_response": user,
                            "steps": steps,
                        },
                        indent=2,
                    )

                # Step 2: attach email credentials so the user can log in.
                try:
                    await session.post(
                        f"/users/{_path_seg(user_id)}/credentials_email",
                        body={"email": email},
                    )
                    steps.append({"step": "create_credentials_email", "ok": True})
                except Exception as e:
                    steps.append(
                        {
                            "step": "create_credentials_email",
                            "ok": False,
                            "error": format_api_error("create_credentials_email", e),
                        }
                    )

                # Step 3: per-user attribute values (optional).
                if user_attribute_values:
                    for ua_id, value in user_attribute_values.items():
                        try:
                            await session.patch(
                                f"/users/{_path_seg(user_id)}/attribute_values/{_path_seg(ua_id)}",
                                body={"value": value},
                            )
                            steps.append(
                                {
                                    "step": "set_user_attribute_user_value",
                                    "user_attribute_id": ua_id,
                                    "ok": True,
                                }
                            )
                        except Exception as e:
                            steps.append(
                                {
                                    "step": "set_user_attribute_user_value",
                                    "user_attribute_id": ua_id,
                                    "ok": False,
                                    "error": format_api_error("set_user_attribute_user_value", e),
                                }
                            )

                # Step 4: invite email (optional, depends on step 2 succeeding).
                if send_invite:
                    try:
                        await session.post(
                            f"/users/{_path_seg(user_id)}/credentials_email/send_password_reset"
                        )
                        steps.append({"step": "send_password_reset", "ok": True})
                    except Exception as e:
                        steps.append(
                            {
                                "step": "send_password_reset",
                                "ok": False,
                                "error": format_api_error("send_password_reset", e),
                            }
                        )

                all_ok = all(s.get("ok", True) for s in steps)
                return json.dumps(
                    {
                        "user_id": user_id,
                        "email": email,
                        "all_steps_ok": all_ok,
                        "steps": steps,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("provision_user", e)

    @server.tool(
        description=(
            "Grant a user or group access to a role. For a user principal, "
            "this adds them to the role's direct user list; for a group "
            "principal, it adds the group to the role's group list. Either "
            "way the call is read-modify-write against the role's "
            "membership — existing assignments are preserved."
        ),
    )
    async def grant_access(
        principal_type: Annotated[str, "'user' or 'group'"],
        principal_id: Annotated[str, "User ID or group ID"],
        role_id: Annotated[str, "Role ID to grant"],
    ) -> str:
        if principal_type not in ("user", "group"):
            return json.dumps(
                {
                    "error": f"principal_type must be 'user' or 'group', got {principal_type!r}",
                },
                indent=2,
            )

        ctx = client.build_context(
            "grant_access",
            "workflows",
            {
                "principal_type": principal_type,
                "principal_id": principal_id,
                "role_id": role_id,
            },
        )
        try:
            async with client.session(ctx) as session:
                endpoint = "users" if principal_type == "user" else "groups"
                # Step 1: read current membership.
                current = await session.get(f"/roles/{_path_seg(role_id)}/{endpoint}")
                current_ids = [int(m["id"]) for m in (current or []) if m.get("id")]
                principal_int = int(principal_id)

                if principal_int in current_ids:
                    return json.dumps(
                        {
                            "already_granted": True,
                            "role_id": role_id,
                            "principal_type": principal_type,
                            "principal_id": principal_id,
                        },
                        indent=2,
                    )

                # Step 2: write the augmented list back.
                new_ids = sorted({*current_ids, principal_int})
                await session.put(f"/roles/{_path_seg(role_id)}/{endpoint}", body=new_ids)
                return json.dumps(
                    {
                        "granted": True,
                        "role_id": role_id,
                        "principal_type": principal_type,
                        "principal_id": principal_id,
                        "member_count": len(new_ids),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("grant_access", e)
