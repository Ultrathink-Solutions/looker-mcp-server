"""Workflows tool group — task-oriented compositions over atomic tools.

Covers both halves of Layer 2: provisioning workflows (get an instance to
a running state) and ops/audit workflows (keep it running, investigate,
wind down).  Each orchestrates several Looker API calls to complete a
full admin task and returns a structured response with per-step status
so partial failures surface rather than being swallowed.

Everything these tools can do is also doable by calling the underlying
atomic tools (``connection``, ``modeling``, ``admin``, ``credentials``,
``user_attributes``, ``audit``) in sequence.  The value is in the
orchestration: correct ordering, structured partial-failure reporting,
and one-call ergonomics for common jobs.

Admin-only surface; disabled by default.  Enable with ``--groups workflows``.
"""

from __future__ import annotations

import json
from datetime import UTC
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

    # ── User lifecycle ───────────────────────────────────────────────

    @server.tool(
        description=(
            "Offboard a user safely: optionally transfer their content, "
            "terminate active sessions, revoke API3 credentials, and either "
            "disable (default, reversible) or delete the user. Reports per-"
            "step status. Default ``deactivate_only=True`` is non-destructive "
            "— flip to False explicitly when you want the user record gone."
        ),
    )
    async def offboard_user(
        user_id: Annotated[str, "User ID"],
        terminate_sessions: Annotated[bool, "Terminate all active sessions for this user"] = True,
        revoke_api_credentials: Annotated[
            bool, "Delete all API3 credential pairs attached to this user"
        ] = True,
        deactivate_only: Annotated[
            bool, "Disable the user (True, reversible) vs delete (False, irreversible)"
        ] = True,
    ) -> str:
        ctx = client.build_context("offboard_user", "workflows", {"user_id": user_id})
        steps: list[dict[str, Any]] = []
        try:
            async with client.session(ctx) as session:
                uid = _path_seg(user_id)

                # Step 1: terminate sessions attributable to this user.
                if terminate_sessions:
                    try:
                        sessions_list = await session.get("/sessions") or []
                        victim_sessions = [
                            s for s in sessions_list if str(s.get("user_id")) == user_id
                        ]
                        for s in victim_sessions:
                            sid = s.get("id")
                            if sid is not None:
                                await session.delete(f"/sessions/{_path_seg(str(sid))}")
                        steps.append(
                            {
                                "step": "terminate_sessions",
                                "ok": True,
                                "count": len(victim_sessions),
                            }
                        )
                    except Exception as e:
                        steps.append(
                            {
                                "step": "terminate_sessions",
                                "ok": False,
                                "error": format_api_error("terminate_sessions", e),
                            }
                        )

                # Step 2: revoke all API3 credential pairs.
                if revoke_api_credentials:
                    try:
                        api_creds = await session.get(f"/users/{uid}/credentials_api3") or []
                        for c in api_creds:
                            cid = c.get("id")
                            if cid is not None:
                                await session.delete(
                                    f"/users/{uid}/credentials_api3/{_path_seg(str(cid))}"
                                )
                        steps.append(
                            {
                                "step": "revoke_api_credentials",
                                "ok": True,
                                "count": len(api_creds),
                            }
                        )
                    except Exception as e:
                        steps.append(
                            {
                                "step": "revoke_api_credentials",
                                "ok": False,
                                "error": format_api_error("revoke_api_credentials", e),
                            }
                        )

                # Step 3: disable or delete the user.
                try:
                    if deactivate_only:
                        await session.patch(f"/users/{uid}", body={"is_disabled": True})
                        steps.append({"step": "disable_user", "ok": True})
                    else:
                        await session.delete(f"/users/{uid}")
                        steps.append({"step": "delete_user", "ok": True})
                except Exception as e:
                    steps.append(
                        {
                            "step": "disable_user" if deactivate_only else "delete_user",
                            "ok": False,
                            "error": format_api_error("offboard_user", e),
                        }
                    )

                all_ok = all(s.get("ok", True) for s in steps)

                # deactivated / deleted must reflect the actual outcome
                # of step 3 — not the requested mode. A caller who sees
                # `deactivated: True` with `all_steps_ok: False` has a
                # contradictory response.
                def _step_succeeded(name: str) -> bool:
                    return any(s.get("step") == name and s.get("ok") is True for s in steps)

                return json.dumps(
                    {
                        "user_id": user_id,
                        "all_steps_ok": all_ok,
                        "requested_action": ("deactivate" if deactivate_only else "delete"),
                        "deactivated": _step_succeeded("disable_user"),
                        "deleted": _step_succeeded("delete_user"),
                        "steps": steps,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("offboard_user", e)

    @server.tool(
        description=(
            "Rotate a user's API3 credentials safely. Creates a new key "
            "pair (returning the client_secret once), and optionally "
            "deletes a specified previous pair. The default behavior does "
            "NOT delete any existing credentials — the caller should "
            "deploy the new pair to consumers, verify it works, then call "
            "this tool again (or ``delete_credentials_api3``) with the "
            "old credentials_api3_id to retire the old pair."
        ),
    )
    async def rotate_api_credentials(
        user_id: Annotated[str, "User ID"],
        delete_previous_id: Annotated[
            str | None,
            (
                "credentials_api3_id of a PREVIOUS key pair to delete. Pass "
                "only after you've verified the new pair works."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("rotate_api_credentials", "workflows", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                uid = _path_seg(user_id)

                # Create the new pair.
                new = await session.post(f"/users/{uid}/credentials_api3")

                deletion_section: dict[str, Any] | None = None
                if delete_previous_id is not None:
                    try:
                        await session.delete(
                            f"/users/{uid}/credentials_api3/{_path_seg(delete_previous_id)}"
                        )
                        deletion_section = {
                            "deleted_previous_id": delete_previous_id,
                            "ok": True,
                        }
                    except Exception as e:
                        deletion_section = {
                            "deleted_previous_id": delete_previous_id,
                            "ok": False,
                            "error": format_api_error("rotate_api_credentials", e),
                        }

                return json.dumps(
                    {
                        "rotated": True,
                        "user_id": user_id,
                        "new_credentials": {
                            "id": (new or {}).get("id"),
                            "client_id": (new or {}).get("client_id"),
                            "client_secret": (new or {}).get("client_secret"),
                        },
                        "old_pair_deletion": deletion_section,
                        "warning": (
                            "The client_secret is returned only once — store "
                            "it now. If delete_previous_id was not provided, "
                            "remember to delete the old pair after verifying "
                            "the new one."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("rotate_api_credentials", e)

    # ── Audit workflows ──────────────────────────────────────────────

    @server.tool(
        description=(
            "Audit query activity with a scope preset. Picks the right "
            "system__activity query shape for a common investigation: "
            "'slow' = longest-running queries first, 'errors' = failed "
            "queries only, 'frequent' = most-run queries (by user + "
            "query fingerprint), 'by_user' = a single user's queries, "
            "'by_content' = queries issued from a specific dashboard or "
            "look. For custom audit queries, use the atomic "
            "``get_query_history`` in the ``audit`` group or the generic "
            "``query`` tool."
        ),
    )
    async def audit_query_activity(
        scope: Annotated[str, "'slow', 'errors', 'frequent', 'by_user', or 'by_content'"],
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "7 days",
        user_email: Annotated[str | None, "Required when scope='by_user'"] = None,
        dashboard_id: Annotated[str | None, "Dashboard to scope to when scope='by_content'"] = None,
        look_id: Annotated[str | None, "Look to scope to when scope='by_content'"] = None,
        limit: Annotated[int, "Maximum rows to return"] = 100,
    ) -> str:
        if scope not in ("slow", "errors", "frequent", "by_user", "by_content"):
            return json.dumps(
                {
                    "error": (
                        f"scope must be one of slow / errors / frequent / "
                        f"by_user / by_content, got {scope!r}"
                    )
                },
                indent=2,
            )
        if scope == "by_user" and not user_email:
            return json.dumps({"error": "scope='by_user' requires user_email"}, indent=2)
        if scope == "by_content" and not dashboard_id and not look_id:
            return json.dumps(
                {"error": "scope='by_content' requires either dashboard_id or look_id"},
                indent=2,
            )
        # Passing both IDs would AND them into an intersection that
        # matches zero rows in practice (a query can be from a dashboard
        # OR a look, never both). Reject rather than silently return [].
        if scope == "by_content" and dashboard_id and look_id:
            return json.dumps(
                {
                    "error": (
                        "scope='by_content' accepts exactly one of "
                        "dashboard_id or look_id, not both"
                    ),
                },
                indent=2,
            )

        filters: dict[str, str] = {"history.created_time": date_range}
        sorts: list[str] = ["history.runtime desc"]
        fields = [
            "history.created_time",
            "user.email",
            "query.model",
            "query.view",
            "history.runtime",
            "history.status",
            "history.issuer_source",
            "history.result_source",
        ]

        if scope == "slow":
            # Default sort already surfaces slowest; no additional filter.
            pass
        elif scope == "errors":
            filters["history.status"] = "-complete"
            sorts = ["history.created_time desc"]
        elif scope == "frequent":
            # Most-run by user + model/view fingerprint.
            fields = ["user.email", "query.model", "query.view", "history.count"]
            sorts = ["history.count desc"]
        elif scope == "by_user":
            filters["user.email"] = user_email or ""
            sorts = ["history.created_time desc"]
        elif scope == "by_content":
            if dashboard_id:
                filters["dashboard.id"] = dashboard_id
            if look_id:
                filters["look.id"] = look_id
            sorts = ["history.created_time desc"]

        ctx = client.build_context(
            "audit_query_activity", "workflows", {"scope": scope, "date_range": date_range}
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "model": "system__activity",
                    "view": "history",
                    "fields": fields,
                    "filters": filters,
                    "sorts": sorts,
                    "limit": str(limit),
                }
                query_def = await session.post("/queries", body=body)
                rows = await session.get(f"/queries/{_path_seg(str(query_def['id']))}/run/json")
                row_count = len(rows) if isinstance(rows, list) else 0
                return json.dumps(
                    {"scope": scope, "row_count": row_count, "rows": rows},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("audit_query_activity", e)

    @server.tool(
        description=(
            "Produce a composite health report for the instance over a "
            "time window: failed PDT builds, failed scheduled-plan runs, "
            "and broken-content counts. Each per-query section returns "
            "``sample_count`` and a ``sample`` capped at 10 rows — intended "
            "for a triage at-a-glance view, not a full audit. If any "
            "section hits its sample cap, the section's ``truncated`` "
            "flag is set — drill into that section with the atomic audit "
            "tool for exact counts. If any section fails outright, the "
            "top-level ``healthy`` flag is False and ``partial_failure`` "
            "is True, so a caller can never mistake a failed query for a "
            "healthy instance."
        ),
    )
    async def audit_instance_health(
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "24 hours",
    ) -> str:
        ctx = client.build_context("audit_instance_health", "workflows", {"date_range": date_range})
        sections: dict[str, Any] = {}
        sample_limit = 10
        try:
            async with client.session(ctx) as session:
                # Section A: failed PDT builds.
                try:
                    body_pdt: dict[str, Any] = {
                        "model": "system__activity",
                        "view": "pdt_event_log",
                        "fields": [
                            "pdt_event_log.created_time",
                            "pdt_event_log.model_name",
                            "pdt_event_log.view_name",
                            "pdt_event_log.status_code",
                            "pdt_event_log.message",
                        ],
                        "filters": {
                            "pdt_event_log.created_time": date_range,
                            "pdt_event_log.status_code": "error",
                        },
                        "sorts": ["pdt_event_log.created_time desc"],
                        "limit": str(sample_limit),
                    }
                    qd = await session.post("/queries", body=body_pdt)
                    rows = await session.get(f"/queries/{_path_seg(str(qd['id']))}/run/json")
                    sample_count = len(rows) if isinstance(rows, list) else 0
                    sections["failed_pdt_builds"] = {
                        "sample_count": sample_count,
                        "sample": rows,
                        "truncated": sample_count >= sample_limit,
                    }
                except Exception as e:
                    sections["failed_pdt_builds"] = {
                        "error": format_api_error("failed_pdt_builds", e)
                    }

                # Section B: failed scheduled-plan runs.
                try:
                    body_sched: dict[str, Any] = {
                        "model": "system__activity",
                        "view": "scheduled_plan",
                        "fields": [
                            "scheduled_job.finalized_time",
                            "scheduled_plan.id",
                            "scheduled_plan.name",
                            "user.email",
                            "scheduled_job.status",
                            "scheduled_job.status_detail",
                        ],
                        "filters": {
                            "scheduled_job.finalized_time": date_range,
                            "scheduled_job.status": "-success",
                        },
                        "sorts": ["scheduled_job.finalized_time desc"],
                        "limit": str(sample_limit),
                    }
                    qd = await session.post("/queries", body=body_sched)
                    rows = await session.get(f"/queries/{_path_seg(str(qd['id']))}/run/json")
                    sample_count = len(rows) if isinstance(rows, list) else 0
                    sections["failed_schedule_runs"] = {
                        "sample_count": sample_count,
                        "sample": rows,
                        "truncated": sample_count >= sample_limit,
                    }
                except Exception as e:
                    sections["failed_schedule_runs"] = {
                        "error": format_api_error("failed_schedule_runs", e)
                    }

                # Section C: content validation (no date filter — snapshot).
                # total_errors is an exact count from Looker, not a sample.
                try:
                    result = await session.get("/content_validation")
                    sections["content_validation"] = {
                        "total_errors": (result or {}).get("total_errors"),
                        "total_looks_validated": (result or {}).get("total_looks_validated"),
                        "total_dashboards_validated": (result or {}).get(
                            "total_dashboards_validated"
                        ),
                    }
                except Exception as e:
                    sections["content_validation"] = {
                        "error": format_api_error("content_validation", e)
                    }

                # A section that errored out contributed zero to the
                # counters below, but we don't actually know its state.
                # Track that separately so 'healthy' can never be True
                # when any section was uninspectable.
                any_section_failed = any(
                    "error" in section_data for section_data in sections.values()
                )
                any_section_truncated = any(
                    section_data.get("truncated") for section_data in sections.values()
                )

                # sample_count is a lower bound when truncated is True.
                # Caller should treat sections with truncated=True as
                # "at least this many" and drill in via the atomic tools.
                sample_issue_count = (
                    sections.get("failed_pdt_builds", {}).get("sample_count", 0)
                    + sections.get("failed_schedule_runs", {}).get("sample_count", 0)
                    + (sections.get("content_validation", {}).get("total_errors") or 0)
                )

                return json.dumps(
                    {
                        "date_range": date_range,
                        "healthy": (
                            sample_issue_count == 0
                            and not any_section_failed
                            and not any_section_truncated
                        ),
                        "partial_failure": any_section_failed,
                        "sample_issue_count": sample_issue_count,
                        "any_section_truncated": any_section_truncated,
                        "sections": sections,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("audit_instance_health", e)

    @server.tool(
        description=(
            "List currently-running queries whose runtime exceeds "
            "``threshold_seconds``, and optionally kill them. "
            "``action='report'`` (default) returns the filtered list "
            "without mutating anything; ``action='kill'`` terminates each "
            "matching query via /running_queries DELETE. Use for instance "
            "triage when the database is saturated."
        ),
    )
    async def investigate_runaway_queries(
        threshold_seconds: Annotated[
            float, "Minimum runtime (in seconds) for a query to be considered runaway"
        ] = 300,
        action: Annotated[str, "'report' (default) or 'kill'"] = "report",
    ) -> str:
        if action not in ("report", "kill"):
            return json.dumps(
                {"error": f"action must be 'report' or 'kill', got {action!r}"},
                indent=2,
            )
        # A non-positive threshold would match every running query — with
        # action='kill' that's a kill-all. Reject before we get anywhere
        # near the DELETE loop.
        if threshold_seconds <= 0:
            return json.dumps(
                {
                    "error": (f"threshold_seconds must be positive, got {threshold_seconds!r}"),
                },
                indent=2,
            )

        ctx = client.build_context(
            "investigate_runaway_queries",
            "workflows",
            {"threshold_seconds": threshold_seconds, "action": action},
        )
        try:
            async with client.session(ctx) as session:
                running = await session.get("/running_queries") or []
                runaways = [q for q in running if (q.get("runtime") or 0) >= threshold_seconds]
                trimmed = [
                    {
                        "query_task_id": q.get("query_task_id"),
                        "query_id": q.get("query_id"),
                        "source": q.get("source"),
                        "runtime": q.get("runtime"),
                        "user_id": q.get("user_id"),
                        "user": (q.get("user") or {}).get("email"),
                    }
                    for q in runaways
                ]

                killed: list[dict[str, Any]] = []
                if action == "kill":
                    for q in runaways:
                        qtid = q.get("query_task_id")
                        if not qtid:
                            continue
                        try:
                            await session.delete(f"/running_queries/{_path_seg(str(qtid))}")
                            killed.append({"query_task_id": qtid, "ok": True})
                        except Exception as e:
                            killed.append(
                                {
                                    "query_task_id": qtid,
                                    "ok": False,
                                    "error": format_api_error("kill_query", e),
                                }
                            )

                return json.dumps(
                    {
                        "threshold_seconds": threshold_seconds,
                        "action": action,
                        "runaway_count": len(runaways),
                        "runaways": trimmed,
                        "killed": killed if action == "kill" else None,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("investigate_runaway_queries", e)

    @server.tool(
        description=(
            "Identify dashboards and looks that haven't been viewed in a "
            "long time — candidates for deletion or archival. Queries "
            "system__activity.content_usage over the window and filters "
            "to content with the lowest view counts. Default threshold "
            "is 90 days; tune via ``min_days_unused``."
        ),
    )
    async def find_stale_content(
        min_days_unused: Annotated[int, "Minimum days since last access"] = 90,
        content_type: Annotated[str, "'dashboard', 'look', or 'all'"] = "all",
        limit: Annotated[int, "Maximum rows to return"] = 100,
    ) -> str:
        # Looker filter syntax: `>=N` on days_since_last_accessed means
        # "at least N days since last view", which is what we want. The
        # earlier version of this tool used history.created_date with a
        # date-window expression, which filters on activity-record
        # creation rather than content last-view age — a different
        # question with near-random results for this use case.
        filters: dict[str, str] = {
            "content_usage.days_since_last_accessed": f">={min_days_unused}",
        }
        if content_type != "all":
            filters["content_usage.content_type"] = content_type

        ctx = client.build_context(
            "find_stale_content",
            "workflows",
            {"min_days_unused": min_days_unused, "content_type": content_type},
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "model": "system__activity",
                    "view": "content_usage",
                    "fields": [
                        "content_usage.content_type",
                        "content_usage.content_id",
                        "content_usage.content_title",
                        "content_usage.last_accessed_date",
                        "content_usage.view_count",
                    ],
                    "filters": filters,
                    "sorts": ["content_usage.last_accessed_date asc"],
                    "limit": str(limit),
                }
                qd = await session.post("/queries", body=body)
                rows = await session.get(f"/queries/{_path_seg(str(qd['id']))}/run/json")
                return json.dumps(
                    {
                        "min_days_unused": min_days_unused,
                        "content_type": content_type,
                        "stale_count": len(rows) if isinstance(rows, list) else 0,
                        "rows": rows,
                        "next_step": (
                            "Review the list and delete via delete_look / "
                            "delete_dashboard from the content group, or "
                            "archive by moving to a trash folder."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("find_stale_content", e)

    @server.tool(
        description=(
            "Terminate active user sessions older than ``max_age_days``. "
            "Returns count + list of terminated session ids. Default "
            "90 days matches common session-hygiene policies. Use "
            "``action='report'`` for a dry-run that lists stale sessions "
            "without terminating them."
        ),
    )
    async def disable_stale_sessions(
        max_age_days: Annotated[int, "Terminate sessions created older than this"] = 90,
        action: Annotated[str, "'report' (default dry-run) or 'terminate'"] = "report",
    ) -> str:
        if action not in ("report", "terminate"):
            return json.dumps(
                {"error": f"action must be 'report' or 'terminate', got {action!r}"},
                indent=2,
            )
        # Negative max_age_days pushes the cutoff into the future, which
        # would mark every session as "older than" the cutoff — with
        # action='terminate' that's a log-everyone-out. Reject early.
        if max_age_days < 0:
            return json.dumps(
                {
                    "error": (f"max_age_days must be non-negative, got {max_age_days!r}"),
                },
                indent=2,
            )

        from datetime import datetime, timedelta

        ctx = client.build_context(
            "disable_stale_sessions",
            "workflows",
            {"max_age_days": max_age_days, "action": action},
        )
        try:
            async with client.session(ctx) as session:
                sessions_list = await session.get("/sessions") or []
                cutoff = datetime.now(UTC) - timedelta(days=max_age_days)

                stale: list[dict[str, Any]] = []
                for s in sessions_list:
                    created_raw = s.get("created_at")
                    if not created_raw:
                        continue
                    try:
                        created = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                    except ValueError:
                        # Unparseable timestamp — skip rather than guess.
                        continue
                    # fromisoformat can return a naive datetime if the
                    # input string has no timezone designator. Comparing
                    # naive < aware raises TypeError, which would crash
                    # the loop and tank the whole call. Skip naive rows
                    # rather than assuming UTC.
                    if created.tzinfo is None:
                        continue
                    if created < cutoff:
                        stale.append(
                            {
                                "id": s.get("id"),
                                "user_id": s.get("user_id"),
                                "created_at": created_raw,
                                "ip_address": s.get("ip_address"),
                            }
                        )

                terminated: list[dict[str, Any]] = []
                if action == "terminate":
                    for s in stale:
                        sid = s.get("id")
                        if sid is None:
                            continue
                        try:
                            await session.delete(f"/sessions/{_path_seg(str(sid))}")
                            terminated.append({"id": sid, "ok": True})
                        except Exception as e:
                            terminated.append(
                                {
                                    "id": sid,
                                    "ok": False,
                                    "error": format_api_error("terminate_session", e),
                                }
                            )

                return json.dumps(
                    {
                        "max_age_days": max_age_days,
                        "action": action,
                        "stale_count": len(stale),
                        "stale_sessions": stale,
                        "terminated": terminated if action == "terminate" else None,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("disable_stale_sessions", e)
