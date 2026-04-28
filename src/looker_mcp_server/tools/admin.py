"""Admin tool group — user, role, group, and schedule management.

These tools require admin-level permissions in Looker and are disabled
by default.  Enable with ``--groups admin`` or ``LOOKER_GROUPS=admin``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg, _set_if


def register_admin_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Users ────────────────────────────────────────────────────────

    @server.tool(description="List all users in the Looker instance.")
    async def list_users(
        email: Annotated[str | None, "Filter by email address"] = None,
        limit: Annotated[int, "Maximum results"] = 100,
    ) -> str:
        ctx = client.build_context("list_users", "admin")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if email:
                    params["email"] = email
                users = await session.get("/users", params=params)
                result = [
                    {
                        "id": u.get("id"),
                        "email": u.get("email"),
                        "first_name": u.get("first_name"),
                        "last_name": u.get("last_name"),
                        "is_disabled": u.get("is_disabled"),
                        "role_ids": u.get("role_ids"),
                        "group_ids": u.get("group_ids"),
                    }
                    for u in (users or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_users", e)

    @server.tool(description="Get detailed information about a specific user.")
    async def get_user(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_user", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                user = await session.get(f"/users/{user_id}")
                return json.dumps(user, indent=2)
        except Exception as e:
            return format_api_error("get_user", e)

    @server.tool(
        description=(
            "Create a new user in Looker. ``email`` populates the user's "
            "email/password credentials object so the user can log in via "
            "the email-auth path; for SSO-only setups create the user "
            "without ``email`` and let SSO link credentials on first login."
        ),
    )
    async def create_user(
        first_name: Annotated[str, "First name"],
        last_name: Annotated[str, "Last name"],
        email: Annotated[
            str | None,
            "Email address (also creates email/password credentials for the user)",
        ] = None,
        role_ids: Annotated[list[int] | None, "Role IDs to assign"] = None,
        group_ids: Annotated[list[int] | None, "Group IDs to add the user to"] = None,
        is_disabled: Annotated[
            bool | None,
            "Create the user in a disabled state (useful for staged rollouts)",
        ] = None,
        home_folder_id: Annotated[str | None, "User's home folder id"] = None,
        locale: Annotated[
            str | None,
            "Preferred UI locale (e.g. 'en', 'de'). Overrides instance default.",
        ] = None,
        ui_state: Annotated[
            dict[str, Any] | None,
            "Per-user UI state dict (Looker-internal — typically left empty)",
        ] = None,
        models_dir_validated: Annotated[
            bool | None,
            "Mark the user's dev workspace as validated against production models",
        ] = None,
        can_manage_api3_creds: Annotated[
            bool | None,
            (
                "Permit the user to self-manage their API3 credentials. May only "
                "be assigned by Looker admins."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("create_user", "admin")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "first_name": first_name,
                    "last_name": last_name,
                }
                _set_if(body, "email", email)
                _set_if(body, "role_ids", role_ids)
                _set_if(body, "group_ids", group_ids)
                _set_if(body, "is_disabled", is_disabled)
                _set_if(body, "home_folder_id", home_folder_id)
                _set_if(body, "locale", locale)
                _set_if(body, "ui_state", ui_state)
                _set_if(body, "models_dir_validated", models_dir_validated)
                _set_if(body, "can_manage_api3_creds", can_manage_api3_creds)
                user = await session.post("/users", body=body)
                return json.dumps({"id": user.get("id"), "email": user.get("email")}, indent=2)
        except Exception as e:
            return format_api_error("create_user", e)

    @server.tool(
        description=(
            "Update a user's profile and access metadata. Group membership "
            "is NOT settable here — use ``add_group_user`` / "
            "``remove_group_user`` for that (``set_role_groups`` manages "
            "role-to-group bindings, not user membership). Email address is "
            "also not directly settable; update the user's email credentials "
            "object via the credentials tool group instead."
        ),
    )
    async def update_user(
        user_id: Annotated[str, "User ID to update"],
        first_name: Annotated[str | None, "New first name"] = None,
        last_name: Annotated[str | None, "New last name"] = None,
        is_disabled: Annotated[bool | None, "Disable or enable the user"] = None,
        role_ids: Annotated[list[int] | None, "New role IDs"] = None,
        home_folder_id: Annotated[str | None, "New home folder id"] = None,
        locale: Annotated[str | None, "New preferred UI locale"] = None,
        ui_state: Annotated[
            dict[str, Any] | None,
            "Replace the per-user UI state dict",
        ] = None,
        models_dir_validated: Annotated[
            bool | None,
            "Toggle the dev-workspace-validated flag",
        ] = None,
        can_manage_api3_creds: Annotated[
            bool | None,
            "Toggle self-management of API3 credentials (admin-only)",
        ] = None,
    ) -> str:
        ctx = client.build_context("update_user", "admin", {"user_id": user_id})
        # Build and validate the body BEFORE opening a Looker session so the
        # no-fields case short-circuits without a wasted login round-trip.
        body: dict[str, Any] = {}
        _set_if(body, "first_name", first_name)
        _set_if(body, "last_name", last_name)
        _set_if(body, "is_disabled", is_disabled)
        _set_if(body, "role_ids", role_ids)
        _set_if(body, "home_folder_id", home_folder_id)
        _set_if(body, "locale", locale)
        _set_if(body, "ui_state", ui_state)
        _set_if(body, "models_dir_validated", models_dir_validated)
        _set_if(body, "can_manage_api3_creds", can_manage_api3_creds)
        if not body:
            return json.dumps(
                {
                    "error": "No fields provided to update.",
                    "hint": (
                        "Pass at least one of: first_name, last_name, "
                        "is_disabled, role_ids, home_folder_id, locale, "
                        "ui_state, models_dir_validated, can_manage_api3_creds."
                    ),
                },
                indent=2,
            )

        try:
            async with client.session(ctx) as session:
                user = await session.patch(f"/users/{user_id}", body=body)
                return json.dumps(
                    {
                        "id": user.get("id"),
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_user", e)

    @server.tool(description="Delete a user from Looker. This action cannot be undone.")
    async def delete_user(
        user_id: Annotated[str, "User ID to delete"],
    ) -> str:
        ctx = client.build_context("delete_user", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{user_id}")
                return json.dumps({"deleted": True, "user_id": user_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_user", e)

    @server.tool(
        description="Attach email/password credentials to a user so they can log in.",
    )
    async def create_credentials_email(
        user_id: Annotated[str, "User ID"],
        email: Annotated[str, "Email address for login"],
        forced_password_reset_at_next_login: Annotated[
            bool | None,
            (
                "Force the user to change their password on next login. Useful "
                "when bootstrapping a user with a temporary password issued "
                "out-of-band."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("create_credentials_email", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"email": email}
                _set_if(
                    body,
                    "forced_password_reset_at_next_login",
                    forced_password_reset_at_next_login,
                )
                creds = await session.post(
                    f"/users/{_path_seg(user_id)}/credentials_email",
                    body=body,
                )
                return json.dumps(
                    {
                        "user_id": user_id,
                        "email": creds.get("email"),
                        "created": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_credentials_email", e)

    @server.tool(
        description=(
            "Get a user's email/password credentials metadata. Returns email, "
            "creation timestamp, and read-only fields like ``has_password``, "
            "``logged_in_at``, ``password_reset_url_expired``, and "
            "``account_setup_url_expired``. The password itself is never "
            "returned."
        ),
    )
    async def get_credentials_email(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_email", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_email")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_email", e)

    @server.tool(
        description=(
            "Update a user's email/password credentials. Only provided fields "
            "are changed — omitted fields are preserved. Use ``email`` to "
            "rename a user's login address (the User schema has no settable "
            "``email`` field; this is the canonical path). Use "
            "``forced_password_reset_at_next_login`` to require the user to "
            "rotate their password the next time they sign in."
        ),
    )
    async def update_credentials_email(
        user_id: Annotated[str, "User ID"],
        email: Annotated[str | None, "New email address for login"] = None,
        forced_password_reset_at_next_login: Annotated[
            bool | None,
            "Toggle the forced-password-reset-at-next-login flag",
        ] = None,
    ) -> str:
        ctx = client.build_context("update_credentials_email", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "email", email)
                _set_if(
                    body,
                    "forced_password_reset_at_next_login",
                    forced_password_reset_at_next_login,
                )

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one of: email, forced_password_reset_at_next_login."
                            ),
                        },
                        indent=2,
                    )

                creds = await session.patch(
                    f"/users/{_path_seg(user_id)}/credentials_email",
                    body=body,
                )
                return json.dumps(
                    {
                        "user_id": user_id,
                        "email": creds.get("email") if creds else email,
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_credentials_email", e)

    @server.tool(
        description=(
            "Remove a user's email/password credentials. They will no longer "
            "be able to log in with email + password until new credentials "
            "are attached via ``create_credentials_email``. Does not affect "
            "SSO credential links."
        ),
    )
    async def delete_credentials_email(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_email", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_email")
                return json.dumps(
                    {"deleted": True, "user_id": user_id, "credential_type": "email"},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_credentials_email", e)

    @server.tool(
        description=(
            "Send a password reset or account setup email to a user. "
            "The user must already have email/password credentials attached."
        ),
    )
    async def send_password_reset(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("send_password_reset", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.post(
                    f"/users/{_path_seg(user_id)}/credentials_email/send_password_reset",
                )
                return json.dumps(
                    {"user_id": user_id, "password_reset_sent": True},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("send_password_reset", e)

    # ── Roles ────────────────────────────────────────────────────────

    @server.tool(description="List all roles defined in Looker.")
    async def list_roles() -> str:
        ctx = client.build_context("list_roles", "admin")
        try:
            async with client.session(ctx) as session:
                roles = await session.get("/roles")
                result = [
                    {
                        "id": r.get("id"),
                        "name": r.get("name"),
                        "permission_set_id": r.get("permission_set_id"),
                        "model_set_id": r.get("model_set_id"),
                        "user_count": r.get("user_count"),
                    }
                    for r in (roles or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_roles", e)

    @server.tool(description="Create a new role with specified permissions and model access.")
    async def create_role(
        name: Annotated[str, "Role name"],
        permission_set_id: Annotated[int, "Permission set ID to associate"],
        model_set_id: Annotated[int, "Model set ID to associate"],
    ) -> str:
        ctx = client.build_context("create_role", "admin")
        try:
            async with client.session(ctx) as session:
                body = {
                    "name": name,
                    "permission_set_id": permission_set_id,
                    "model_set_id": model_set_id,
                }
                role = await session.post("/roles", body=body)
                return json.dumps({"id": role.get("id"), "name": role.get("name")}, indent=2)
        except Exception as e:
            return format_api_error("create_role", e)

    @server.tool(description="Get detailed information about a specific role.")
    async def get_role(
        role_id: Annotated[str, "Role ID"],
    ) -> str:
        ctx = client.build_context("get_role", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                role = await session.get(f"/roles/{role_id}")
                return json.dumps(role, indent=2)
        except Exception as e:
            return format_api_error("get_role", e)

    @server.tool(
        description="Update an existing role's name, permission set, or model set.",
    )
    async def update_role(
        role_id: Annotated[str, "Role ID to update"],
        name: Annotated[str | None, "New role name"] = None,
        permission_set_id: Annotated[int | None, "New permission set ID"] = None,
        model_set_id: Annotated[int | None, "New model set ID"] = None,
    ) -> str:
        ctx = client.build_context("update_role", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if name is not None:
                    body["name"] = name
                if permission_set_id is not None:
                    body["permission_set_id"] = permission_set_id
                if model_set_id is not None:
                    body["model_set_id"] = model_set_id
                role = await session.patch(f"/roles/{role_id}", body=body)
                return json.dumps(
                    {
                        "id": role.get("id"),
                        "name": role.get("name"),
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_role", e)

    @server.tool(description="Delete a role. This action cannot be undone.")
    async def delete_role(
        role_id: Annotated[str, "Role ID to delete"],
    ) -> str:
        ctx = client.build_context("delete_role", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/roles/{role_id}")
                return json.dumps({"deleted": True, "role_id": role_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_role", e)

    # ── Permission Sets ─────────────────────────────────────────────

    @server.tool(
        description=("List all valid permission strings that can be assigned to permission sets."),
    )
    async def list_permissions() -> str:
        ctx = client.build_context("list_permissions", "admin")
        try:
            async with client.session(ctx) as session:
                permissions = await session.get("/permissions")
                result = [
                    {
                        "permission": p.get("permission"),
                        "description": p.get("description"),
                    }
                    for p in (permissions or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_permissions", e)

    @server.tool(description="List all permission sets defined in Looker.")
    async def list_permission_sets() -> str:
        ctx = client.build_context("list_permission_sets", "admin")
        try:
            async with client.session(ctx) as session:
                psets = await session.get("/permission_sets")
                result = [
                    {
                        "id": ps.get("id"),
                        "name": ps.get("name"),
                        "permissions": ps.get("permissions"),
                        "built_in": ps.get("built_in"),
                        "all_access": ps.get("all_access"),
                    }
                    for ps in (psets or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_permission_sets", e)

    @server.tool(
        description="Create a custom permission set with specified permissions.",
    )
    async def create_permission_set(
        name: Annotated[str, "Permission set name"],
        permissions: Annotated[
            list[str],
            "List of permission strings (use list_permissions to see valid values)",
        ],
    ) -> str:
        ctx = client.build_context("create_permission_set", "admin")
        try:
            async with client.session(ctx) as session:
                body = {"name": name, "permissions": permissions}
                pset = await session.post("/permission_sets", body=body)
                return json.dumps(
                    {
                        "id": pset.get("id"),
                        "name": pset.get("name"),
                        "permissions": pset.get("permissions"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_permission_set", e)

    @server.tool(
        description="Update an existing permission set's name or permissions.",
    )
    async def update_permission_set(
        permission_set_id: Annotated[str, "Permission set ID to update"],
        name: Annotated[str | None, "New name"] = None,
        permissions: Annotated[list[str] | None, "New list of permission strings"] = None,
    ) -> str:
        ctx = client.build_context(
            "update_permission_set",
            "admin",
            {"permission_set_id": permission_set_id},
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if name is not None:
                    body["name"] = name
                if permissions is not None:
                    body["permissions"] = permissions
                pset = await session.patch(f"/permission_sets/{permission_set_id}", body=body)
                return json.dumps(
                    {
                        "id": pset.get("id"),
                        "name": pset.get("name"),
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_permission_set", e)

    @server.tool(
        description="Delete a permission set. This action cannot be undone.",
    )
    async def delete_permission_set(
        permission_set_id: Annotated[str, "Permission set ID to delete"],
    ) -> str:
        ctx = client.build_context(
            "delete_permission_set",
            "admin",
            {"permission_set_id": permission_set_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/permission_sets/{permission_set_id}")
                return json.dumps(
                    {
                        "deleted": True,
                        "permission_set_id": permission_set_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_permission_set", e)

    # ── Model Sets ──────────────────────────────────────────────────

    @server.tool(description="List all model sets defined in Looker.")
    async def list_model_sets() -> str:
        ctx = client.build_context("list_model_sets", "admin")
        try:
            async with client.session(ctx) as session:
                msets = await session.get("/model_sets")
                result = [
                    {
                        "id": ms.get("id"),
                        "name": ms.get("name"),
                        "models": ms.get("models"),
                        "built_in": ms.get("built_in"),
                        "all_access": ms.get("all_access"),
                    }
                    for ms in (msets or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_model_sets", e)

    @server.tool(
        description="Create a custom model set with specified LookML models.",
    )
    async def create_model_set(
        name: Annotated[str, "Model set name"],
        models: Annotated[list[str], "List of LookML model names to include"],
    ) -> str:
        ctx = client.build_context("create_model_set", "admin")
        try:
            async with client.session(ctx) as session:
                body = {"name": name, "models": models}
                mset = await session.post("/model_sets", body=body)
                return json.dumps(
                    {
                        "id": mset.get("id"),
                        "name": mset.get("name"),
                        "models": mset.get("models"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_model_set", e)

    @server.tool(
        description="Update an existing model set's name or models.",
    )
    async def update_model_set(
        model_set_id: Annotated[str, "Model set ID to update"],
        name: Annotated[str | None, "New name"] = None,
        models: Annotated[list[str] | None, "New list of LookML model names"] = None,
    ) -> str:
        ctx = client.build_context("update_model_set", "admin", {"model_set_id": model_set_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if name is not None:
                    body["name"] = name
                if models is not None:
                    body["models"] = models
                mset = await session.patch(f"/model_sets/{model_set_id}", body=body)
                return json.dumps(
                    {
                        "id": mset.get("id"),
                        "name": mset.get("name"),
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_model_set", e)

    @server.tool(
        description="Delete a model set. This action cannot be undone.",
    )
    async def delete_model_set(
        model_set_id: Annotated[str, "Model set ID to delete"],
    ) -> str:
        ctx = client.build_context("delete_model_set", "admin", {"model_set_id": model_set_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/model_sets/{model_set_id}")
                return json.dumps(
                    {"deleted": True, "model_set_id": model_set_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_model_set", e)

    # ── Groups ───────────────────────────────────────────────────────

    @server.tool(description="List all user groups in Looker.")
    async def list_groups(
        limit: Annotated[int, "Maximum results"] = 100,
    ) -> str:
        ctx = client.build_context("list_groups", "admin")
        try:
            async with client.session(ctx) as session:
                groups = await session.get("/groups", params={"limit": limit})
                result = [
                    {
                        "id": g.get("id"),
                        "name": g.get("name"),
                        "user_count": g.get("user_count"),
                        "externally_managed": g.get("externally_managed"),
                    }
                    for g in (groups or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_groups", e)

    @server.tool(description="Add a user to a group.")
    async def add_group_user(
        group_id: Annotated[str, "Group ID"],
        user_id: Annotated[str, "User ID to add to the group"],
    ) -> str:
        ctx = client.build_context("add_group_user", "admin")
        try:
            async with client.session(ctx) as session:
                await session.post(f"/groups/{group_id}/users", body={"user_id": user_id})
                return json.dumps(
                    {"added": True, "group_id": group_id, "user_id": user_id}, indent=2
                )
        except Exception as e:
            return format_api_error("add_group_user", e)

    @server.tool(description="Remove a user from a group.")
    async def remove_group_user(
        group_id: Annotated[str, "Group ID"],
        user_id: Annotated[str, "User ID to remove from the group"],
    ) -> str:
        ctx = client.build_context("remove_group_user", "admin")
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/groups/{group_id}/users/{user_id}")
                return json.dumps(
                    {"removed": True, "group_id": group_id, "user_id": user_id}, indent=2
                )
        except Exception as e:
            return format_api_error("remove_group_user", e)

    @server.tool(description="Create a new user group.")
    async def create_group(
        name: Annotated[str, "Group name"],
        can_add_to_content_metadata: Annotated[
            bool | None,
            "Allow this group to be used in content access controls",
        ] = None,
    ) -> str:
        ctx = client.build_context("create_group", "admin")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name}
                _set_if(body, "can_add_to_content_metadata", can_add_to_content_metadata)
                group = await session.post("/groups", body=body)
                return json.dumps(
                    {"id": group.get("id"), "name": group.get("name")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_group", e)

    @server.tool(
        description=(
            "Update a group's metadata. Only provided fields are changed; "
            "omitted fields are preserved. Note: group *membership* is "
            "managed via ``add_group_user`` / ``remove_group_user`` and the "
            "group-of-groups tools, not via this update."
        ),
    )
    async def update_group(
        group_id: Annotated[str, "Group ID to update"],
        name: Annotated[str | None, "New group name"] = None,
        can_add_to_content_metadata: Annotated[
            bool | None,
            "Toggle whether the group can be used in content access controls",
        ] = None,
    ) -> str:
        ctx = client.build_context("update_group", "admin", {"group_id": group_id})
        # Build and validate the body BEFORE opening a Looker session so the
        # no-fields case short-circuits without a wasted login round-trip.
        body: dict[str, Any] = {}
        _set_if(body, "name", name)
        _set_if(body, "can_add_to_content_metadata", can_add_to_content_metadata)
        if not body:
            return json.dumps(
                {
                    "error": "No fields provided to update.",
                    "hint": "Pass at least one of: name, can_add_to_content_metadata.",
                },
                indent=2,
            )

        try:
            async with client.session(ctx) as session:
                group = await session.patch(f"/groups/{_path_seg(group_id)}", body=body)
                return json.dumps(
                    {
                        "id": group.get("id") if group else group_id,
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_group", e)

    @server.tool(description="Delete a group. This action cannot be undone.")
    async def delete_group(
        group_id: Annotated[str, "Group ID to delete"],
    ) -> str:
        ctx = client.build_context("delete_group", "admin", {"group_id": group_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/groups/{group_id}")
                return json.dumps({"deleted": True, "group_id": group_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_group", e)

    # ── Group hierarchy / membership readers ─────────────────────────

    @server.tool(
        description=(
            "List the users that are direct members of a group. Complements "
            "``add_group_user`` / ``remove_group_user`` for visibility — "
            "useful when auditing who has access via a group's role bindings."
        ),
    )
    async def list_group_users(
        group_id: Annotated[str, "Group ID"],
        limit: Annotated[int, "Maximum results"] = 100,
    ) -> str:
        ctx = client.build_context("list_group_users", "admin", {"group_id": group_id})
        try:
            async with client.session(ctx) as session:
                users = await session.get(
                    f"/groups/{_path_seg(group_id)}/users",
                    params={"limit": limit},
                )
                result = [
                    {
                        "id": u.get("id"),
                        "email": u.get("email"),
                        "first_name": u.get("first_name"),
                        "last_name": u.get("last_name"),
                        "is_disabled": u.get("is_disabled"),
                    }
                    for u in (users or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_group_users", e)

    @server.tool(
        description=(
            "List the groups that are direct sub-groups of a parent group. "
            "Looker supports nesting groups inside groups (group hierarchies); "
            "this lists one level. Roles assigned to a parent group are "
            "inherited by users in its sub-groups."
        ),
    )
    async def list_group_groups(
        group_id: Annotated[str, "Parent group ID"],
    ) -> str:
        ctx = client.build_context("list_group_groups", "admin", {"group_id": group_id})
        try:
            async with client.session(ctx) as session:
                groups = await session.get(f"/groups/{_path_seg(group_id)}/groups")
                result = [
                    {
                        "id": g.get("id"),
                        "name": g.get("name"),
                        "user_count": g.get("user_count"),
                    }
                    for g in (groups or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_group_groups", e)

    @server.tool(
        description=(
            "Add a sub-group to a parent group. Members of the sub-group "
            "automatically inherit any role bindings on the parent. Useful "
            "for building role-by-team hierarchies (e.g. 'all-engineers' "
            "contains 'data-team', 'platform-team', etc.)."
        ),
    )
    async def add_group_to_group(
        parent_group_id: Annotated[str, "Parent group ID"],
        child_group_id: Annotated[str, "Sub-group ID to add to the parent"],
    ) -> str:
        ctx = client.build_context(
            "add_group_to_group",
            "admin",
            {"parent_group_id": parent_group_id, "child_group_id": child_group_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.post(
                    f"/groups/{_path_seg(parent_group_id)}/groups",
                    body={"group_id": child_group_id},
                )
                return json.dumps(
                    {
                        "added": True,
                        "parent_group_id": parent_group_id,
                        "child_group_id": child_group_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("add_group_to_group", e)

    @server.tool(
        description=(
            "Remove a sub-group from a parent group. Inverse of "
            "``add_group_to_group``. The sub-group itself is not deleted."
        ),
    )
    async def remove_group_from_group(
        parent_group_id: Annotated[str, "Parent group ID"],
        child_group_id: Annotated[str, "Sub-group ID to remove from the parent"],
    ) -> str:
        ctx = client.build_context(
            "remove_group_from_group",
            "admin",
            {"parent_group_id": parent_group_id, "child_group_id": child_group_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(
                    f"/groups/{_path_seg(parent_group_id)}/groups/{_path_seg(child_group_id)}"
                )
                return json.dumps(
                    {
                        "removed": True,
                        "parent_group_id": parent_group_id,
                        "child_group_id": child_group_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("remove_group_from_group", e)

    # ── Role Assignments ────────────────────────────────────────────

    @server.tool(
        description=(
            "Set the groups assigned to a role. "
            "This replaces all current group assignments for the role."
        ),
    )
    async def set_role_groups(
        role_id: Annotated[str, "Role ID"],
        group_ids: Annotated[list[int], "List of group IDs to assign to this role"],
    ) -> str:
        ctx = client.build_context("set_role_groups", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                groups = await session.put(f"/roles/{role_id}/groups", body=group_ids)
                result = [{"id": g.get("id"), "name": g.get("name")} for g in (groups or [])]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("set_role_groups", e)

    @server.tool(
        description=(
            "Set the users assigned to a role. "
            "This replaces all current user assignments for the role."
        ),
    )
    async def set_role_users(
        role_id: Annotated[str, "Role ID"],
        user_ids: Annotated[list[int], "List of user IDs to assign to this role"],
    ) -> str:
        ctx = client.build_context("set_role_users", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                users = await session.put(f"/roles/{role_id}/users", body=user_ids)
                result = [{"id": u.get("id"), "email": u.get("email")} for u in (users or [])]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("set_role_users", e)

    @server.tool(
        description=(
            "Set the roles assigned to a user. "
            "This replaces all current role assignments for the user."
        ),
    )
    async def set_user_roles(
        user_id: Annotated[str, "User ID"],
        role_ids: Annotated[list[int], "List of role IDs to assign to this user"],
    ) -> str:
        ctx = client.build_context("set_user_roles", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                roles = await session.put(f"/users/{user_id}/roles", body=role_ids)
                result = [{"id": r.get("id"), "name": r.get("name")} for r in (roles or [])]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("set_user_roles", e)

    @server.tool(description="Get all roles assigned to a specific user.")
    async def get_user_roles(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_user_roles", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                roles = await session.get(f"/users/{user_id}/roles")
                result = [
                    {
                        "id": r.get("id"),
                        "name": r.get("name"),
                        "permission_set_id": r.get("permission_set_id"),
                        "model_set_id": r.get("model_set_id"),
                    }
                    for r in (roles or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_user_roles", e)

    # ── Scheduled plans ──────────────────────────────────────────────

    @server.tool(description="List all scheduled delivery plans.")
    async def list_schedules(
        user_id: Annotated[str | None, "Filter by owner user ID"] = None,
        limit: Annotated[int, "Maximum results"] = 100,
    ) -> str:
        ctx = client.build_context("list_schedules", "admin")
        try:
            async with client.session(ctx) as session:
                params: dict[str, Any] = {"limit": limit}
                if user_id:
                    params["user_id"] = user_id
                schedules = await session.get("/scheduled_plans", params=params)
                result = [
                    {
                        "id": s.get("id"),
                        "name": s.get("name"),
                        "crontab": s.get("crontab"),
                        "look_id": s.get("look_id"),
                        "dashboard_id": s.get("dashboard_id"),
                        "enabled": s.get("enabled"),
                        "last_run_at": s.get("last_run_at"),
                        "next_run_at": s.get("next_run_at"),
                    }
                    for s in (schedules or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_schedules", e)

    @server.tool(
        description=(
            "Create a scheduled delivery plan for a Look, dashboard, LookML "
            "dashboard, or query. Exposes every writable ``WriteScheduledPlan`` "
            "field, so all delivery types (email, webhook, S3, SFTP), conditional "
            "delivery (require_results / require_no_results / require_change), "
            "PDF/render options, custom branded URLs, datagroup triggers, and "
            "delegated ownership are reachable.\n\n"
            "Targeting: pass exactly one of ``look_id``, ``dashboard_id``, "
            "``lookml_dashboard_id``, or ``query_id`` to choose what gets "
            "delivered.\n\n"
            "Destinations: prefer ``destinations`` (full ``ScheduledPlanDestination`` "
            "shape, supports email/webhook/s3/sftp). ``recipients`` is a "
            "shorthand that builds an email-only destinations array — useful "
            "for simple cases. Pass at most one."
        ),
    )
    async def create_schedule(
        name: Annotated[str, "Schedule name"],
        crontab: Annotated[
            str | None,
            (
                "Cron expression (e.g. '0 9 * * 1' for Mondays at 9am). "
                "Mutually exclusive with ``datagroup`` — pass exactly one."
            ),
        ] = None,
        # ── Target (pick one) ────────────────────────────────────────
        look_id: Annotated[str | None, "Look ID to schedule"] = None,
        dashboard_id: Annotated[str | None, "Dashboard ID to schedule"] = None,
        lookml_dashboard_id: Annotated[str | None, "LookML dashboard ID to schedule"] = None,
        query_id: Annotated[str | None, "Query ID to schedule"] = None,
        # ── Destinations ─────────────────────────────────────────────
        recipients: Annotated[
            list[str] | None,
            "Email-only shorthand: each address becomes an email destination",
        ] = None,
        email_format: Annotated[
            str,
            (
                "Format for the ``recipients`` shorthand's email destinations. "
                "Looker requires every destination to specify a format. Defaults "
                "to ``wysiwyg_pdf`` (Looker UI's default for dashboard email "
                "schedules). Other common values: ``csv``, ``xlsx``, ``html``, "
                "``json_detail``, ``assembled_pdf``. Ignored when "
                "``destinations`` is used (set ``format`` per destination there)."
            ),
        ] = "wysiwyg_pdf",
        destinations: Annotated[
            list[dict[str, Any]] | None,
            (
                "Full ``ScheduledPlanDestination`` array. Each entry accepts: "
                "``type`` ('email'|'webhook'|'s3'|'sftp'), ``address``, "
                "``format``, ``apply_formatting``, ``apply_vis``, ``message``, "
                "``parameters`` (JSON string for s3/sftp/webhook), and "
                "``secret_parameters`` (write-only JSON string for credentials)."
            ),
        ] = None,
        # ── Ownership ────────────────────────────────────────────────
        user_id: Annotated[
            str | None,
            (
                "Owner user id (defaults to the calling user). Lets admins "
                "schedule on behalf of others."
            ),
        ] = None,
        run_as_recipient: Annotated[
            bool | None,
            "Run as each recipient — only applies to email destinations",
        ] = None,
        # ── Trigger options ──────────────────────────────────────────
        enabled: Annotated[bool | None, "Whether the plan is active"] = None,
        run_once: Annotated[
            bool | None,
            "Run only one time, then disable (good for tests)",
        ] = None,
        datagroup: Annotated[
            str | None,
            (
                "Datagroup name — runs when the datagroup triggers. "
                "Mutually exclusive with ``crontab``."
            ),
        ] = None,
        timezone: Annotated[
            str | None,
            "Timezone for the crontab (default: Looker instance timezone)",
        ] = None,
        # ── Conditional delivery ─────────────────────────────────────
        require_results: Annotated[
            bool | None,
            "Only deliver when the dashboard/look returns rows",
        ] = None,
        require_no_results: Annotated[
            bool | None,
            "Only deliver when the dashboard/look returns NO rows (alerting on empty)",
        ] = None,
        require_change: Annotated[
            bool | None,
            "Only deliver when results changed since the last run",
        ] = None,
        send_all_results: Annotated[
            bool | None,
            "Run an unlimited query and send all results (overrides default row caps)",
        ] = None,
        # ── Filters ──────────────────────────────────────────────────
        filters_string: Annotated[
            str | None,
            "Query string applied to the look/dashboard at delivery time",
        ] = None,
        # ── Render / PDF options ─────────────────────────────────────
        include_links: Annotated[
            bool | None,
            "Include links back to Looker in the delivered content",
        ] = None,
        pdf_paper_size: Annotated[
            str | None,
            "PDF paper size: 'letter', 'legal', 'tabloid', 'a0'..'a4'",
        ] = None,
        pdf_landscape: Annotated[bool | None, "PDF landscape orientation"] = None,
        long_tables: Annotated[
            bool | None,
            "Expand table visualizations to full length (no row caps in PDF)",
        ] = None,
        inline_table_width: Annotated[
            int | None,
            "Pixel width for inline table visualizations",
        ] = None,
        color_theme: Annotated[
            str | None,
            "Color scheme name applied to the dashboard at delivery time",
        ] = None,
        embed: Annotated[bool | None, "Treat as an embed-context schedule"] = None,
        # ── Branded / custom URLs ────────────────────────────────────
        show_custom_url: Annotated[
            bool | None,
            "Show the custom link instead of the Looker link",
        ] = None,
        custom_url_base: Annotated[str | None, "Custom URL domain for the scheduled entity"] = None,
        custom_url_params: Annotated[
            str | None,
            "Custom URL path and query params for the scheduled entity",
        ] = None,
        custom_url_label: Annotated[str | None, "Custom URL label text"] = None,
    ) -> str:
        ctx = client.build_context("create_schedule", "admin")
        try:
            # Use ``is not None`` semantics throughout so an explicit empty list
            # (recipients=[] or destinations=[]) is detected as "the caller
            # named this argument" rather than "the caller omitted it." Truthy
            # checks let recipients=[] slip past the mutual-exclusion guard,
            # which then writes the wrong destinations block.
            if recipients is not None and destinations is not None:
                return json.dumps(
                    {
                        "error": "Pass either ``recipients`` or ``destinations``, not both.",
                        "hint": (
                            "``recipients`` is a shorthand that builds an email-only "
                            "destinations array — pass it OR ``destinations``."
                        ),
                    },
                    indent=2,
                )
            # crontab and datagroup are mutually exclusive trigger modes per
            # the WriteScheduledPlan spec ("can't be used in combination with
            # crontab"). Reject the combination up front rather than letting
            # Looker return a less actionable 422.
            if crontab is not None and datagroup is not None:
                return json.dumps(
                    {
                        "error": (
                            "``crontab`` and ``datagroup`` are mutually exclusive "
                            "trigger modes — pass exactly one."
                        ),
                        "hint": (
                            "Use ``crontab`` for time-based delivery, ``datagroup`` "
                            "for delivery on data refresh."
                        ),
                    },
                    indent=2,
                )
            target_count = sum(
                1 for v in (look_id, dashboard_id, lookml_dashboard_id, query_id) if v is not None
            )
            if target_count == 0:
                return json.dumps(
                    {
                        "error": "No target provided.",
                        "hint": (
                            "Pass exactly one of look_id, dashboard_id, "
                            "lookml_dashboard_id, query_id."
                        ),
                    },
                    indent=2,
                )
            if target_count > 1:
                return json.dumps(
                    {
                        "error": "Multiple targets provided.",
                        "hint": (
                            "Pass exactly one of look_id, dashboard_id, "
                            "lookml_dashboard_id, query_id."
                        ),
                    },
                    indent=2,
                )
            # Trigger required: a schedule with neither crontab nor datagroup
            # has no way to fire. Reject up front rather than letting Looker
            # 422 with a less actionable message.
            if crontab is None and datagroup is None:
                return json.dumps(
                    {
                        "error": "No trigger provided.",
                        "hint": (
                            "Pass either ``crontab`` (time-based) or "
                            "``datagroup`` (data-refresh-based)."
                        ),
                    },
                    indent=2,
                )
            # Destination required: a schedule with no destinations cannot
            # deliver. Resolve destinations vs. recipients shorthand once and
            # reject if the resolved list is empty (handles destinations=[]
            # with no fallback recipients, and the both-None case).
            resolved_destinations: list[dict[str, Any]] | None = None
            if destinations is not None:
                resolved_destinations = destinations
            elif recipients is not None:
                resolved_destinations = [
                    {"type": "email", "address": addr, "format": email_format}
                    for addr in recipients
                ]
            if not resolved_destinations:
                return json.dumps(
                    {
                        "error": "No destinations provided.",
                        "hint": (
                            "Pass at least one entry via ``destinations`` "
                            "(full ScheduledPlanDestination shape) or "
                            "``recipients`` (email shorthand). A schedule "
                            "with no destinations cannot deliver."
                        ),
                    },
                    indent=2,
                )

            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name}
                _set_if(body, "crontab", crontab)
                _set_if(body, "look_id", look_id)
                _set_if(body, "dashboard_id", dashboard_id)
                _set_if(body, "lookml_dashboard_id", lookml_dashboard_id)
                _set_if(body, "query_id", query_id)
                _set_if(body, "user_id", user_id)
                _set_if(body, "run_as_recipient", run_as_recipient)
                _set_if(body, "enabled", enabled)
                _set_if(body, "run_once", run_once)
                _set_if(body, "datagroup", datagroup)
                _set_if(body, "timezone", timezone)
                _set_if(body, "require_results", require_results)
                _set_if(body, "require_no_results", require_no_results)
                _set_if(body, "require_change", require_change)
                _set_if(body, "send_all_results", send_all_results)
                _set_if(body, "filters_string", filters_string)
                _set_if(body, "include_links", include_links)
                _set_if(body, "pdf_paper_size", pdf_paper_size)
                _set_if(body, "pdf_landscape", pdf_landscape)
                _set_if(body, "long_tables", long_tables)
                _set_if(body, "inline_table_width", inline_table_width)
                _set_if(body, "color_theme", color_theme)
                _set_if(body, "embed", embed)
                _set_if(body, "show_custom_url", show_custom_url)
                _set_if(body, "custom_url_base", custom_url_base)
                _set_if(body, "custom_url_params", custom_url_params)
                _set_if(body, "custom_url_label", custom_url_label)

                # The resolved destinations list was computed and validated
                # in the preflight above; reuse it directly.
                body["scheduled_plan_destination"] = resolved_destinations

                schedule = await session.post("/scheduled_plans", body=body)
                return json.dumps(
                    {"id": schedule.get("id"), "name": schedule.get("name")}, indent=2
                )
        except Exception as e:
            return format_api_error("create_schedule", e)

    @server.tool(description="Delete a scheduled delivery plan.")
    async def delete_schedule(
        schedule_id: Annotated[str, "Scheduled plan ID to delete"],
    ) -> str:
        ctx = client.build_context("delete_schedule", "admin", {"schedule_id": schedule_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/scheduled_plans/{_path_seg(schedule_id)}")
                return json.dumps({"deleted": True, "schedule_id": schedule_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_schedule", e)

    @server.tool(
        description=(
            "Update a scheduled delivery plan. Only provided fields are "
            "changed; omitted fields are preserved. Exposes every writable "
            "``WriteScheduledPlan`` field, so any aspect of an existing plan "
            "can be retargeted, rescheduled, re-routed, or reconfigured "
            "without rebuilding it. Returns an actionable error when no "
            "fields are supplied.\n\n"
            "Destinations: ``destinations`` and ``recipients`` are mutually "
            "exclusive. Setting either replaces the full destination list "
            "with the provided entries. Looker requires every ScheduledPlan "
            "to have at least one destination, so an empty list is rejected "
            "up front; to leave existing destinations unchanged, omit both "
            "arguments."
        ),
    )
    async def update_schedule(
        schedule_id: Annotated[str, "Scheduled plan ID to update"],
        name: Annotated[str | None, "New display name"] = None,
        crontab: Annotated[
            str | None, "New cron expression (e.g. '0 9 * * *' for 9am daily)"
        ] = None,
        # ── Target (retarget the schedule's source) ─────────────────
        look_id: Annotated[str | None, "New look_id"] = None,
        dashboard_id: Annotated[str | None, "New dashboard_id"] = None,
        lookml_dashboard_id: Annotated[str | None, "New lookml_dashboard_id"] = None,
        query_id: Annotated[str | None, "New query_id"] = None,
        # ── Destinations ─────────────────────────────────────────────
        recipients: Annotated[
            list[str] | None,
            "Email-only shorthand that replaces the destinations list",
        ] = None,
        email_format: Annotated[
            str,
            (
                "Format for the ``recipients`` shorthand's email destinations. "
                "Looker requires every destination to specify a format. Defaults "
                "to ``wysiwyg_pdf``. See ``create_schedule`` for the full list "
                "of supported values."
            ),
        ] = "wysiwyg_pdf",
        destinations: Annotated[
            list[dict[str, Any]] | None,
            (
                "Full ``ScheduledPlanDestination`` array that replaces the "
                "existing destinations list. See ``create_schedule`` for the "
                "accepted shape. Pass an empty list to clear all destinations."
            ),
        ] = None,
        # ── Ownership ────────────────────────────────────────────────
        user_id: Annotated[str | None, "Reassign owner user id"] = None,
        run_as_recipient: Annotated[
            bool | None,
            "Run the schedule impersonating each recipient (email destinations only)",
        ] = None,
        # ── Trigger options ──────────────────────────────────────────
        enabled: Annotated[bool | None, "Enable or disable the schedule"] = None,
        run_once: Annotated[bool | None, "Toggle run-once mode"] = None,
        datagroup: Annotated[
            str | None,
            "Datagroup name (mutually exclusive with crontab)",
        ] = None,
        timezone: Annotated[str | None, "New crontab timezone"] = None,
        # ── Conditional delivery ─────────────────────────────────────
        require_results: Annotated[
            bool | None,
            "Toggle delivery-only-when-results condition",
        ] = None,
        require_no_results: Annotated[
            bool | None,
            "Toggle delivery-only-when-no-results condition",
        ] = None,
        require_change: Annotated[
            bool | None,
            "Toggle delivery-only-when-results-changed condition",
        ] = None,
        send_all_results: Annotated[
            bool | None,
            "Toggle unlimited-result mode (no row caps)",
        ] = None,
        # ── Filters ──────────────────────────────────────────────────
        filters_string: Annotated[str | None, "New filters_string"] = None,
        # ── Render / PDF options ─────────────────────────────────────
        include_links: Annotated[bool | None, "Include links in the scheduled content"] = None,
        pdf_paper_size: Annotated[str | None, "New PDF paper size"] = None,
        pdf_landscape: Annotated[bool | None, "Toggle PDF landscape orientation"] = None,
        long_tables: Annotated[bool | None, "Toggle long-table rendering"] = None,
        inline_table_width: Annotated[int | None, "New inline-table pixel width"] = None,
        color_theme: Annotated[str | None, "New dashboard color theme"] = None,
        embed: Annotated[bool | None, "Toggle embed-context flag"] = None,
        # ── Branded / custom URLs ────────────────────────────────────
        show_custom_url: Annotated[bool | None, "Toggle custom-link visibility"] = None,
        custom_url_base: Annotated[str | None, "New custom URL base"] = None,
        custom_url_params: Annotated[str | None, "New custom URL params"] = None,
        custom_url_label: Annotated[str | None, "New custom URL label"] = None,
    ) -> str:
        ctx = client.build_context("update_schedule", "admin", {"schedule_id": schedule_id})
        try:
            if recipients is not None and destinations is not None:
                return json.dumps(
                    {
                        "error": "Pass either ``recipients`` or ``destinations``, not both.",
                        "hint": (
                            "``recipients`` is a shorthand that builds an email-only "
                            "destinations array — pass it OR ``destinations``."
                        ),
                    },
                    indent=2,
                )
            # crontab and datagroup are mutually exclusive trigger modes per
            # the WriteScheduledPlan spec. On update either one can clear the
            # other (the new value overrides), so reject only the case where
            # both are explicitly provided in the same call.
            if crontab is not None and datagroup is not None:
                return json.dumps(
                    {
                        "error": (
                            "``crontab`` and ``datagroup`` are mutually exclusive "
                            "trigger modes — pass exactly one."
                        ),
                        "hint": (
                            "Use ``crontab`` for time-based delivery, ``datagroup`` "
                            "for delivery on data refresh."
                        ),
                    },
                    indent=2,
                )
            # Mirror create_schedule's at-most-one-target guard. A schedule
            # has exactly one source (look / dashboard / lookml_dashboard /
            # query) — retargeting to multiple at once is ambiguous and
            # would push an avoidable validation error to Looker.
            update_target_count = sum(
                1 for v in (look_id, dashboard_id, lookml_dashboard_id, query_id) if v is not None
            )
            if update_target_count > 1:
                return json.dumps(
                    {
                        "error": "Multiple targets provided.",
                        "hint": (
                            "Pass at most one of look_id, dashboard_id, "
                            "lookml_dashboard_id, query_id when retargeting an "
                            "existing schedule."
                        ),
                    },
                    indent=2,
                )
            # Looker requires every ScheduledPlan to have at least one
            # destination — an empty array is rejected with a 422. Catch
            # this preflight and return an actionable error.
            if destinations == [] or (recipients == [] and destinations is None):
                return json.dumps(
                    {
                        "error": (
                            "Empty destination list is not allowed — "
                            "Looker requires every ScheduledPlan to have "
                            "at least one destination."
                        ),
                        "hint": (
                            "To replace destinations, pass a non-empty "
                            "list. To leave existing destinations "
                            "unchanged, omit both ``destinations`` and "
                            "``recipients`` from the call."
                        ),
                    },
                    indent=2,
                )

            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "name", name)
                _set_if(body, "crontab", crontab)
                _set_if(body, "look_id", look_id)
                _set_if(body, "dashboard_id", dashboard_id)
                _set_if(body, "lookml_dashboard_id", lookml_dashboard_id)
                _set_if(body, "query_id", query_id)
                _set_if(body, "user_id", user_id)
                _set_if(body, "run_as_recipient", run_as_recipient)
                _set_if(body, "enabled", enabled)
                _set_if(body, "run_once", run_once)
                _set_if(body, "datagroup", datagroup)
                _set_if(body, "timezone", timezone)
                _set_if(body, "require_results", require_results)
                _set_if(body, "require_no_results", require_no_results)
                _set_if(body, "require_change", require_change)
                _set_if(body, "send_all_results", send_all_results)
                _set_if(body, "filters_string", filters_string)
                _set_if(body, "include_links", include_links)
                _set_if(body, "pdf_paper_size", pdf_paper_size)
                _set_if(body, "pdf_landscape", pdf_landscape)
                _set_if(body, "long_tables", long_tables)
                _set_if(body, "inline_table_width", inline_table_width)
                _set_if(body, "color_theme", color_theme)
                _set_if(body, "embed", embed)
                _set_if(body, "show_custom_url", show_custom_url)
                _set_if(body, "custom_url_base", custom_url_base)
                _set_if(body, "custom_url_params", custom_url_params)
                _set_if(body, "custom_url_label", custom_url_label)

                # destinations / recipients use `is not None` (not
                # `_set_if`) so a non-empty list explicitly replaces
                # existing destinations. Omitting both leaves them
                # untouched. (Empty-list rejection happens preflight
                # because Looker rejects empty arrays.)
                if destinations is not None:
                    body["scheduled_plan_destination"] = destinations
                elif recipients is not None:
                    body["scheduled_plan_destination"] = [
                        {"type": "email", "address": addr, "format": email_format}
                        for addr in recipients
                    ]

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one writable WriteScheduledPlan field. "
                                "See the tool description for the full list."
                            ),
                        },
                        indent=2,
                    )

                plan = await session.patch(f"/scheduled_plans/{_path_seg(schedule_id)}", body=body)
                return json.dumps(
                    {
                        "id": plan.get("id") if plan else schedule_id,
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_schedule", e)

    @server.tool(
        description=(
            "Trigger a scheduled plan to run once, immediately, outside its "
            "normal cron schedule. Useful to deliver a fresh copy after "
            "fixing a data issue, or to smoke-test a newly-created schedule."
        ),
    )
    async def run_schedule_once(
        schedule_id: Annotated[str, "Scheduled plan ID to trigger"],
    ) -> str:
        ctx = client.build_context("run_schedule_once", "admin", {"schedule_id": schedule_id})
        try:
            async with client.session(ctx) as session:
                result = await session.post(f"/scheduled_plans/{_path_seg(schedule_id)}/run_once")
                return json.dumps(
                    {
                        "schedule_id": schedule_id,
                        "triggered": True,
                        "id": result.get("id") if result else None,
                        "next_step": (
                            "Poll the system__activity scheduled_plan explore (via "
                            "get_schedule_history in the audit group) to watch for "
                            "completion and status."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("run_schedule_once", e)

    # ── Role membership readers ──────────────────────────────────────

    @server.tool(
        description=(
            "List the groups currently assigned to a role. Complements "
            "``set_role_groups`` (which replaces the full set). Returns "
            "``id`` and ``name`` for each assigned group."
        ),
    )
    async def get_role_groups(
        role_id: Annotated[str, "Role ID"],
    ) -> str:
        ctx = client.build_context("get_role_groups", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                groups_list = await session.get(f"/roles/{_path_seg(role_id)}/groups")
                result = [{"id": g.get("id"), "name": g.get("name")} for g in (groups_list or [])]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_role_groups", e)

    @server.tool(
        description=(
            "List the users assigned to a role directly (not via group "
            "membership). Complements ``set_role_users`` (which replaces "
            "the full set). Note: this does NOT include users who inherit "
            "the role via a group — to find those, combine "
            "``get_role_groups`` with each group's members, or check a "
            "specific user's effective role set via ``get_user_roles``."
        ),
    )
    async def get_role_users(
        role_id: Annotated[str, "Role ID"],
    ) -> str:
        ctx = client.build_context("get_role_users", "admin", {"role_id": role_id})
        try:
            async with client.session(ctx) as session:
                users_list = await session.get(f"/roles/{_path_seg(role_id)}/users")
                result = [
                    {
                        "id": u.get("id"),
                        "email": u.get("email"),
                        "first_name": u.get("first_name"),
                        "last_name": u.get("last_name"),
                        "is_disabled": u.get("is_disabled"),
                    }
                    for u in (users_list or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_role_users", e)
