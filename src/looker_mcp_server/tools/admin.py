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
    ) -> str:
        ctx = client.build_context("create_credentials_email", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.post(
                    f"/users/{user_id}/credentials_email",
                    body={"email": email},
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
                    f"/users/{user_id}/credentials_email/send_password_reset",
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
            "Create a scheduled delivery plan for a Look or dashboard. "
            "Supports email, webhook, S3, SFTP, and other destinations."
        ),
    )
    async def create_schedule(
        name: Annotated[str, "Schedule name"],
        crontab: Annotated[str, "Cron expression (e.g. '0 9 * * 1' for Mondays at 9am)"],
        look_id: Annotated[str | None, "Look ID to schedule"] = None,
        dashboard_id: Annotated[str | None, "Dashboard ID to schedule"] = None,
        recipients: Annotated[list[str] | None, "Email recipients"] = None,
    ) -> str:
        ctx = client.build_context("create_schedule", "admin")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "crontab": crontab}
                if look_id:
                    body["look_id"] = look_id
                if dashboard_id:
                    body["dashboard_id"] = dashboard_id
                if recipients:
                    body["scheduled_plan_destination"] = [
                        {"type": "email", "address": addr} for addr in recipients
                    ]
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
            "changed; omitted fields are preserved. Use to retarget the "
            "recipient, change the cron schedule, or toggle enabled state "
            "without rebuilding the plan. Returns an actionable error when "
            "no fields are supplied."
        ),
    )
    async def update_schedule(
        schedule_id: Annotated[str, "Scheduled plan ID to update"],
        name: Annotated[str | None, "New display name"] = None,
        crontab: Annotated[
            str | None, "New cron expression (e.g. '0 9 * * *' for 9am daily)"
        ] = None,
        enabled: Annotated[bool | None, "Enable or disable the schedule"] = None,
        run_as_recipient: Annotated[
            bool | None, "Run the schedule impersonating each recipient"
        ] = None,
        include_links: Annotated[bool | None, "Include links in the scheduled content"] = None,
    ) -> str:
        ctx = client.build_context("update_schedule", "admin", {"schedule_id": schedule_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "name", name)
                _set_if(body, "crontab", crontab)
                _set_if(body, "enabled", enabled)
                _set_if(body, "run_as_recipient", run_as_recipient)
                _set_if(body, "include_links", include_links)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one of: name, crontab, enabled, "
                                "run_as_recipient, include_links."
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
