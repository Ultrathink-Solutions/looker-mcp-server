"""Admin tool group — user, role, group, and schedule management.

These tools require admin-level permissions in Looker and are disabled
by default.  Enable with ``--groups admin`` or ``LOOKER_GROUPS=admin``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


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

    @server.tool(description="Create a new user in Looker.")
    async def create_user(
        first_name: Annotated[str, "First name"],
        last_name: Annotated[str, "Last name"],
        email: Annotated[str, "Email address"],
        role_ids: Annotated[list[int] | None, "Role IDs to assign"] = None,
        group_ids: Annotated[list[int] | None, "Group IDs to add the user to"] = None,
    ) -> str:
        ctx = client.build_context("create_user", "admin")
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                }
                if role_ids:
                    body["role_ids"] = role_ids
                if group_ids:
                    body["group_ids"] = group_ids
                user = await session.post("/users", body=body)
                return json.dumps({"id": user.get("id"), "email": user.get("email")}, indent=2)
        except Exception as e:
            return format_api_error("create_user", e)

    @server.tool(description="Update a user's information.")
    async def update_user(
        user_id: Annotated[str, "User ID to update"],
        first_name: Annotated[str | None, "New first name"] = None,
        last_name: Annotated[str | None, "New last name"] = None,
        is_disabled: Annotated[bool | None, "Disable or enable the user"] = None,
        role_ids: Annotated[list[int] | None, "New role IDs"] = None,
    ) -> str:
        ctx = client.build_context("update_user", "admin", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if first_name is not None:
                    body["first_name"] = first_name
                if last_name is not None:
                    body["last_name"] = last_name
                if is_disabled is not None:
                    body["is_disabled"] = is_disabled
                if role_ids is not None:
                    body["role_ids"] = role_ids
                user = await session.patch(f"/users/{user_id}", body=body)
                return json.dumps({"id": user.get("id"), "updated": True}, indent=2)
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
    ) -> str:
        ctx = client.build_context("create_group", "admin")
        try:
            async with client.session(ctx) as session:
                group = await session.post("/groups", body={"name": name})
                return json.dumps(
                    {"id": group.get("id"), "name": group.get("name")},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_group", e)

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
                await session.delete(f"/scheduled_plans/{schedule_id}")
                return json.dumps({"deleted": True, "schedule_id": schedule_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_schedule", e)
