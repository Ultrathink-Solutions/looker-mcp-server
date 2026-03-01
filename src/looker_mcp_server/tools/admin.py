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
