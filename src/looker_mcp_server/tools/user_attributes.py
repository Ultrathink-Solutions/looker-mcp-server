"""User-attributes tool group — per-user/per-group data entitlements.

Looker user attributes are named values that can hold different data for
each user or group (e.g. a ``region`` attribute that resolves to ``EMEA``
for one group and ``NA`` for another). They power row-level security,
per-developer git credentials, and runtime filter defaults in LookML.

Admin-only surface; disabled by default. Enable with
``--groups user_attributes`` or ``all``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg, _set_if


def register_user_attribute_tools(server: FastMCP, client: LookerClient) -> None:
    # ── Attribute definitions ─────────────────────────────────────────

    @server.tool(
        description=(
            "List all user-attribute definitions in the instance. Returns a "
            "trimmed summary (id, name, label, type, default_value) suitable "
            "for discovery; use ``get_user_attribute`` for the full definition."
        ),
    )
    async def list_user_attributes() -> str:
        ctx = client.build_context("list_user_attributes", "user_attributes")
        try:
            async with client.session(ctx) as session:
                attrs = await session.get("/user_attributes")
                result = [
                    {
                        "id": a.get("id"),
                        "name": a.get("name"),
                        "label": a.get("label"),
                        "type": a.get("type"),
                        "default_value": a.get("default_value"),
                        "value_is_hidden": a.get("value_is_hidden"),
                        "user_can_view": a.get("user_can_view"),
                        "user_can_edit": a.get("user_can_edit"),
                    }
                    for a in (attrs or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_user_attributes", e)

    @server.tool(
        description=(
            "Get the full definition of a single user attribute, including "
            "any domain whitelist for hidden values. Use ``list_user_attributes`` "
            "to discover attribute IDs."
        ),
    )
    async def get_user_attribute(
        user_attribute_id: Annotated[str, "User attribute ID"],
    ) -> str:
        ctx = client.build_context(
            "get_user_attribute",
            "user_attributes",
            {"user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                attr = await session.get(f"/user_attributes/{_path_seg(user_attribute_id)}")
                return json.dumps(attr, indent=2)
        except Exception as e:
            return format_api_error("get_user_attribute", e)

    @server.tool(
        description=(
            "Create a new user attribute. The ``type`` controls how LookML can "
            "use the value: 'string', 'number', 'datetime', 'yesno', 'zipcode', "
            "or one of the 'advanced_filter_*' types for filter defaults. "
            "Set ``value_is_hidden`` for secrets; combine with "
            "``hidden_value_domain_whitelist`` to restrict where hidden values "
            "can be sent (e.g. git credentials)."
        ),
    )
    async def create_user_attribute(
        name: Annotated[
            str, "Short machine name, referenced from LookML as _user_attributes['name']"
        ],
        label: Annotated[str, "Human-readable label"],
        type: Annotated[
            str,
            (
                "One of: string, number, datetime, yesno, zipcode, "
                "advanced_filter_string, advanced_filter_number, "
                "advanced_filter_datetime"
            ),
        ],
        default_value: Annotated[str | None, "Default value when no override is set"] = None,
        value_is_hidden: Annotated[
            bool | None, "Treat the value as a secret (obscured in UI + logs)"
        ] = None,
        user_can_view: Annotated[bool | None, "Whether users can see their own value"] = None,
        user_can_edit: Annotated[bool | None, "Whether users can edit their own value"] = None,
        hidden_value_domain_whitelist: Annotated[
            str | None,
            (
                "Semicolon-separated URL patterns that hidden values may be sent "
                "to. Required when value_is_hidden is true and the attribute is "
                "referenced in a liquid-templated URL."
            ),
        ] = None,
    ) -> str:
        ctx = client.build_context("create_user_attribute", "user_attributes", {"name": name})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {"name": name, "label": label, "type": type}
                _set_if(body, "default_value", default_value)
                _set_if(body, "value_is_hidden", value_is_hidden)
                _set_if(body, "user_can_view", user_can_view)
                _set_if(body, "user_can_edit", user_can_edit)
                _set_if(body, "hidden_value_domain_whitelist", hidden_value_domain_whitelist)

                attr = await session.post("/user_attributes", body=body)
                return json.dumps(
                    {
                        "id": attr.get("id") if attr else None,
                        "name": attr.get("name") if attr else name,
                        "created": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_user_attribute", e)

    @server.tool(
        description=(
            "Update a user-attribute definition. Only provided fields are "
            "changed; omitted fields are preserved. Note: the ``type`` cannot "
            "be changed after creation — create a new attribute instead."
        ),
    )
    async def update_user_attribute(
        user_attribute_id: Annotated[str, "User attribute ID"],
        label: Annotated[str | None, "New label"] = None,
        default_value: Annotated[str | None, "New default value"] = None,
        value_is_hidden: Annotated[bool | None, "Toggle hidden-value handling"] = None,
        user_can_view: Annotated[bool | None, "Toggle self-view"] = None,
        user_can_edit: Annotated[bool | None, "Toggle self-edit"] = None,
        hidden_value_domain_whitelist: Annotated[
            str | None, "New semicolon-separated URL whitelist"
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "update_user_attribute",
            "user_attributes",
            {"user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "label", label)
                _set_if(body, "default_value", default_value)
                _set_if(body, "value_is_hidden", value_is_hidden)
                _set_if(body, "user_can_view", user_can_view)
                _set_if(body, "user_can_edit", user_can_edit)
                _set_if(body, "hidden_value_domain_whitelist", hidden_value_domain_whitelist)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": (
                                "Pass at least one of: label, default_value, "
                                "value_is_hidden, user_can_view, user_can_edit, "
                                "hidden_value_domain_whitelist."
                            ),
                        },
                        indent=2,
                    )

                attr = await session.patch(
                    f"/user_attributes/{_path_seg(user_attribute_id)}", body=body
                )
                return json.dumps(
                    {
                        "id": attr.get("id") if attr else user_attribute_id,
                        "updated": True,
                        "fields_changed": sorted(body.keys()),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_user_attribute", e)

    @server.tool(
        description=(
            "Delete a user-attribute definition. Any LookML that references this "
            "attribute via ``_user_attributes['name']`` will stop resolving. "
            "Cannot be undone."
        ),
    )
    async def delete_user_attribute(
        user_attribute_id: Annotated[str, "User attribute ID"],
    ) -> str:
        ctx = client.build_context(
            "delete_user_attribute",
            "user_attributes",
            {"user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/user_attributes/{_path_seg(user_attribute_id)}")
                return json.dumps(
                    {"deleted": True, "user_attribute_id": user_attribute_id},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_user_attribute", e)

    # ── Per-group values ──────────────────────────────────────────────

    @server.tool(
        description=(
            "List the value of a user attribute for each group that has an "
            "override (groups without an override inherit the attribute's "
            "default). Returns ``{group_id, group_name, value, rank}`` for each "
            "override; lower rank wins when a user belongs to multiple groups."
        ),
    )
    async def list_user_attribute_group_values(
        user_attribute_id: Annotated[str, "User attribute ID"],
    ) -> str:
        ctx = client.build_context(
            "list_user_attribute_group_values",
            "user_attributes",
            {"user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                values = await session.get(
                    f"/user_attributes/{_path_seg(user_attribute_id)}/group_values"
                )
                result = [
                    {
                        "group_id": v.get("group_id"),
                        "group_name": v.get("group_name"),
                        "value": v.get("value"),
                        "rank": v.get("rank"),
                        "value_is_hidden": v.get("value_is_hidden"),
                    }
                    for v in (values or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_user_attribute_group_values", e)

    @server.tool(
        description=(
            "Replace the set of group overrides for a user attribute. The "
            "provided list becomes the complete set — groups not included lose "
            "their override and fall back to the attribute default. Use "
            "``list_user_attribute_group_values`` first if you want to preserve "
            "existing overrides."
        ),
    )
    async def set_user_attribute_group_values(
        user_attribute_id: Annotated[str, "User attribute ID"],
        values: Annotated[
            list[dict[str, Any]],
            (
                "List of ``{group_id, value, rank?}``. ``rank`` is optional; "
                "when omitted, Looker assigns ranks by list order."
            ),
        ],
    ) -> str:
        ctx = client.build_context(
            "set_user_attribute_group_values",
            "user_attributes",
            {"user_attribute_id": user_attribute_id, "count": len(values)},
        )
        try:
            async with client.session(ctx) as session:
                updated = await session.post(
                    f"/user_attributes/{_path_seg(user_attribute_id)}/group_values",
                    body=values,
                )
                return json.dumps(
                    {
                        "user_attribute_id": user_attribute_id,
                        "override_count": len(updated or []),
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("set_user_attribute_group_values", e)

    @server.tool(
        description=(
            "Remove a single group's override for a user attribute. Members "
            "of the group will fall back to other group overrides (by rank) "
            "or the attribute's default value."
        ),
    )
    async def delete_user_attribute_group_value(
        group_id: Annotated[str, "Group ID whose override to remove"],
        user_attribute_id: Annotated[str, "User attribute ID"],
    ) -> str:
        ctx = client.build_context(
            "delete_user_attribute_group_value",
            "user_attributes",
            {"group_id": group_id, "user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(
                    f"/groups/{_path_seg(group_id)}/attribute_values/{_path_seg(user_attribute_id)}"
                )
                return json.dumps(
                    {
                        "deleted": True,
                        "group_id": group_id,
                        "user_attribute_id": user_attribute_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_user_attribute_group_value", e)

    # ── Per-user values ───────────────────────────────────────────────

    @server.tool(
        description=(
            "Get all user-attribute values for a single user, showing which "
            "source (user override, group, or default) resolved each value. "
            "Useful for debugging why a user sees specific LookML behavior."
        ),
    )
    async def list_user_attribute_values_for_user(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context(
            "list_user_attribute_values_for_user",
            "user_attributes",
            {"user_id": user_id},
        )
        try:
            async with client.session(ctx) as session:
                values = await session.get(f"/users/{_path_seg(user_id)}/attribute_values")
                result = [
                    {
                        "user_attribute_id": v.get("user_attribute_id"),
                        "name": v.get("name"),
                        "label": v.get("label"),
                        "value": v.get("value"),
                        "source": v.get("source"),
                        "hidden_value_domain_whitelist": v.get("hidden_value_domain_whitelist"),
                    }
                    for v in (values or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_user_attribute_values_for_user", e)

    @server.tool(
        description=(
            "Set an individual user's value for a user attribute (overrides "
            "any group value and the default). Use sparingly — prefer group "
            "values for per-team scoping."
        ),
    )
    async def set_user_attribute_user_value(
        user_id: Annotated[str, "User ID"],
        user_attribute_id: Annotated[str, "User attribute ID"],
        value: Annotated[str, "Value to assign for this user"],
    ) -> str:
        ctx = client.build_context(
            "set_user_attribute_user_value",
            "user_attributes",
            {"user_id": user_id, "user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                result = await session.patch(
                    f"/users/{_path_seg(user_id)}/attribute_values/{_path_seg(user_attribute_id)}",
                    body={"value": value},
                )
                return json.dumps(
                    {
                        "user_id": user_id,
                        "user_attribute_id": user_attribute_id,
                        "value": result.get("value") if result else value,
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("set_user_attribute_user_value", e)

    @server.tool(
        description=(
            "Remove a user's per-user override for a user attribute. The user "
            "will then resolve this attribute from their group values or the "
            "attribute default."
        ),
    )
    async def delete_user_attribute_user_value(
        user_id: Annotated[str, "User ID"],
        user_attribute_id: Annotated[str, "User attribute ID"],
    ) -> str:
        ctx = client.build_context(
            "delete_user_attribute_user_value",
            "user_attributes",
            {"user_id": user_id, "user_attribute_id": user_attribute_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(
                    f"/users/{_path_seg(user_id)}/attribute_values/{_path_seg(user_attribute_id)}"
                )
                return json.dumps(
                    {
                        "deleted": True,
                        "user_id": user_id,
                        "user_attribute_id": user_attribute_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_user_attribute_user_value", e)
