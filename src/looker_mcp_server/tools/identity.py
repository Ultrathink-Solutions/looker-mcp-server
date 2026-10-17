"""Identity tool group — current-user introspection.

Tools that answer "as which Looker user is this MCP session
authenticated?" Useful for confirming the active identity before
running operations that depend on user-specific state (dev workspace,
content access, license seat). When the configured admin credentials
are sudo-impersonating another user (via ``act_as_user`` per-call or
``X-User-Token`` header), ``whoami`` returns the impersonated user's
record because Looker resolves ``GET /user`` against the bearer token
the session is currently using.
"""

from __future__ import annotations

import json

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_identity_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "Return the Looker user this session acts as: ID, display name, email, "
            "role IDs and group IDs. Use it to confirm whose permissions and dev "
            "workspace apply before running work that depends on the user. When the "
            "session impersonates another user, it returns that user."
        ),
    )
    async def whoami() -> str:
        ctx = client.build_context("whoami", "identity")
        try:
            async with client.session(ctx) as session:
                user = await session.get("/user")
                # Field allow-list: don't pass through raw response. Looker
                # adds new fields over time (home_folder_id, OAuth provider
                # IDs, etc.) and a permissive default would surface them
                # without us deciding they're appropriate. Add to this list
                # deliberately when a new field becomes useful.
                return json.dumps(
                    {
                        "id": user.get("id"),
                        "display_name": user.get("display_name"),
                        "email": user.get("email"),
                        "first_name": user.get("first_name"),
                        "last_name": user.get("last_name"),
                        "role_ids": user.get("role_ids"),
                        "group_ids": user.get("group_ids"),
                        "verified_looker_employee": user.get("verified_looker_employee"),
                        "is_disabled": user.get("is_disabled"),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("whoami", e)
