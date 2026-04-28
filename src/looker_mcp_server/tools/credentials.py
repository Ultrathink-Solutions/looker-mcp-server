"""Credentials tool group — non-email user credentials.

Covers the credential types that authenticate a Looker user to the
instance: API3 key pairs (for service accounts and programmatic access),
TOTP (two-factor codes), and the four common SSO identity types
(LDAP, SAML, OIDC, Google).

Email/password credentials live in the ``admin`` group next to
``create_user`` — see ``create_credentials_email``,
``update_credentials_email``, and ``send_password_reset``.

Admin-only surface; disabled by default. Enable with
``--groups credentials`` (or ``all``).
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg, _set_if


def register_credentials_tools(server: FastMCP, client: LookerClient) -> None:
    # ── API3 (multi-instance, full lifecycle) ────────────────────────

    @server.tool(
        description=(
            "List all API3 credential pairs attached to a user. Returns one "
            "entry per key pair with its ``id`` and ``client_id`` (never the "
            "secret — Looker only surfaces that once, at creation). Use this "
            "to audit which keys exist before rotating."
        ),
    )
    async def list_credentials_api3(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("list_credentials_api3", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_api3")
                result = [
                    {
                        "id": c.get("id"),
                        "client_id": c.get("client_id"),
                        "created_at": c.get("created_at"),
                        "is_disabled": c.get("is_disabled"),
                    }
                    for c in (creds or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_credentials_api3", e)

    @server.tool(
        description=(
            "Generate a new API3 client_id/client_secret pair for a user. "
            "IMPORTANT: the response is the only place Looker surfaces the "
            "``client_secret`` — store it immediately. Use this to rotate "
            "service-account credentials: create a new pair, deploy it to "
            "consumers, then delete the old pair."
        ),
    )
    async def create_credentials_api3(
        user_id: Annotated[str, "User ID to attach the new key pair to"],
    ) -> str:
        ctx = client.build_context("create_credentials_api3", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.post(f"/users/{_path_seg(user_id)}/credentials_api3")
                return json.dumps(
                    {
                        "id": creds.get("id") if creds else None,
                        "client_id": creds.get("client_id") if creds else None,
                        "client_secret": creds.get("client_secret") if creds else None,
                        "created": True,
                        "warning": (
                            "The client_secret is only returned once. Store it "
                            "now or delete these credentials and create a new "
                            "pair."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_credentials_api3", e)

    @server.tool(
        description=(
            "Get metadata for a specific API3 credential pair. The "
            "``client_secret`` is never returned after creation — use this "
            "only to read ``client_id``, timestamps, and enabled state."
        ),
    )
    async def get_credentials_api3(
        user_id: Annotated[str, "User ID"],
        credentials_api3_id: Annotated[str, "API3 credential pair ID (from list_credentials_api3)"],
    ) -> str:
        ctx = client.build_context(
            "get_credentials_api3",
            "credentials",
            {"user_id": user_id, "credentials_api3_id": credentials_api3_id},
        )
        try:
            async with client.session(ctx) as session:
                creds = await session.get(
                    f"/users/{_path_seg(user_id)}/credentials_api3/{_path_seg(credentials_api3_id)}"
                )
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_api3", e)

    @server.tool(
        description=(
            "Update metadata for an API3 credential pair. Currently the only "
            "settable field is ``purpose`` — a free-form description used to "
            "remember what an API key is for (e.g. 'CI/CD pipeline', "
            "'data warehouse sync'). Useful when auditing keys: "
            "``list_credentials_api3`` shows ``client_id`` but not purpose, "
            "so without this metadata the only way to identify what a key "
            "does is to test it."
        ),
    )
    async def update_credentials_api3(
        user_id: Annotated[str, "User ID"],
        credentials_api3_id: Annotated[str, "API3 credential pair ID"],
        purpose: Annotated[
            str | None,
            "Free-form description of what this credential pair is used for",
        ] = None,
    ) -> str:
        ctx = client.build_context(
            "update_credentials_api3",
            "credentials",
            {"user_id": user_id, "credentials_api3_id": credentials_api3_id},
        )
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                _set_if(body, "purpose", purpose)

                if not body:
                    return json.dumps(
                        {
                            "error": "No fields provided to update.",
                            "hint": "Pass ``purpose``.",
                        },
                        indent=2,
                    )

                creds = await session.patch(
                    f"/users/{_path_seg(user_id)}/credentials_api3/"
                    f"{_path_seg(credentials_api3_id)}",
                    body=body,
                )
                return json.dumps(
                    {
                        "id": creds.get("id") if creds else credentials_api3_id,
                        "client_id": creds.get("client_id") if creds else None,
                        "purpose": creds.get("purpose") if creds else purpose,
                        "updated": True,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("update_credentials_api3", e)

    @server.tool(
        description=(
            "Delete an API3 credential pair. Any integration using the pair's "
            "``client_id``/``client_secret`` will stop being able to "
            "authenticate immediately. This action cannot be undone."
        ),
    )
    async def delete_credentials_api3(
        user_id: Annotated[str, "User ID"],
        credentials_api3_id: Annotated[str, "API3 credential pair ID"],
    ) -> str:
        ctx = client.build_context(
            "delete_credentials_api3",
            "credentials",
            {"user_id": user_id, "credentials_api3_id": credentials_api3_id},
        )
        try:
            async with client.session(ctx) as session:
                await session.delete(
                    f"/users/{_path_seg(user_id)}/credentials_api3/{_path_seg(credentials_api3_id)}"
                )
                return json.dumps(
                    {
                        "deleted": True,
                        "user_id": user_id,
                        "credentials_api3_id": credentials_api3_id,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_credentials_api3", e)

    # ── TOTP (two-factor) ────────────────────────────────────────────

    @server.tool(
        description=(
            "Get a user's TOTP (two-factor) credential metadata. Returns "
            "creation timestamp, ``verified`` (whether the user has confirmed "
            "their authenticator app), and ``is_disabled``. Returns a 404-"
            "shaped error if the user has no TOTP enrolled."
        ),
    )
    async def get_credentials_totp(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_totp", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_totp")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_totp", e)

    @server.tool(
        description=(
            "Enroll a user in TOTP (two-factor authentication). After "
            "creation the user must scan the secret with an authenticator "
            "app and confirm a code on next sign-in to move ``verified`` to "
            "true. Returns a 4xx if the user already has TOTP enrolled — "
            "delete the existing credential first if rotating."
        ),
    )
    async def create_credentials_totp(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("create_credentials_totp", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.post(f"/users/{_path_seg(user_id)}/credentials_totp")
                return json.dumps(
                    {
                        "user_id": user_id,
                        "created": True,
                        "verified": creds.get("verified") if creds else False,
                        "next_step": (
                            "User must complete enrollment in their "
                            "authenticator app on next sign-in."
                        ),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("create_credentials_totp", e)

    @server.tool(
        description=(
            "Remove a user's TOTP credential. Use to reset a user whose "
            "authenticator was lost or to allow re-enrollment with a new "
            "device. After deletion the user can sign in with just their "
            "primary credential until TOTP is re-created."
        ),
    )
    async def delete_credentials_totp(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_totp", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_totp")
                return json.dumps(
                    {"deleted": True, "user_id": user_id, "credential_type": "totp"},
                    indent=2,
                )
        except Exception as e:
            return format_api_error("delete_credentials_totp", e)

    # ── LDAP ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get a user's LDAP credential link. Returns the LDAP DN or email "
            "the user is bound to, plus its enabled state. Returns a 404-shaped "
            "error if no LDAP link exists."
        ),
    )
    async def get_credentials_ldap(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_ldap", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_ldap")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_ldap", e)

    @server.tool(
        description=(
            "Remove a user's LDAP credential link. They will no longer be able "
            "to authenticate via LDAP until the link is re-established (which "
            "typically happens automatically on their next LDAP sign-in)."
        ),
    )
    async def delete_credentials_ldap(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_ldap", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_ldap")
                return json.dumps({"deleted": True, "user_id": user_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_credentials_ldap", e)

    # ── SAML ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get a user's SAML credential link. Returns the SAML subject/email and enabled state."
        ),
    )
    async def get_credentials_saml(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_saml", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_saml")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_saml", e)

    @server.tool(
        description=(
            "Remove a user's SAML credential link. Re-established automatically "
            "on the user's next successful SAML sign-in."
        ),
    )
    async def delete_credentials_saml(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_saml", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_saml")
                return json.dumps({"deleted": True, "user_id": user_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_credentials_saml", e)

    # ── OIDC ─────────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get a user's OIDC credential link. Returns the OIDC subject and enabled state."
        ),
    )
    async def get_credentials_oidc(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_oidc", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_oidc")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_oidc", e)

    @server.tool(
        description=(
            "Remove a user's OIDC credential link. Re-established automatically "
            "on the user's next successful OIDC sign-in."
        ),
    )
    async def delete_credentials_oidc(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_oidc", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_oidc")
                return json.dumps({"deleted": True, "user_id": user_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_credentials_oidc", e)

    # ── Google ───────────────────────────────────────────────────────

    @server.tool(
        description=(
            "Get a user's Google (OAuth) credential link. Returns the linked "
            "Google email and enabled state."
        ),
    )
    async def get_credentials_google(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("get_credentials_google", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                creds = await session.get(f"/users/{_path_seg(user_id)}/credentials_google")
                return json.dumps(creds, indent=2)
        except Exception as e:
            return format_api_error("get_credentials_google", e)

    @server.tool(
        description=(
            "Remove a user's Google (OAuth) credential link. Re-established "
            "automatically when the user next signs in via Google."
        ),
    )
    async def delete_credentials_google(
        user_id: Annotated[str, "User ID"],
    ) -> str:
        ctx = client.build_context("delete_credentials_google", "credentials", {"user_id": user_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/users/{_path_seg(user_id)}/credentials_google")
                return json.dumps({"deleted": True, "user_id": user_id}, indent=2)
        except Exception as e:
            return format_api_error("delete_credentials_google", e)
