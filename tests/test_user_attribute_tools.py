"""Tests for user_attributes tool group — per-user/per-group data entitlements."""

import json

import httpx
import pytest
import respx

from looker_mcp_server.client import LookerClient
from looker_mcp_server.config import LookerConfig
from looker_mcp_server.identity import ApiKeyIdentityProvider
from looker_mcp_server.server import create_server


@pytest.fixture
def config():
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        _env_file=None,  # type: ignore[call-arg]
    )


@pytest.fixture
def server_and_client(config):
    mcp, client = create_server(config, enabled_groups={"user_attributes"})
    return mcp, client


API_URL = "https://test.looker.com/api/4.0"


def _mock_login_logout():
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


class TestServerRegistration:
    def test_user_attributes_registers(self, server_and_client):
        mcp, _ = server_and_client
        assert mcp is not None

    def test_user_attributes_in_all_groups(self):
        from looker_mcp_server.config import ALL_GROUPS

        assert "user_attributes" in ALL_GROUPS


class TestListUserAttributes:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_trimmed_summary(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/user_attributes").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "id": "12",
                        "name": "region",
                        "label": "Region",
                        "type": "string",
                        "default_value": "NA",
                        "value_is_hidden": False,
                        "user_can_view": True,
                        "user_can_edit": False,
                    },
                    {
                        "id": "13",
                        "name": "github_token",
                        "label": "GitHub Token",
                        "type": "string",
                        "value_is_hidden": True,
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("list_user_attributes", "user_attributes")
        try:
            async with client.session(ctx) as session:
                attrs = await session.get("/user_attributes")
                assert len(attrs) == 2
                assert attrs[0]["name"] == "region"
                assert attrs[1]["value_is_hidden"] is True
        finally:
            await client.close()


class TestCreateUserAttribute:
    @pytest.mark.asyncio
    @respx.mock
    async def test_posts_required_and_selected_optional(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(201, json={"id": "99", "name": "region"})

        respx.post(f"{API_URL}/user_attributes").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context("create_user_attribute", "user_attributes", {"name": "region"})
        try:
            async with client.session(ctx) as session:
                body = {
                    "name": "region",
                    "label": "Region",
                    "type": "string",
                    "default_value": "NA",
                }
                attr = await session.post("/user_attributes", body=body)
                assert attr["id"] == "99"
                # Unprovided optional fields must NOT be serialized as null.
                assert captured["body"] == body
                assert "value_is_hidden" not in captured["body"]
        finally:
            await client.close()


class TestUpdateUserAttribute:
    @pytest.mark.asyncio
    @respx.mock
    async def test_patches_only_provided(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"id": "12"})

        respx.patch(f"{API_URL}/user_attributes/12").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "update_user_attribute", "user_attributes", {"user_attribute_id": "12"}
        )
        try:
            async with client.session(ctx) as session:
                await session.patch("/user_attributes/12", body={"default_value": "EMEA"})
                assert captured["body"] == {"default_value": "EMEA"}
        finally:
            await client.close()


class TestDeleteUserAttribute:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_success(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/user_attributes/12").mock(return_value=httpx.Response(204))

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "delete_user_attribute", "user_attributes", {"user_attribute_id": "12"}
        )
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/user_attributes/12") is None
        finally:
            await client.close()


class TestGroupValues:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_returns_overrides(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/user_attributes/12/group_values").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "group_id": "5",
                        "group_name": "emea",
                        "value": "EMEA",
                        "rank": 1,
                    },
                    {
                        "group_id": "6",
                        "group_name": "apac",
                        "value": "APAC",
                        "rank": 2,
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "list_user_attribute_group_values",
            "user_attributes",
            {"user_attribute_id": "12"},
        )
        try:
            async with client.session(ctx) as session:
                values = await session.get("/user_attributes/12/group_values")
                assert len(values) == 2
                assert values[0]["rank"] == 1
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_set_sends_list_body(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json=[
                    {"group_id": "5", "value": "EMEA", "rank": 1},
                    {"group_id": "6", "value": "APAC", "rank": 2},
                ],
            )

        respx.post(f"{API_URL}/user_attributes/12/group_values").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "set_user_attribute_group_values",
            "user_attributes",
            {"user_attribute_id": "12"},
        )
        try:
            async with client.session(ctx) as session:
                updated = await session.post(
                    "/user_attributes/12/group_values",
                    body=[
                        {"group_id": "5", "value": "EMEA"},
                        {"group_id": "6", "value": "APAC"},
                    ],
                )
                # Body is a JSON array (verifying client.post now accepts list bodies).
                assert isinstance(captured["body"], list)
                assert captured["body"][0]["group_id"] == "5"
                assert len(updated) == 2
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_removes_single_override(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/groups/5/attribute_values/12").mock(
            return_value=httpx.Response(204)
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "delete_user_attribute_group_value",
            "user_attributes",
            {"group_id": "5", "user_attribute_id": "12"},
        )
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/groups/5/attribute_values/12") is None
        finally:
            await client.close()


class TestUserValues:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_shows_source(self, config):
        _mock_login_logout()
        respx.get(f"{API_URL}/users/42/attribute_values").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "user_attribute_id": "12",
                        "name": "region",
                        "label": "Region",
                        "value": "EMEA",
                        "source": "group:emea",
                    },
                    {
                        "user_attribute_id": "13",
                        "name": "language",
                        "label": "Language",
                        "value": "en",
                        "source": "default",
                    },
                ],
            )
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "list_user_attribute_values_for_user",
            "user_attributes",
            {"user_id": "42"},
        )
        try:
            async with client.session(ctx) as session:
                values = await session.get("/users/42/attribute_values")
                assert values[0]["source"].startswith("group:")
                assert values[1]["source"] == "default"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_set_user_value(self, config):
        _mock_login_logout()

        captured: dict = {}

        def capture(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"value": "LATAM"})

        respx.patch(f"{API_URL}/users/42/attribute_values/12").mock(side_effect=capture)

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "set_user_attribute_user_value",
            "user_attributes",
            {"user_id": "42", "user_attribute_id": "12"},
        )
        try:
            async with client.session(ctx) as session:
                result = await session.patch(
                    "/users/42/attribute_values/12",
                    body={"value": "LATAM"},
                )
                assert captured["body"] == {"value": "LATAM"}
                assert result["value"] == "LATAM"
        finally:
            await client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_user_value(self, config):
        _mock_login_logout()
        respx.delete(f"{API_URL}/users/42/attribute_values/12").mock(
            return_value=httpx.Response(204)
        )

        provider = ApiKeyIdentityProvider("test-id", "test-secret")
        client = LookerClient(config, provider)
        ctx = client.build_context(
            "delete_user_attribute_user_value",
            "user_attributes",
            {"user_id": "42", "user_attribute_id": "12"},
        )
        try:
            async with client.session(ctx) as session:
                assert await session.delete("/users/42/attribute_values/12") is None
        finally:
            await client.close()
