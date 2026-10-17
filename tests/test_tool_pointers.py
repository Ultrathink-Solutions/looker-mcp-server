"""Tool descriptions only point at tools a deployment of the read-only groups serves.

A description that tells the model to find an ID with another tool is only
useful when that tool is served. The read-only groups (the default groups
minus ``content``) are a common deployment, so a description in a default group
may name a tool in its own group or in a read-only group, and nothing else.
"""

import re

import pytest
from fastmcp import Client

from looker_mcp_server.config import ALL_GROUPS, DEFAULT_GROUPS, LookerConfig
from looker_mcp_server.server import create_server

READ_ONLY_GROUPS = DEFAULT_GROUPS - {"content"}


def _config() -> LookerConfig:
    # Developer arguments are on so their descriptions are checked too.
    return LookerConfig(
        base_url="https://test.looker.com",
        client_id="test-id",
        client_secret="test-secret",
        sudo_as_user=False,
        query_dev_params=True,
        _env_file=None,  # type: ignore[call-arg]
    )


async def _tools(group: str) -> list:
    mcp, looker_client = create_server(_config(), enabled_groups={group})
    try:
        async with Client(mcp) as mcp_client:
            return await mcp_client.list_tools()
    finally:
        await looker_client.close()


def _texts(tool) -> list[str]:
    properties = tool.inputSchema.get("properties") or {}
    return [tool.description or ""] + [p.get("description") or "" for p in properties.values()]


@pytest.mark.asyncio
async def test_default_group_descriptions_name_only_tools_the_read_only_groups_serve():
    owner: dict[str, str] = {}
    for group in sorted(ALL_GROUPS):
        for tool in await _tools(group):
            owner.setdefault(tool.name, group)
    # Longest names first, so a name is never matched as part of a longer one.
    alternatives = "|".join(re.escape(n) for n in sorted(owner, key=len, reverse=True))
    names = re.compile(rf"\b({alternatives})\b")

    unserved: list[str] = []
    for group in sorted(DEFAULT_GROUPS):
        for tool in await _tools(group):
            for text in _texts(tool):
                for named in names.findall(text):
                    target = owner[named]
                    if target != group and target not in READ_ONLY_GROUPS:
                        unserved.append(f"{tool.name} names {named} ({target} group)")

    assert not unserved, unserved
