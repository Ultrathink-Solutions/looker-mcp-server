"""Shared pytest configuration.

``LOOKER_MCP_TEST_CLIENT_MODE`` sets the protocol era that every
``fastmcp.Client`` in the suite negotiates when a test does not pin one.
FastMCP 4's default, ``mode="auto"``, speaks 2026-07-28 to this server, so a
plain run covers the modern era. CI runs the suite a second time with
``LOOKER_MCP_TEST_CLIENT_MODE=legacy`` so every tool test also runs over the
2025-11-25 initialize handshake. Tests that pin ``mode=`` keep their own.
"""

from __future__ import annotations

import functools
import os
from typing import Any

import pytest
from fastmcp import Client

_CLIENT_MODE = os.environ.get("LOOKER_MCP_TEST_CLIENT_MODE") or None


@pytest.fixture(autouse=True)
def _default_client_protocol_era(monkeypatch: pytest.MonkeyPatch) -> None:
    if _CLIENT_MODE is None:
        return
    original = Client.__init__

    @functools.wraps(original)
    def __init__(self: Client, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("mode", _CLIENT_MODE)
        original(self, *args, **kwargs)

    monkeypatch.setattr(Client, "__init__", __init__)
