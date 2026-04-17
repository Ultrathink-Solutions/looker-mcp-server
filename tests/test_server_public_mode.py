"""Tests for the ``LOOKER_MCP_MODE=public`` wiring in :mod:`looker_mcp_server.server`.

Covers:
- ``build_public_mode_middleware`` returns ``None`` in dev mode.
- ``build_public_mode_middleware`` returns a Starlette ``Middleware``
  wrapper in public mode, with the expected target class + init kwargs
  derived from config.
- ``create_server`` registers the PRM route only in public mode, and
  the route serves the expected RFC 9728 document shape.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastmcp import Client
from mcp.types import TextContent

from looker_mcp_server.config import LookerConfig, LookerMcpMode
from looker_mcp_server.oidc import PublicModeAuthMiddleware
from looker_mcp_server.server import (
    PRM_PATH,
    build_public_mode_middleware,
    create_server,
)

RESOURCE_URI = "https://looker.example.com/mcp"
ISSUER_URL = "https://as.example.com"
JWKS_URI = "https://as.example.com/.well-known/jwks.json"


def _public_config(**overrides: Any) -> LookerConfig:
    base: dict[str, Any] = {
        "base_url": "https://test.looker.com",
        "client_id": "test-id",
        "client_secret": "test-secret",
        "sudo_as_user": False,
        "mcp_mode": LookerMcpMode.PUBLIC,
        "mcp_jwks_uri": JWKS_URI,
        "mcp_issuer_url": ISSUER_URL,
        "mcp_resource_uri": RESOURCE_URI,
    }
    base.update(overrides)
    return LookerConfig(_env_file=None, **base)  # type: ignore[call-arg]


def _dev_config(**overrides: Any) -> LookerConfig:
    base: dict[str, Any] = {
        "base_url": "https://test.looker.com",
        "client_id": "test-id",
        "client_secret": "test-secret",
        "sudo_as_user": False,
        "mcp_mode": LookerMcpMode.DEV,
    }
    base.update(overrides)
    return LookerConfig(_env_file=None, **base)  # type: ignore[call-arg]


class TestBuildPublicModeMiddleware:
    def test_dev_mode_returns_none(self):
        """Dev-mode deployments get no auth gate — the HTTP transport
        stays permissive so local iteration works with no OIDC setup."""
        assert build_public_mode_middleware(_dev_config()) is None

    def test_public_mode_returns_wrapped_auth_middleware(self):
        """Public mode returns a Starlette ``Middleware`` whose target
        is ``PublicModeAuthMiddleware`` pre-bound with the right realm
        and PRM URL derived from config."""
        mw = build_public_mode_middleware(_public_config())
        assert mw is not None
        # Starlette's Middleware wrapper carries the target class + kwargs.
        assert mw.cls is PublicModeAuthMiddleware
        # `.kwargs` holds the positional/keyword options that will be
        # passed to ``cls(app, **kwargs)`` when the ASGI stack is built.
        assert mw.kwargs["realm"] == RESOURCE_URI
        # RFC 9728 §3: the advertised PRM URL inserts the well-known
        # prefix between the authority and the resource identifier's
        # path. For ``https://looker.example.com/mcp`` the canonical
        # URL is ``https://looker.example.com/.well-known/oauth-
        # protected-resource/mcp``.
        assert mw.kwargs["prm_url"] == f"https://looker.example.com{PRM_PATH}/mcp"
        # The OAuth21ResourceServer instance is pre-built; the kid-miss
        # throttle / JWKS fetch only fires on first token, so construction
        # is cheap even when the AS is unreachable at startup.
        assert mw.kwargs["resource_server"] is not None

    def test_public_mode_prm_url_preserves_nested_resource_path(self):
        """RFC 9728 §3: multi-segment resource paths must round-trip
        intact as the well-known URL's suffix."""
        mw = build_public_mode_middleware(
            _public_config(mcp_resource_uri="https://looker.example.com/mcp/nested")
        )
        assert mw is not None
        assert mw.kwargs["prm_url"] == f"https://looker.example.com{PRM_PATH}/mcp/nested"

    def test_public_mode_prm_url_collapses_bare_origin(self):
        """Origin-only resource identifiers (with or without a trailing
        slash) must reduce to bare ``PRM_PATH`` — no ``/`` artifact in
        the advertised URL."""
        for origin in ("https://looker.example.com", "https://looker.example.com/"):
            mw = build_public_mode_middleware(_public_config(mcp_resource_uri=origin))
            assert mw is not None, origin
            assert mw.kwargs["prm_url"] == f"https://looker.example.com{PRM_PATH}", origin


class TestPrmRouteRegistration:
    @pytest.mark.asyncio
    async def test_dev_mode_does_not_register_prm(self):
        """PRM discovery is a public-mode affordance; dev deployments
        neither need nor advertise it."""
        mcp, client = create_server(_dev_config())
        try:
            async with Client(mcp) as mcp_client:
                # FastMCP exposes registered custom routes via the internal
                # app's route table. Easiest assertion: call the route via
                # an HTTP transport and confirm 404. Here we inspect the
                # underlying Starlette app.
                app = mcp.http_app() if hasattr(mcp, "http_app") else None
                if app is not None:
                    route_paths = {getattr(r, "path", None) for r in getattr(app, "routes", [])}
                    assert PRM_PATH not in route_paths, "dev mode must not register the PRM route"
                # Regardless of transport-level probe, the client should be
                # able to connect in stdio-equivalent in-process mode.
                await mcp_client.ping()
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_public_mode_registers_prm_root_and_suffix_variants(self):
        """Public mode registers BOTH the origin-rooted ``PRM_PATH``
        and — when the resource identifier has a path — the RFC 9728 §3
        suffix-variant path. The root stays available as a defensive
        fallback for clients probing the origin well-known location
        before following the ``WWW-Authenticate`` challenge hint; the
        suffix variant is the spec-canonical location."""
        mcp, client = create_server(_public_config())
        try:
            app = mcp.http_app() if hasattr(mcp, "http_app") else None
            assert app is not None, "FastMCP should expose an HTTP app"
            route_paths = {getattr(r, "path", None) for r in getattr(app, "routes", [])}
            assert PRM_PATH in route_paths, f"expected {PRM_PATH} in routes, got {route_paths}"
            # RESOURCE_URI = "https://looker.example.com/mcp" → suffix
            # variant path is PRM_PATH + "/mcp".
            suffix_path = f"{PRM_PATH}/mcp"
            assert suffix_path in route_paths, (
                f"expected {suffix_path} (RFC 9728 §3 suffix variant) in routes, got {route_paths}"
            )
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_public_mode_origin_only_resource_registers_only_root(self):
        """When the resource identifier has no path, the suffix-variant
        path collapses to ``PRM_PATH``, so the server registers only one
        route (no duplicate-registration error from FastMCP)."""
        mcp, client = create_server(_public_config(mcp_resource_uri="https://looker.example.com"))
        try:
            app = mcp.http_app() if hasattr(mcp, "http_app") else None
            assert app is not None
            route_paths = [getattr(r, "path", None) for r in getattr(app, "routes", [])]
            # Exactly one PRM route in the route table.
            assert route_paths.count(PRM_PATH) == 1, (
                f"expected exactly one {PRM_PATH} registration, got {route_paths}"
            )
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_prm_route_serves_rfc9728_document_at_both_paths(self):
        """Both the root and the suffix-variant paths serve the same
        RFC 9728 document. The document body always reports the
        configured resource identifier — serving location doesn't change
        the ``resource`` claim."""
        from starlette.applications import Starlette
        from starlette.testclient import TestClient

        mcp, client = create_server(_public_config())
        try:
            app = mcp.http_app()
            assert isinstance(app, Starlette)
            suffix_path = f"{PRM_PATH}/mcp"
            with TestClient(app) as http:
                for path in (PRM_PATH, suffix_path):
                    resp = http.get(path)
                    assert resp.status_code == 200, (
                        f"{path} should serve PRM doc; got {resp.status_code}"
                    )
                    assert resp.headers.get("cache-control", "").startswith("public"), (
                        f"{path} should carry a public cache-control"
                    )
                    doc = resp.json()
                    assert doc["resource"] == RESOURCE_URI, path
                    assert doc["authorization_servers"] == [ISSUER_URL], path
                    assert doc["bearer_methods_supported"] == ["header"], path
                    assert doc["resource_signing_alg_values_supported"] == [
                        "RS256",
                        "ES256",
                    ], path
        finally:
            await client.close()

    # Silence unused-import warnings when the module's imports are
    # legitimately needed by the ``Client`` / ``TextContent`` round-trips.
    _ = Client, TextContent, json
