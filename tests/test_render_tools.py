"""Tests for the render tool group — RenderTask wrapping for Looker.

Covers all four tools (``render_query``, ``render_look``,
``render_dashboard``, ``render_dashboard_tile``) and all three return
shapes (Image, File, timeout-/cap-escape-hatch JSON).
"""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest
import respx
from fastmcp import Client
from mcp.types import EmbeddedResource, ImageContent, TextContent

from looker_mcp_server.config import LookerConfig
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


API_URL = "https://test.looker.com/api/4.0"

# Minimal 1×1 PNG (valid header — enough for content-type sniffing /
# round-trip assertions; tests never decode the pixels).
PNG_BYTES = bytes.fromhex(
    "89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C4"
    "890000000A49444154789C6300010000000500010D0A2DB40000000049454E44AE426082"
)
PDF_BYTES = b"%PDF-1.4\n%test-fixture\n%%EOF"


def _mock_login_logout() -> None:
    respx.post(f"{API_URL}/login").mock(
        return_value=httpx.Response(200, json={"access_token": "sess-token"})
    )
    respx.delete(f"{API_URL}/logout").mock(return_value=httpx.Response(204))


def _mock_render_task_success(
    *,
    create_path: str,
    task_id: str,
    result_bytes: bytes,
    result_content_type: str,
    create_response_extra: dict[str, Any] | None = None,
) -> tuple[respx.Route, respx.Route, respx.Route]:
    """Wire the three-call lifecycle to a single happy-path render."""
    body = {"id": task_id, "status": "enqueued_for_query"}
    if create_response_extra:
        body.update(create_response_extra)
    create = respx.post(f"{API_URL}{create_path}").mock(return_value=httpx.Response(200, json=body))
    poll = respx.get(f"{API_URL}/render_tasks/{task_id}").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": task_id,
                "status": "success",
                "query_runtime": 0.12,
                "render_runtime": 0.34,
                "runtime": 0.46,
            },
        )
    )
    results = respx.get(f"{API_URL}/render_tasks/{task_id}/results").mock(
        return_value=httpx.Response(
            200,
            content=result_bytes,
            headers={"content-type": result_content_type},
        )
    )
    return create, poll, results


class TestRenderQuery:
    """``render_query`` posts /queries first, then drives a queries-render-task."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_query_then_renders_png(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/queries").mock(return_value=httpx.Response(201, json={"id": "q1"}))
        create, poll, results = _mock_render_task_success(
            create_path="/render_tasks/queries/q1/png",
            task_id="rt-1",
            result_bytes=PNG_BYTES,
            result_content_type="image/png",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_query",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region"],
                        "width": 800,
                        "height": 600,
                        "result_format": "png",
                    },
                )
                content = result.content[0]
                assert isinstance(content, ImageContent)
                assert content.mimeType == "image/png"
                assert base64.b64decode(content.data) == PNG_BYTES
        finally:
            await looker_client.close()

        assert create.called
        assert poll.called
        assert results.called
        # Width/height must reach Looker as query-string params on create.
        assert create.calls.last.request.url.params["width"] == "800"
        assert create.calls.last.request.url.params["height"] == "600"

    @pytest.mark.asyncio
    @respx.mock
    async def test_rejects_oversized_dimensions(self, config):
        _mock_login_logout()
        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_query",
                    {
                        "model": "ecommerce",
                        "view": "orders",
                        "fields": ["orders.region"],
                        "width": 5000,
                        "height": 5000,
                        "result_format": "png",
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert "pixel cap" in payload["error"]
        finally:
            await looker_client.close()


class TestRenderLook:
    """Saved-Look path: no query POST, direct create-task."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_renders_jpg(self, config):
        _mock_login_logout()
        create, poll, results = _mock_render_task_success(
            create_path="/render_tasks/looks/look-1/jpg",
            task_id="rt-2",
            result_bytes=PNG_BYTES,  # bytes opaque to FastMCP routing
            result_content_type="image/jpeg",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_look",
                    {
                        "look_id": "look-1",
                        "width": 400,
                        "height": 300,
                        "result_format": "jpg",
                    },
                )
                content = result.content[0]
                assert isinstance(content, ImageContent)
                # FastMCP's Image(format="jpg") emits the literal
                # "image/jpg" MIME — not "image/jpeg" — so the format
                # round-trips through the MCP envelope unchanged.
                assert content.mimeType == "image/jpg"
        finally:
            await looker_client.close()

        assert create.called
        assert poll.called
        assert results.called


class TestRenderDashboard:
    """Dashboard renders cover the full param surface including PDF output."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_pdf_returns_embedded_resource(self, config):
        _mock_login_logout()
        create, poll, results = _mock_render_task_success(
            create_path="/render_tasks/dashboards/42/pdf",
            task_id="rt-3",
            result_bytes=PDF_BYTES,
            result_content_type="application/pdf",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_dashboard",
                    {
                        "dashboard_id": "42",
                        "width": 1024,
                        "height": 768,
                        "result_format": "pdf",
                        "pdf_paper_size": "letter",
                        "pdf_landscape": True,
                        "long_tables": False,
                        "theme": "corporate",
                    },
                )
                content = result.content[0]
                # FastMCP serializes File(format='pdf') to an
                # EmbeddedResource with BlobResourceContents.
                assert isinstance(content, EmbeddedResource)
                blob_b64 = content.resource.blob  # type: ignore[union-attr]
                assert base64.b64decode(blob_b64) == PDF_BYTES
        finally:
            await looker_client.close()

        assert create.called
        params = create.calls.last.request.url.params
        assert params["width"] == "1024"
        assert params["height"] == "768"
        assert params["pdf_paper_size"] == "letter"
        assert params["pdf_landscape"] == "true"
        assert params["long_tables"] == "false"
        assert params["theme"] == "corporate"
        assert poll.called
        assert results.called

    @pytest.mark.asyncio
    @respx.mock
    async def test_dashboard_filters_url_encoded(self, config):
        _mock_login_logout()
        create, _poll, _results = _mock_render_task_success(
            create_path="/render_tasks/dashboards/42/png",
            task_id="rt-4",
            result_bytes=PNG_BYTES,
            result_content_type="image/png",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "render_dashboard",
                    {
                        "dashboard_id": "42",
                        "width": 800,
                        "height": 600,
                        "result_format": "png",
                        "dashboard_filters": {
                            "Region": "West",
                            "Date Range": "30 days",
                        },
                    },
                )
        finally:
            await looker_client.close()

        params = create.calls.last.request.url.params
        # urlencode preserves dict insertion order; both pairs round-trip
        # through httpx's URL parser without further escaping.
        assert "Region=West" in params["dashboard_filters"]
        assert "Date+Range=30+days" in params["dashboard_filters"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_pdf_only_params_omitted_for_png_render(self, config):
        """PDF-specific knobs (pdf_paper_size, pdf_landscape, long_tables)
        must not reach Looker on PNG/JPG renders — Looker has no use for
        page orientation or paper size on a raster. ``theme`` and
        ``dashboard_filters`` are format-agnostic and must still
        propagate. Pins the gating so future refactors can't regress it.
        """
        _mock_login_logout()
        create, _poll, _results = _mock_render_task_success(
            create_path="/render_tasks/dashboards/42/png",
            task_id="rt-png-gated",
            result_bytes=PNG_BYTES,
            result_content_type="image/png",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                await mcp_client.call_tool(
                    "render_dashboard",
                    {
                        "dashboard_id": "42",
                        "width": 800,
                        "height": 600,
                        "result_format": "png",
                        # All four are set but only the first should
                        # reach Looker for a PNG render.
                        "theme": "corporate",
                        "pdf_paper_size": "letter",
                        "pdf_landscape": True,
                        "long_tables": True,
                    },
                )
        finally:
            await looker_client.close()

        params = create.calls.last.request.url.params
        assert "pdf_paper_size" not in params
        assert "pdf_landscape" not in params
        assert "long_tables" not in params
        # theme is format-agnostic — must still reach Looker.
        assert params["theme"] == "corporate"


class TestRenderDashboardTile:
    """One-tile renders use the dashboard_elements endpoint."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_renders_png(self, config):
        _mock_login_logout()
        create, poll, results = _mock_render_task_success(
            create_path="/render_tasks/dashboard_elements/elem-1/png",
            task_id="rt-5",
            result_bytes=PNG_BYTES,
            result_content_type="image/png",
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_dashboard_tile",
                    {
                        "dashboard_element_id": "elem-1",
                        "width": 320,
                        "height": 240,
                    },
                )
                content = result.content[0]
                assert isinstance(content, ImageContent)
                assert content.mimeType == "image/png"
        finally:
            await looker_client.close()

        assert create.called
        assert poll.called
        assert results.called


class TestRenderLifecycle:
    """End-to-end behaviours that span the create→poll→fetch flow."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_failure_status_propagates_status_detail(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/render_tasks/looks/L/png").mock(
            return_value=httpx.Response(200, json={"id": "rt-fail", "status": "enqueued_for_query"})
        )
        respx.get(f"{API_URL}/render_tasks/rt-fail").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "rt-fail",
                    "status": "failure",
                    "status_detail": "Query timed out after 60s",
                },
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_look",
                    {"look_id": "L", "width": 400, "height": 300},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                # The Looker error body must reach the caller verbatim
                # so they can see WHY the render failed.
                assert "Query timed out after 60s" in json.dumps(payload)
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_timeout_returns_escape_hatch_json(self, config):
        _mock_login_logout()
        respx.post(f"{API_URL}/render_tasks/looks/L/png").mock(
            return_value=httpx.Response(200, json={"id": "rt-slow"})
        )
        # Status never reaches a terminal state within max_wait_seconds.
        respx.get(f"{API_URL}/render_tasks/rt-slow").mock(
            return_value=httpx.Response(200, json={"id": "rt-slow", "status": "rendering"})
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_look",
                    {
                        "look_id": "L",
                        "width": 400,
                        "height": 300,
                        # Zero seconds: first poll returns non-terminal,
                        # deadline immediately blown, escape hatch fires.
                        "max_wait_seconds": 0.0,
                    },
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["status"] == "timeout"
                assert payload["render_task_id"] == "rt-slow"
                assert payload["last_status"] == "rendering"
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_oversized_result_returns_too_large_escape_hatch(self, config):
        """A render result bigger than the 10 MB transport cap must not
        materialize in memory and must reach the caller as a JSON
        escape-hatch payload carrying ``render_task_id`` + ``size_bytes``.

        respx serves the full payload; ``get_bytes`` triggers its
        Content-Length fast path because httpx auto-sets the header from
        the byte length, so no body is actually buffered in the
        oversized branch. This pins the byte-budget invariant.
        """
        _mock_login_logout()
        respx.post(f"{API_URL}/render_tasks/looks/L/png").mock(
            return_value=httpx.Response(200, json={"id": "rt-big"})
        )
        respx.get(f"{API_URL}/render_tasks/rt-big").mock(
            return_value=httpx.Response(200, json={"id": "rt-big", "status": "success"})
        )
        # 10 MB + 1 — one byte over the cap is enough to exercise the
        # truncated path without bloating the test process. respx serves
        # the payload but the streaming helper never appends a chunk.
        oversized_size = 10 * 1024 * 1024 + 1
        respx.get(f"{API_URL}/render_tasks/rt-big/results").mock(
            return_value=httpx.Response(
                200,
                content=b"x" * oversized_size,
                headers={"content-type": "image/png"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_look",
                    {"look_id": "L", "width": 400, "height": 300, "result_format": "png"},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["status"] == "too_large"
                assert payload["render_task_id"] == "rt-big"
                assert payload["format"] == "png"
                # Content-Length fast path → exact size signalled.
                assert payload["size_bytes"] == oversized_size
        finally:
            await looker_client.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_oversized_result_streaming_fallback_escape_hatch(self, config):
        """When the server omits Content-Length, the 10 MB cap is enforced
        by the ``aiter_bytes()`` early-break in ``_request_bytes`` rather
        than the header fast path. Same too_large escape-hatch contract;
        only the lower-bound size signal differs (chunks read so far
        rather than exact Content-Length). Pins the streaming branch
        against regressions like a refactor reintroducing
        ``response.content`` buffering.
        """
        _mock_login_logout()
        respx.post(f"{API_URL}/render_tasks/looks/L/png").mock(
            return_value=httpx.Response(200, json={"id": "rt-big-stream"})
        )
        respx.get(f"{API_URL}/render_tasks/rt-big-stream").mock(
            return_value=httpx.Response(200, json={"id": "rt-big-stream", "status": "success"})
        )

        oversized_size = 10 * 1024 * 1024 + 1
        chunk_size = 1024 * 1024

        class _NoLengthStream(httpx.AsyncByteStream):
            """Yields oversized_size bytes without setting Content-Length.

            httpx auto-derives Content-Length when ``content=bytes`` is
            used, but not when a custom ``stream=`` is provided — so
            attaching this drives ``_request_bytes`` down the streaming
            fallback branch.
            """

            async def __aiter__(self):
                remaining = oversized_size
                while remaining > 0:
                    n = min(chunk_size, remaining)
                    yield b"x" * n
                    remaining -= n

            async def aclose(self) -> None:
                return

        respx.get(f"{API_URL}/render_tasks/rt-big-stream/results").mock(
            return_value=httpx.Response(
                200,
                stream=_NoLengthStream(),
                headers={"content-type": "image/png"},
            )
        )

        mcp, looker_client = create_server(config, enabled_groups={"render"})
        try:
            async with Client(mcp) as mcp_client:
                result = await mcp_client.call_tool(
                    "render_look",
                    {"look_id": "L", "width": 400, "height": 300, "result_format": "png"},
                )
                content = result.content[0]
                assert isinstance(content, TextContent)
                payload = json.loads(content.text)
                assert payload["status"] == "too_large"
                assert payload["render_task_id"] == "rt-big-stream"
                assert payload["format"] == "png"
                # Streaming fallback only knows the lower bound: the
                # loop breaks as soon as the running total passes the
                # 10 MB cap, so the surfaced size is somewhere in
                # (10 MB, 10 MB + chunk_size].
                assert payload["size_bytes"] > 10 * 1024 * 1024
        finally:
            await looker_client.close()
