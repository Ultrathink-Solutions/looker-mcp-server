"""Render tool group — async PNG/JPG/PDF rendering of Looker content.

Wraps Looker's ``/render_tasks/*`` API surface as four MCP tools:

* ``render_query``           — ad-hoc explore (model+view+fields → PNG/JPG)
* ``render_look``            — saved Look (PNG/JPG)
* ``render_dashboard``       — full dashboard (PDF/PNG/JPG, with paper-size,
                               orientation, theme, dashboard_filters)
* ``render_dashboard_tile``  — individual dashboard element (PNG/JPG)

All four share the same three-step Looker pattern: POST a create-task
endpoint, poll ``GET /render_tasks/{id}`` until the task reaches
``success`` or ``failure``, then fetch the rendered bytes from
``GET /render_tasks/{id}/results``. The shared
``_create_and_wait_for_render_task`` helper owns that loop so the four
tools cannot drift on backoff, terminal-state handling, or failure
detail propagation.

Binary results are returned via FastMCP's typed helpers:

* PNG/JPG → ``Image`` → MCP ``ImageContent`` (LLM-visible)
* PDF     → ``File``  → MCP ``EmbeddedResource`` with ``BlobResourceContents``

Timeout, dimension-cap, and size-cap conditions return JSON strings so
the caller can recover (e.g. re-fetch results with the surfaced
``render_task_id``) without losing the in-flight render.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Annotated, Any, Literal
from urllib.parse import urlencode

from fastmcp import FastMCP
from fastmcp.utilities.types import File, Image

from ..client import LookerApiError, LookerClient, LookerSession, format_api_error
from ._helpers import (
    ActAsUser,
    _maybe_use_branch,
    _path_seg,
    _set_if,
    _validate_branch_args,
)

# ─── Constants ─────────────────────────────────────────────────────────

# Hard pixel-product cap. Looker accepts very large render requests but
# starves its own queue under them; rejecting client-side keeps a single
# bad call from blocking unrelated renders on the same instance.
_MAX_PIXELS = 4000 * 4000

# Cap returned binary at 10 MB so a giant PDF doesn't blow up the MCP
# transport. Past this, return the ``render_task_id`` and let the caller
# fetch the bytes through the Looker UI / direct API.
_MAX_BYTES = 10 * 1024 * 1024

# Looker's RenderTask.status state machine. Terminal states end polling.
_TERMINAL_STATUSES = frozenset({"success", "failure"})

# Polling backoff schedule (seconds). Fast renders feel instant; long
# PDFs don't burn tokens on a tight loop. Capped at the last value.
_POLL_DELAYS_SECONDS = (0.5, 1.0, 2.0)


# ─── Shared dataclasses ────────────────────────────────────────────────


@dataclass(frozen=True)
class _RenderResult:
    """Outcome of a render-task create/poll/fetch cycle.

    Three terminal shapes the tool layer turns into MCP responses:

    * **Success**  ``body`` carries the bytes; ``truncated`` is False.
    * **Timeout**  ``body`` is None and ``truncated`` is False — the
      polling deadline expired before the task reached a terminal
      status. ``meta["render_task_id"]`` lets callers recover.
    * **Truncated**  ``truncated`` is True — the response exceeded the
      streaming cap. ``body`` is unsafe to return (empty when the
      Content-Length fast path fired, or a prefix when the cap fired
      mid-chunk); ``size_bytes`` carries the size signal (exact when
      Content-Length was honoured, a lower bound when the cap fired
      mid-stream).
    """

    body: bytes | None
    content_type: str | None
    size_bytes: int | None
    truncated: bool
    meta: dict[str, Any]


# ─── Validation helpers ────────────────────────────────────────────────


def _validate_dimensions(width: int, height: int) -> None:
    """Reject obviously-bad render dimensions before hitting Looker.

    Looker validates each dimension server-side but the more interesting
    failure mode is the product — a 10_000 × 10_000 PNG is technically
    "valid" but starves the render queue. Capping the product keeps a
    single oversized request from blocking unrelated renders on the
    same instance.
    """
    if width <= 0 or height <= 0:
        raise ValueError(f"width and height must be positive; got width={width}, height={height}.")
    if width * height > _MAX_PIXELS:
        raise ValueError(
            f"width * height ({width * height}) exceeds the {_MAX_PIXELS} pixel "
            f"cap. Looker accepts larger requests but they starve the render "
            f"queue. Try a smaller size, then upscale client-side if needed."
        )


def _encode_dashboard_filters(filters: dict[str, str] | None) -> str | None:
    """Encode a ``{field: value}`` filter map as Looker's URL-query string.

    Looker's ``POST /render_tasks/dashboards/{id}/{format}`` takes
    ``dashboard_filters`` as a single URL-encoded string in the
    ``?Foo=bar&Baz=qux`` shape Looker URLs use natively. Accepting a
    typed dict at the tool boundary keeps the surface symmetric with
    the existing ``query`` tool's ``filters: dict[str, str]`` — at the
    cost of one urlencode here.
    """
    if not filters:
        return None
    return urlencode(filters, doseq=False)


# ─── Render-task lifecycle helper ──────────────────────────────────────


async def _create_and_wait_for_render_task(
    session: LookerSession,
    *,
    create_path: str,
    create_params: dict[str, Any],
    max_wait_seconds: float,
) -> _RenderResult:
    """POST create, poll status, GET binary results.

    The three Looker calls in lock-step:

    1. ``POST {create_path}?width=…&height=…&…`` → returns
       ``{"id": render_task_id, …}``
    2. ``GET /render_tasks/{id}`` repeatedly until
       ``status in {"success", "failure"}`` or the wall-clock deadline
       passes. Backoff is ``_POLL_DELAYS_SECONDS`` capped at the last
       value.
    3. On ``success``: ``GET /render_tasks/{id}/results`` for the bytes.
       Returns ``_RenderResult(body, content_type, meta)``.

    Behaviours that diverge from raw Looker:

    * ``failure`` status raises :class:`LookerApiError` with the
      ``status_detail`` body Looker returned, so tool error paths look
      identical to a 4xx/5xx HTTP error.
    * Deadline-hit returns ``_RenderResult(body=None, content_type=None,
      meta={...render_task_id, last_status...})`` so the tool can return
      a JSON escape hatch rather than swallow an in-flight render.
    """
    create_response = await session.post(create_path, params=create_params)
    if not isinstance(create_response, dict) or "id" not in create_response:
        raise LookerApiError(
            500,
            "Looker render-task create returned no id",
            f"POST {create_path} returned: {create_response!r}",
        )
    render_task_id = str(create_response["id"])

    poll_path = f"/render_tasks/{_path_seg(render_task_id)}"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_seconds
    last_status: str | None = None
    last_payload: dict[str, Any] | None = None
    attempt = 0
    while True:
        payload = await session.get(poll_path)
        # Fail fast on contract violations: Looker's render-task API
        # always returns a dict with a non-empty string ``status``
        # field. Anything else (a non-dict body, a missing status, a
        # numeric status) indicates a proxy/middleware injection or an
        # API change we should surface immediately rather than burn
        # poll cycles waiting for a terminal state we can never reach.
        if not isinstance(payload, dict):
            raise LookerApiError(
                500,
                "Looker render-task poll returned a non-dict payload",
                f"GET {poll_path} returned: {payload!r}",
            )
        status = payload.get("status")
        if not isinstance(status, str) or not status:
            raise LookerApiError(
                500,
                "Looker render-task poll missing string status field",
                f"GET {poll_path} returned: {payload!r}",
            )
        last_payload = payload
        last_status = status
        if last_status in _TERMINAL_STATUSES:
            break
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        delay = _POLL_DELAYS_SECONDS[min(attempt, len(_POLL_DELAYS_SECONDS) - 1)]
        await asyncio.sleep(min(delay, remaining))
        attempt += 1

    meta: dict[str, Any] = {
        "render_task_id": render_task_id,
        "last_status": last_status,
    }
    if last_payload is not None:
        for k in ("query_runtime", "render_runtime", "runtime", "status_detail"):
            if last_payload.get(k) is not None:
                meta[k] = last_payload[k]

    if last_status == "failure":
        raise LookerApiError(
            500,
            "Looker render task failed",
            (last_payload or {}).get("status_detail") or "no status_detail provided",
            body=last_payload,
        )

    if last_status != "success":
        # Deadline hit before Looker reached a terminal state. Return
        # the task id so the caller can poll later via Looker directly.
        return _RenderResult(
            body=None,
            content_type=None,
            size_bytes=None,
            truncated=False,
            meta=meta,
        )

    # Pass the transport cap to ``get_bytes`` so oversized renders are
    # short-circuited (via Content-Length) or streamed-and-truncated
    # rather than fully materialized in memory before we'd reject them.
    result_path = f"/render_tasks/{_path_seg(render_task_id)}/results"
    body, content_type, total, truncated = await session.get_bytes(
        result_path, max_bytes=_MAX_BYTES
    )
    return _RenderResult(
        body=None if truncated else body,
        content_type=content_type,
        size_bytes=total,
        truncated=truncated,
        meta=meta,
    )


def _build_result(
    result: _RenderResult,
    *,
    result_format: str,
    subject_name: str,
    subject_id: str,
) -> Image | File | str:
    """Convert a ``_RenderResult`` into the tool's MCP-friendly return.

    * Truncated (``result.truncated``) → JSON escape hatch with size + id
    * Timeout (``body is None``) → JSON escape hatch with ``render_task_id``
    * ``png``/``jpg`` → ``Image`` (FastMCP serializes to ``ImageContent``)
    * ``pdf`` → ``File`` (FastMCP serializes to ``EmbeddedResource``)

    ``subject_name`` / ``subject_id`` are only used to name the embedded
    PDF file resource — they have no effect on the rendered bytes.
    """
    if result.truncated:
        # ``size_bytes`` is exact when Content-Length was honoured and a
        # lower bound (``> _MAX_BYTES``) when the cap fired mid-stream.
        return json.dumps(
            {
                "status": "too_large",
                "size_bytes": result.size_bytes,
                "format": result_format,
                "note": (
                    f"Render exceeded the {_MAX_BYTES}-byte MCP transport cap. "
                    "Fetch directly from Looker's UI or via "
                    f"GET /render_tasks/{result.meta['render_task_id']}/results."
                ),
                **result.meta,
            },
            indent=2,
        )

    if result.body is None:
        return json.dumps(
            {
                "status": "timeout",
                "note": (
                    "Render exceeded max_wait_seconds. The task is still running "
                    "on Looker; fetch the bytes via Looker's UI or the API "
                    f"endpoint GET /render_tasks/{result.meta['render_task_id']}/results."
                ),
                **result.meta,
            },
            indent=2,
        )

    if result_format == "pdf":
        return File(
            data=result.body,
            format="pdf",
            name=f"{subject_name}-{subject_id}.pdf",
        )
    # png / jpg
    return Image(data=result.body, format=result_format)


# ─── Tool registration ─────────────────────────────────────────────────


def register_render_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "Render an ad-hoc Looker query as an image. Specify the same "
            "model/view/fields/filters/sorts surface as the ``query`` tool, "
            "plus width/height and ``png``/``jpg`` format. Looker creates the "
            "Query, renders it asynchronously, and returns the bytes. "
            "Use ``render_look`` for saved Looks or ``render_dashboard`` for "
            "dashboards (the only render subject that supports PDF)."
        ),
    )
    async def render_query(
        model: Annotated[str, "LookML model name"],
        view: Annotated[str, "Explore/view name within the model"],
        fields: Annotated[list[str], "Fully-qualified field names to select"],
        width: Annotated[int, "Output width in pixels"],
        height: Annotated[int, "Output height in pixels"],
        result_format: Annotated[Literal["png", "jpg"], "Image format"] = "png",
        filters: Annotated[
            dict[str, str] | None,
            "Filter expressions as field:value pairs",
        ] = None,
        sorts: Annotated[list[str] | None, "Sort expressions"] = None,
        limit: Annotated[int, "Row limit"] = 500,
        max_wait_seconds: Annotated[
            float,
            "Polling deadline in seconds. On timeout the tool returns a "
            "JSON escape hatch with the ``render_task_id`` so the render "
            "is recoverable without restarting.",
        ] = 120.0,
        dev_mode: Annotated[
            bool,
            "Resolve the query against the dev workspace's LookML rather than "
            "production. Implied when ``branch`` is set.",
        ] = False,
        branch: Annotated[
            str | None,
            "Project branch to atomically swap to for this call (saved branch "
            "restored on exit). Requires project_id.",
        ] = None,
        project_id: Annotated[
            str | None,
            "LookML project ID — required when ``branch`` is set.",
        ] = None,
        act_as_user: ActAsUser = None,
    ) -> Image | File | str:
        ctx = client.build_context(
            "render_query",
            "render",
            {
                "model": model,
                "view": view,
                "result_format": result_format,
                "width": width,
                "height": height,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_dimensions(width, height)
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    query_body: dict[str, Any] = {
                        "model": model,
                        "view": view,
                        "fields": fields,
                        "limit": str(limit),
                    }
                    if filters:
                        query_body["filters"] = filters
                    if sorts:
                        query_body["sorts"] = sorts
                    query_def = await session.post("/queries", body=query_body)
                    # Mirror the shape check in
                    # ``_create_and_wait_for_render_task``: surface a
                    # ``LookerApiError`` carrying the full payload
                    # instead of an opaque ``KeyError`` / ``TypeError``
                    # when Looker returns a non-dict or omits ``id``,
                    # and coerce to ``str`` so downstream path-building
                    # never trips on a numeric id.
                    if not isinstance(query_def, dict) or "id" not in query_def:
                        raise LookerApiError(
                            500,
                            "Looker /queries create returned no id",
                            f"POST /queries returned: {query_def!r}",
                        )
                    query_id = str(query_def["id"])

                    result = await _create_and_wait_for_render_task(
                        session,
                        create_path=(
                            f"/render_tasks/queries/{_path_seg(query_id)}/{result_format}"
                        ),
                        create_params={"width": width, "height": height},
                        max_wait_seconds=max_wait_seconds,
                    )
                    return _build_result(
                        result,
                        result_format=result_format,
                        subject_name="query",
                        subject_id=str(query_id),
                    )
        except Exception as e:
            return format_api_error("render_query", e)

    @server.tool(
        description=(
            "Render a saved Look as an image. Returns PNG or JPG bytes. "
            "For PDF, use ``render_dashboard``."
        ),
    )
    async def render_look(
        look_id: Annotated[str, "ID of the saved Look"],
        width: Annotated[int, "Output width in pixels"],
        height: Annotated[int, "Output height in pixels"],
        result_format: Annotated[Literal["png", "jpg"], "Image format"] = "png",
        max_wait_seconds: Annotated[
            float,
            "Polling deadline in seconds. On timeout the tool returns a "
            "JSON escape hatch with the ``render_task_id``.",
        ] = 120.0,
        act_as_user: ActAsUser = None,
    ) -> Image | File | str:
        ctx = client.build_context(
            "render_look",
            "render",
            {
                "look_id": look_id,
                "result_format": result_format,
                "width": width,
                "height": height,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_dimensions(width, height)
            async with client.session(ctx) as session:
                result = await _create_and_wait_for_render_task(
                    session,
                    create_path=(f"/render_tasks/looks/{_path_seg(look_id)}/{result_format}"),
                    create_params={"width": width, "height": height},
                    max_wait_seconds=max_wait_seconds,
                )
                return _build_result(
                    result,
                    result_format=result_format,
                    subject_name="look",
                    subject_id=look_id,
                )
        except Exception as e:
            return format_api_error("render_look", e)

    @server.tool(
        description=(
            "Render a Looker dashboard as PDF, PNG, or JPG. Dashboards are the "
            "only render subject that supports PDF output. Accepts paper size, "
            "orientation, ``long_tables``, ``theme``, and a "
            "``dashboard_filters`` map. ``dashboard_id`` accepts both UDD "
            "numeric IDs and LookML dashboard IDs (``model::dashboard``)."
        ),
    )
    async def render_dashboard(
        dashboard_id: Annotated[
            str,
            "Dashboard ID. UDD numeric (e.g. '42') or LookML 'model::dashboard'",
        ],
        width: Annotated[int, "Output width in pixels"],
        height: Annotated[int, "Output height in pixels"],
        result_format: Annotated[Literal["pdf", "png", "jpg"], "Output format"] = "pdf",
        pdf_paper_size: Annotated[
            Literal["letter", "legal", "tabloid", "a0", "a1", "a2", "a3", "a4", "a5"] | None,
            "Paper size for PDF output. Ignored for png/jpg.",
        ] = None,
        pdf_landscape: Annotated[
            bool | None,
            "Render PDF in landscape orientation. Ignored for png/jpg.",
        ] = None,
        long_tables: Annotated[
            bool | None,
            "Expand table visualizations to their full row count. PDF only.",
        ] = None,
        theme: Annotated[
            str | None,
            "Looker theme name to apply. Renders the embedded version of the dashboard when set.",
        ] = None,
        dashboard_filters: Annotated[
            dict[str, str] | None,
            "Filter values as field:value pairs (URL-encoded internally into "
            "Looker's ?Foo=bar&Baz=qux dashboard-filter format).",
        ] = None,
        max_wait_seconds: Annotated[
            float,
            "Polling deadline in seconds. Dashboard PDFs commonly take "
            "30-90s; bump this for large dashboards. On timeout the tool "
            "returns a JSON escape hatch with the ``render_task_id``.",
        ] = 180.0,
        act_as_user: ActAsUser = None,
    ) -> Image | File | str:
        ctx = client.build_context(
            "render_dashboard",
            "render",
            {
                "dashboard_id": dashboard_id,
                "result_format": result_format,
                "width": width,
                "height": height,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_dimensions(width, height)
            params: dict[str, Any] = {"width": width, "height": height}
            # PDF-only knobs are gated on the format so an image render
            # never carries page-orientation/paper-size hints Looker has
            # no use for. ``theme`` and ``dashboard_filters`` apply to
            # all formats, so they stay outside the gate.
            if result_format == "pdf":
                _set_if(params, "pdf_paper_size", pdf_paper_size)
                # Looker's query-string parser accepts lowercase
                # 'true'/'false' but not Python's str(True) = 'True'.
                # Convert explicitly.
                if pdf_landscape is not None:
                    params["pdf_landscape"] = "true" if pdf_landscape else "false"
                if long_tables is not None:
                    params["long_tables"] = "true" if long_tables else "false"
            _set_if(params, "theme", theme)
            _set_if(params, "dashboard_filters", _encode_dashboard_filters(dashboard_filters))

            async with client.session(ctx) as session:
                result = await _create_and_wait_for_render_task(
                    session,
                    create_path=(
                        f"/render_tasks/dashboards/{_path_seg(dashboard_id)}/{result_format}"
                    ),
                    create_params=params,
                    max_wait_seconds=max_wait_seconds,
                )
                return _build_result(
                    result,
                    result_format=result_format,
                    subject_name="dashboard",
                    subject_id=dashboard_id,
                )
        except Exception as e:
            return format_api_error("render_dashboard", e)

    @server.tool(
        description=(
            "Render a single dashboard tile (one element) as PNG or JPG. Useful "
            "when you want one chart, not the whole dashboard. "
            "``dashboard_element_id`` is the element's ID — numeric for UDD "
            "dashboards, ``model::dashboard::tile`` for LookML dashboards. "
            "PDF is not supported by the underlying Looker endpoint."
        ),
    )
    async def render_dashboard_tile(
        dashboard_element_id: Annotated[
            str,
            "Dashboard element ID. UDD numeric or LookML 'model::dashboard::tile'.",
        ],
        width: Annotated[int, "Output width in pixels"],
        height: Annotated[int, "Output height in pixels"],
        result_format: Annotated[Literal["png", "jpg"], "Image format"] = "png",
        max_wait_seconds: Annotated[
            float,
            "Polling deadline in seconds. On timeout the tool returns a "
            "JSON escape hatch with the ``render_task_id``.",
        ] = 120.0,
        act_as_user: ActAsUser = None,
    ) -> Image | File | str:
        ctx = client.build_context(
            "render_dashboard_tile",
            "render",
            {
                "dashboard_element_id": dashboard_element_id,
                "result_format": result_format,
                "width": width,
                "height": height,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_dimensions(width, height)
            async with client.session(ctx) as session:
                result = await _create_and_wait_for_render_task(
                    session,
                    create_path=(
                        f"/render_tasks/dashboard_elements/"
                        f"{_path_seg(dashboard_element_id)}/{result_format}"
                    ),
                    create_params={"width": width, "height": height},
                    max_wait_seconds=max_wait_seconds,
                )
                return _build_result(
                    result,
                    result_format=result_format,
                    subject_name="tile",
                    subject_id=dashboard_element_id,
                )
        except Exception as e:
            return format_api_error("render_dashboard_tile", e)
