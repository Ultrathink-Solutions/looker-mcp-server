"""Audit tool group — query history, content usage, live-ops observability.

Covers two observability surfaces:

1. **``system__activity`` wrappers** — thin convenience queries over
   Looker's built-in ``system__activity`` LookML model, which is the
   canonical source of query history, content usage, PDT build logs,
   scheduled-plan run history, and event/login audit data. Each wrapper
   composes the right explore + fields + filters for a common audit
   question so callers don't have to know the schema; custom queries
   against ``system__activity`` can still use the generic ``query`` tool.

2. **Live-ops REST endpoints** — current instance state that isn't
   available through ``system__activity``: running queries, active user
   sessions, and project CI runs.  These are higher-velocity signals used
   for triage rather than historical audit.

Admin-only surface; disabled by default. Enable with ``--groups audit``.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import _path_seg


def register_audit_tools(server: FastMCP, client: LookerClient) -> None:
    # ── system__activity wrappers ────────────────────────────────────

    @server.tool(
        description=(
            "Audit query history from Looker's system__activity model. "
            "Returns rows of {time, user, model/explore, runtime, status, "
            "source} for queries run in the given window. Default sort is "
            "slowest-first, which surfaces runaway queries and cache misses. "
            "Use to answer: who ran what, when, how long did it take, which "
            "cache tier, did it fail?"
        ),
    )
    async def get_query_history(
        date_range: Annotated[
            str,
            (
                "Looker filter-syntax date expression, e.g. '7 days', "
                "'yesterday', '2026-04-01 to 2026-04-15'"
            ),
        ] = "7 days",
        user_email: Annotated[str | None, "Filter to a single user's queries"] = None,
        dashboard_id: Annotated[
            str | None, "Filter to queries issued from a specific dashboard"
        ] = None,
        min_runtime_seconds: Annotated[
            float | None, "Exclude queries that ran faster than this threshold"
        ] = None,
        errors_only: Annotated[bool, "Only include queries whose status is not 'complete'"] = False,
        limit: Annotated[int, "Maximum rows to return"] = 500,
    ) -> str:
        filters: dict[str, str] = {"history.created_time": date_range}
        if user_email is not None:
            filters["user.email"] = user_email
        if dashboard_id is not None:
            filters["dashboard.id"] = dashboard_id
        if min_runtime_seconds is not None:
            filters["history.runtime"] = f">={min_runtime_seconds}"
        if errors_only:
            filters["history.status"] = "-complete"

        return await _run_system_activity_query(
            client=client,
            tool_name="get_query_history",
            explore="history",
            fields=[
                "history.created_time",
                "user.email",
                "query.model",
                "query.view",
                "history.runtime",
                "history.status",
                "history.issuer_source",
                "history.result_source",
                "dashboard.id",
                "look.id",
            ],
            filters=filters,
            sorts=["history.runtime desc"],
            limit=limit,
        )

    @server.tool(
        description=(
            "Audit content usage (dashboards + looks) over a time window. "
            "Returns each piece of content with its view count, API count, "
            "and last-viewed timestamp. Default sort is most-viewed first. "
            "Use to identify popular or stale content, or to answer 'is "
            "anyone still using dashboard X before I delete it'."
        ),
    )
    async def get_content_usage(
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "30 days",
        content_type: Annotated[str, "'dashboard', 'look', or 'all' (both types)"] = "all",
        min_views: Annotated[int, "Exclude content with fewer views than this"] = 0,
        limit: Annotated[int, "Maximum rows to return"] = 100,
    ) -> str:
        filters: dict[str, str] = {"history.created_date": date_range}
        if content_type != "all":
            filters["content_usage.content_type"] = content_type
        if min_views > 0:
            filters["content_usage.view_count"] = f">={min_views}"

        return await _run_system_activity_query(
            client=client,
            tool_name="get_content_usage",
            explore="content_usage",
            fields=[
                "content_usage.content_type",
                "content_usage.content_id",
                "content_usage.content_title",
                "content_usage.last_accessed_date",
                "content_usage.view_count",
                "content_usage.api_count",
            ],
            filters=filters,
            sorts=["content_usage.view_count desc"],
            limit=limit,
        )

    @server.tool(
        description=(
            "Audit PDT (Persistent Derived Table) build events. Returns rows "
            "describing each build attempt — when it ran, against which "
            "model/view, runtime, status, and any error message. Filter by "
            "``failed_only`` to triage broken PDTs."
        ),
    )
    async def get_pdt_build_log(
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "7 days",
        model: Annotated[str | None, "Filter to one LookML model"] = None,
        view: Annotated[str | None, "Filter to one view (PDT name)"] = None,
        failed_only: Annotated[bool, "Only include builds with errors"] = False,
        limit: Annotated[int, "Maximum rows to return"] = 500,
    ) -> str:
        filters: dict[str, str] = {"pdt_event_log.created_time": date_range}
        if model is not None:
            filters["pdt_event_log.model_name"] = model
        if view is not None:
            filters["pdt_event_log.view_name"] = view
        if failed_only:
            filters["pdt_event_log.status_code"] = "error"

        return await _run_system_activity_query(
            client=client,
            tool_name="get_pdt_build_log",
            explore="pdt_event_log",
            fields=[
                "pdt_event_log.created_time",
                "pdt_event_log.model_name",
                "pdt_event_log.view_name",
                "pdt_event_log.action_name",
                "pdt_event_log.status_code",
                "pdt_event_log.runtime",
                "pdt_event_log.message",
            ],
            filters=filters,
            sorts=["pdt_event_log.created_time desc"],
            limit=limit,
        )

    @server.tool(
        description=(
            "Audit scheduled-plan run history. Returns one row per run with "
            "plan title, owner, timestamp, and status. Filter by "
            "``failed_only`` to triage broken schedules (most common use)."
        ),
    )
    async def get_schedule_history(
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "7 days",
        schedule_id: Annotated[str | None, "Filter to one schedule"] = None,
        failed_only: Annotated[bool, "Only include non-successful runs"] = False,
        limit: Annotated[int, "Maximum rows to return"] = 500,
    ) -> str:
        filters: dict[str, str] = {"scheduled_job.finalized_time": date_range}
        if schedule_id is not None:
            filters["scheduled_plan.id"] = schedule_id
        if failed_only:
            filters["scheduled_job.status"] = "-success"

        return await _run_system_activity_query(
            client=client,
            tool_name="get_schedule_history",
            explore="scheduled_plan",
            fields=[
                "scheduled_job.finalized_time",
                "scheduled_plan.id",
                "scheduled_plan.name",
                "user.email",
                "scheduled_job.status",
                "scheduled_job.status_detail",
            ],
            filters=filters,
            sorts=["scheduled_job.finalized_time desc"],
            limit=limit,
        )

    @server.tool(
        description=(
            "Audit Looker event log (logins, permission changes, content "
            "create/update/delete, sudo activity). Returns rows of "
            "{time, user, event_type, object_type, object_id}. Use to "
            "answer 'who changed permissions on X' or 'who has logged in "
            "recently'. ``event_types`` accepts a comma-separated list — "
            "common values: 'login', 'logout', 'create_dashboard', "
            "'update_user', 'sudo_login'."
        ),
    )
    async def get_user_activity_log(
        date_range: Annotated[str, "Looker filter-syntax date expression"] = "7 days",
        user_email: Annotated[str | None, "Filter to one user's events"] = None,
        event_types: Annotated[str | None, "Comma-separated list of event names to include"] = None,
        limit: Annotated[int, "Maximum rows to return"] = 500,
    ) -> str:
        filters: dict[str, str] = {"event.created_time": date_range}
        if user_email is not None:
            filters["user.email"] = user_email
        if event_types is not None:
            filters["event.name"] = event_types

        return await _run_system_activity_query(
            client=client,
            tool_name="get_user_activity_log",
            explore="event",
            fields=[
                "event.created_time",
                "user.email",
                "event.name",
                "event.object_type",
                "event.object_id",
            ],
            filters=filters,
            sorts=["event.created_time desc"],
            limit=limit,
        )

    # ── Live-ops: running queries ────────────────────────────────────

    @server.tool(
        description=(
            "List currently-running queries against the Looker instance. "
            "Returns query_task_id, source (e.g. dashboard, api, explore), "
            "runtime-so-far, and the user who issued the query. Use for "
            "triage when the instance is slow — pair with ``kill_query`` "
            "to stop runaway queries."
        ),
    )
    async def list_running_queries() -> str:
        ctx = client.build_context("list_running_queries", "audit")
        try:
            async with client.session(ctx) as session:
                running = await session.get("/running_queries")
                result = [
                    {
                        "query_task_id": q.get("query_task_id"),
                        "query_id": q.get("query_id"),
                        "source": q.get("source"),
                        "created_at": q.get("created_at"),
                        "runtime": q.get("runtime"),
                        "user_id": q.get("user_id"),
                        "user": (q.get("user") or {}).get("email"),
                    }
                    for q in (running or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_running_queries", e)

    @server.tool(
        description=(
            "Kill a currently-running query by its query_task_id (from "
            "``list_running_queries``). The query's database work is "
            "aborted and the issuing user sees an error. Use sparingly — "
            "killing a query mid-flight can leave partial state in scratch "
            "schemas for some databases."
        ),
    )
    async def kill_query(
        query_task_id: Annotated[str, "query_task_id of the running query"],
    ) -> str:
        ctx = client.build_context("kill_query", "audit", {"query_task_id": query_task_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/running_queries/{_path_seg(query_task_id)}")
                return json.dumps({"killed": True, "query_task_id": query_task_id}, indent=2)
        except Exception as e:
            return format_api_error("kill_query", e)

    # ── Live-ops: sessions ───────────────────────────────────────────

    @server.tool(
        description=(
            "List all currently active user sessions on the Looker instance. "
            "Returns session id, user, IP, created time, and expiration. "
            "Use for compliance audits or to identify stale sessions before "
            "terminating them."
        ),
    )
    async def list_active_sessions() -> str:
        ctx = client.build_context("list_active_sessions", "audit")
        try:
            async with client.session(ctx) as session:
                sessions_list = await session.get("/sessions")
                result = [
                    {
                        "id": s.get("id"),
                        "user_id": s.get("user_id"),
                        "ip_address": s.get("ip_address"),
                        "browser": s.get("browser"),
                        "created_at": s.get("created_at"),
                        "expires_at": s.get("expires_at"),
                        "extended_at": s.get("extended_at"),
                    }
                    for s in (sessions_list or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_active_sessions", e)

    @server.tool(
        description=(
            "Get metadata for a single session by id (from "
            "``list_active_sessions``). Useful when auditing a specific "
            "session without enumerating the whole list."
        ),
    )
    async def get_session(
        session_id: Annotated[str, "Session ID"],
    ) -> str:
        ctx = client.build_context("get_session", "audit", {"session_id": session_id})
        try:
            async with client.session(ctx) as session:
                data = await session.get(f"/sessions/{_path_seg(session_id)}")
                return json.dumps(data, indent=2)
        except Exception as e:
            return format_api_error("get_session", e)

    @server.tool(
        description=(
            "Terminate a user session by id. The user is signed out and "
            "will need to re-authenticate. Use for offboarding or to force "
            "re-auth after a permission change."
        ),
    )
    async def terminate_session(
        session_id: Annotated[str, "Session ID to terminate"],
    ) -> str:
        ctx = client.build_context("terminate_session", "audit", {"session_id": session_id})
        try:
            async with client.session(ctx) as session:
                await session.delete(f"/sessions/{_path_seg(session_id)}")
                return json.dumps({"terminated": True, "session_id": session_id}, indent=2)
        except Exception as e:
            return format_api_error("terminate_session", e)

    # ── Live-ops: project CI runs ────────────────────────────────────

    @server.tool(
        description=(
            "List Looker CI (continuous integration) runs for a LookML "
            "project. Returns each run with its trigger, status, "
            "started/finalized timestamps, and summary counts. Use to "
            "answer 'is the CI pipeline healthy' or 'when did LookML "
            "validation last pass'."
        ),
    )
    async def list_project_ci_runs(
        project_id: Annotated[str, "LookML project ID"],
    ) -> str:
        ctx = client.build_context("list_project_ci_runs", "audit", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                runs = await session.get(f"/projects/{_path_seg(project_id)}/ci/runs")
                result = [
                    {
                        "id": r.get("id"),
                        "status": r.get("status"),
                        "trigger": r.get("trigger"),
                        "branch": r.get("branch"),
                        "started_at": r.get("started_at"),
                        "finalized_at": r.get("finalized_at"),
                    }
                    for r in (runs or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_project_ci_runs", e)

    @server.tool(
        description=(
            "Get details for a single CI run, including the per-suite "
            "results and any error messages. Useful to diagnose a CI "
            "failure surfaced by ``list_project_ci_runs``."
        ),
    )
    async def get_project_ci_run(
        project_id: Annotated[str, "LookML project ID"],
        run_id: Annotated[str, "CI run ID"],
    ) -> str:
        ctx = client.build_context(
            "get_project_ci_run",
            "audit",
            {"project_id": project_id, "run_id": run_id},
        )
        try:
            async with client.session(ctx) as session:
                run = await session.get(
                    f"/projects/{_path_seg(project_id)}/ci/runs/{_path_seg(run_id)}"
                )
                return json.dumps(run, indent=2)
        except Exception as e:
            return format_api_error("get_project_ci_run", e)

    @server.tool(
        description=(
            "Trigger a new CI run for a LookML project. Returns the run "
            "id so the caller can poll ``get_project_ci_run`` for "
            "completion. Pair with the ``modeling`` group's "
            "``validate_project`` for ad-hoc LookML validation — CI runs "
            "exercise the full configured suite set."
        ),
    )
    async def trigger_project_ci_run(
        project_id: Annotated[str, "LookML project ID"],
        branch: Annotated[
            str | None,
            "Branch to validate (defaults to the project's production branch)",
        ] = None,
    ) -> str:
        ctx = client.build_context("trigger_project_ci_run", "audit", {"project_id": project_id})
        try:
            async with client.session(ctx) as session:
                body: dict[str, Any] = {}
                if branch is not None:
                    body["branch"] = branch
                run = await session.post(
                    f"/projects/{_path_seg(project_id)}/ci/runs", body=body or None
                )
                return json.dumps(
                    {
                        "id": run.get("id") if run else None,
                        "status": run.get("status") if run else None,
                        "triggered": True,
                        "next_step": ("Poll get_project_ci_run to watch for completion."),
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("trigger_project_ci_run", e)


async def _run_system_activity_query(
    *,
    client: LookerClient,
    tool_name: str,
    explore: str,
    fields: list[str],
    filters: dict[str, str],
    sorts: list[str] | None = None,
    limit: int = 500,
) -> str:
    """Compose + run an inline query against the ``system__activity`` model.

    Thin wrapper around ``POST /queries`` + ``GET /queries/{id}/run/json``.
    Each audit tool picks the right explore, field list, and filter map for
    its question; this helper handles the boilerplate.
    """
    ctx = client.build_context(tool_name, "audit", {"explore": explore})
    try:
        async with client.session(ctx) as session:
            body: dict[str, Any] = {
                "model": "system__activity",
                "view": explore,
                "fields": fields,
                "filters": filters,
                "limit": str(limit),
            }
            if sorts:
                body["sorts"] = sorts

            query_def = await session.post("/queries", body=body)
            rows = await session.get(f"/queries/{_path_seg(str(query_def['id']))}/run/json")
            row_count = len(rows) if isinstance(rows, list) else 0
            return json.dumps({"row_count": row_count, "rows": rows}, indent=2)
    except Exception as e:
        return format_api_error(tool_name, e)
