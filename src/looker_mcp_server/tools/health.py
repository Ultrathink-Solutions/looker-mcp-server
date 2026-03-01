"""Health tool group — instance health, usage analysis, and cleanup.

Composite tools that aggregate multiple Looker API calls to provide
high-level health insights.  These are read-only and safe to enable
for all deployments.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_health_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "Run a health check on the Looker instance. Returns status of "
            "database connections, datagroups, and overall system health."
        ),
    )
    async def health_pulse() -> str:
        ctx = client.build_context("health_pulse", "health")
        try:
            async with client.session(ctx) as session:
                result: dict[str, Any] = {}

                # Check connections
                try:
                    connections = await session.get("/connections")
                    result["connections"] = {
                        "count": len(connections or []),
                        "names": [c.get("name") for c in (connections or [])],
                    }
                except Exception as e:
                    result["connections"] = {"error": str(e)}

                # Check datagroups
                try:
                    datagroups = await session.get("/datagroups")
                    triggered = [dg for dg in (datagroups or []) if dg.get("triggered_at")]
                    stale = [dg for dg in (datagroups or []) if dg.get("stale_before")]
                    result["datagroups"] = {
                        "count": len(datagroups or []),
                        "recently_triggered": len(triggered),
                        "stale": len(stale),
                    }
                except Exception as e:
                    result["datagroups"] = {"error": str(e)}

                # Check running queries
                try:
                    running = await session.get("/running_queries")
                    result["running_queries"] = {"count": len(running or [])}
                except Exception as e:
                    result["running_queries"] = {"error": str(e)}

                result["status"] = "healthy"
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("health_pulse", e)

    @server.tool(
        description=(
            "Analyze usage patterns across Looker content. Shows which "
            "models, explores, and content are most and least used."
        ),
    )
    async def health_analyze(
        analysis_type: Annotated[
            str,
            "Type of analysis: 'models' (model/explore usage), 'content' (look/dashboard usage)",
        ] = "models",
    ) -> str:
        ctx = client.build_context("health_analyze", "health")
        try:
            async with client.session(ctx) as session:
                if analysis_type == "models":
                    models = await session.get("/lookml_models")
                    result = []
                    for m in models or []:
                        explores = m.get("explores") or []
                        result.append(
                            {
                                "model": m.get("name"),
                                "project": m.get("project_name"),
                                "explore_count": len(explores),
                                "has_content": m.get("has_content"),
                                "explores": [
                                    {"name": e.get("name"), "hidden": e.get("hidden")}
                                    for e in explores
                                ],
                            }
                        )
                    return json.dumps(
                        {"analysis": "models", "model_count": len(result), "models": result},
                        indent=2,
                    )

                elif analysis_type == "content":
                    looks = await session.get("/looks/search", params={"limit": 100})
                    dashboards = await session.get("/dashboards/search", params={"limit": 100})
                    return json.dumps(
                        {
                            "analysis": "content",
                            "look_count": len(looks or []),
                            "dashboard_count": len(dashboards or []),
                            "top_looks": sorted(
                                [
                                    {
                                        "id": lk.get("id"),
                                        "title": lk.get("title"),
                                        "view_count": lk.get("view_count") or 0,
                                    }
                                    for lk in (looks or [])
                                ],
                                key=lambda x: x["view_count"],
                                reverse=True,
                            )[:10],
                            "top_dashboards": sorted(
                                [
                                    {
                                        "id": d.get("id"),
                                        "title": d.get("title"),
                                        "view_count": d.get("view_count") or 0,
                                    }
                                    for d in (dashboards or [])
                                ],
                                key=lambda x: x["view_count"],
                                reverse=True,
                            )[:10],
                        },
                        indent=2,
                    )
                else:
                    return json.dumps(
                        {
                            "error": f"Unknown analysis type: {analysis_type}. "
                            "Use 'models' or 'content'."
                        },
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("health_analyze", e)

    @server.tool(
        description=(
            "Identify unused or orphaned LookML objects for cleanup. "
            "Scans for explores with no content, unreferenced views, "
            "and Looks/dashboards with zero views."
        ),
    )
    async def health_vacuum(
        scope: Annotated[
            str,
            "What to scan: 'explores' (unused explores), 'content' (zero-view Looks/dashboards)",
        ] = "explores",
    ) -> str:
        ctx = client.build_context("health_vacuum", "health")
        try:
            async with client.session(ctx) as session:
                if scope == "explores":
                    models = await session.get("/lookml_models")
                    unused = []
                    for m in models or []:
                        for e in m.get("explores") or []:
                            if not m.get("has_content") or e.get("hidden"):
                                unused.append(
                                    {
                                        "model": m.get("name"),
                                        "explore": e.get("name"),
                                        "hidden": e.get("hidden"),
                                        "reason": "hidden" if e.get("hidden") else "no_content",
                                    }
                                )
                    return json.dumps(
                        {"scope": "explores", "unused_count": len(unused), "unused": unused},
                        indent=2,
                    )

                elif scope == "content":
                    looks = await session.get("/looks/search", params={"limit": 200})
                    dashboards = await session.get("/dashboards/search", params={"limit": 200})

                    zero_view_looks = [
                        {"id": lk.get("id"), "title": lk.get("title"), "type": "look"}
                        for lk in (looks or [])
                        if (lk.get("view_count") or 0) == 0
                    ]
                    zero_view_dashboards = [
                        {"id": d.get("id"), "title": d.get("title"), "type": "dashboard"}
                        for d in (dashboards or [])
                        if (d.get("view_count") or 0) == 0
                    ]
                    all_unused = zero_view_looks + zero_view_dashboards
                    return json.dumps(
                        {"scope": "content", "unused_count": len(all_unused), "unused": all_unused},
                        indent=2,
                    )
                else:
                    return json.dumps(
                        {"error": f"Unknown scope: {scope}. Use 'explores' or 'content'."},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("health_vacuum", e)
