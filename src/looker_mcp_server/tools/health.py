"""Health tool group — instance health, usage analysis, and cleanup.

Composite tools that aggregate multiple Looker API calls to provide
high-level health insights.  These are read-only and safe to enable
for all deployments.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_health_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "Check the Looker instance's health and return counts of its database "
            "connections, datagroups (with how many are stale or recently triggered) "
            "and running queries. Use it to spot an instance-wide problem, such as stale "
            "datagroups or a pile-up of running queries. It does not test whether each "
            "connection works or look for broken content; use validate_content for that."
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
            "Summarize how the instance is used, either per model (its explores and "
            "whether it has content) or per piece of content (the most viewed Looks and "
            "dashboards). Use it to see which models or content matter most before a "
            "cleanup or a change. The content summary reads only the first 100 Looks and "
            "100 dashboards, so on a large instance it is a sample."
        ),
    )
    async def health_analyze(
        analysis_type: Annotated[
            Literal["models", "content"],
            "What to summarize: 'models' (each model's explores; the default) or "
            "'content' (the ten most viewed Looks and dashboards).",
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
            "Find candidates for cleanup: hidden explores and explores in models with no "
            "content, or Looks and dashboards that have never been viewed. Use it when "
            "tidying an instance, and review the candidates before deleting anything. "
            "The content scan reads only the first 200 Looks and 200 dashboards, so on a "
            "large instance it is partial."
        ),
    )
    async def health_vacuum(
        scope: Annotated[
            Literal["explores", "content"],
            "What to scan: 'explores' (hidden explores and those in models without "
            "content; the default) or 'content' (Looks and dashboards with zero views).",
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
