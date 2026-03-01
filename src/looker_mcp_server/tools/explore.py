"""Explore tool group — semantic model discovery.

Tools for browsing LookML models, explores, dimensions, and measures.
These are read-only and form the foundation for understanding what data
is available before querying.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error


def register_explore_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "List all LookML models accessible to the current user. "
            "Returns model names, labels, associated projects, and their explores."
        ),
    )
    async def list_models() -> str:
        ctx = client.build_context("list_models", "explore")
        try:
            async with client.session(ctx) as session:
                models = await session.get("/lookml_models")
                result = [
                    {
                        "name": m.get("name"),
                        "label": m.get("label"),
                        "project_name": m.get("project_name"),
                        "has_content": m.get("has_content"),
                        "explores": [
                            {"name": e.get("name"), "label": e.get("label")}
                            for e in (m.get("explores") or [])
                        ],
                    }
                    for m in (models or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_models", e)

    @server.tool(
        description=(
            "Get detailed information about a specific LookML model, "
            "including all its explores and their descriptions."
        ),
    )
    async def get_model(
        model_name: Annotated[str, "Name of the LookML model"],
    ) -> str:
        ctx = client.build_context("get_model", "explore", {"model_name": model_name})
        try:
            async with client.session(ctx) as session:
                model = await session.get(f"/lookml_models/{model_name}")
                result = {
                    "name": model.get("name"),
                    "label": model.get("label"),
                    "project_name": model.get("project_name"),
                    "explores": [
                        {
                            "name": e.get("name"),
                            "label": e.get("label"),
                            "description": e.get("description"),
                            "group_label": e.get("group_label"),
                            "hidden": e.get("hidden"),
                        }
                        for e in (model.get("explores") or [])
                    ],
                }
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_model", e)

    @server.tool(
        description=(
            "Get full details of an explore including all its dimensions, measures, "
            "filters, and parameters. This is the primary tool for understanding "
            "what fields are available for querying."
        ),
    )
    async def get_explore(
        model_name: Annotated[str, "Name of the LookML model"],
        explore_name: Annotated[str, "Name of the explore within the model"],
    ) -> str:
        ctx = client.build_context(
            "get_explore", "explore", {"model_name": model_name, "explore_name": explore_name}
        )
        try:
            async with client.session(ctx) as session:
                explore = await session.get(f"/lookml_models/{model_name}/explores/{explore_name}")
                result = {
                    "name": explore.get("name"),
                    "label": explore.get("label"),
                    "description": explore.get("description"),
                    "dimensions": [
                        {
                            "name": d.get("name"),
                            "label": d.get("label"),
                            "type": d.get("type"),
                            "description": d.get("description"),
                            "sql": d.get("sql"),
                            "hidden": d.get("hidden"),
                        }
                        for d in (explore.get("fields", {}).get("dimensions") or [])
                        if not d.get("hidden")
                    ],
                    "measures": [
                        {
                            "name": m.get("name"),
                            "label": m.get("label"),
                            "type": m.get("type"),
                            "description": m.get("description"),
                            "sql": m.get("sql"),
                            "hidden": m.get("hidden"),
                        }
                        for m in (explore.get("fields", {}).get("measures") or [])
                        if not m.get("hidden")
                    ],
                    "filters": [
                        {
                            "name": f.get("name"),
                            "label": f.get("label"),
                            "type": f.get("type"),
                            "description": f.get("description"),
                        }
                        for f in (explore.get("fields", {}).get("filters") or [])
                    ],
                    "parameters": [
                        {
                            "name": p.get("name"),
                            "label": p.get("label"),
                            "type": p.get("type"),
                            "description": p.get("description"),
                        }
                        for p in (explore.get("fields", {}).get("parameters") or [])
                    ],
                }
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_explore", e)

    @server.tool(
        description=(
            "List dimensions in an explore. Convenience tool that returns "
            "only the dimension fields (name, label, type, description)."
        ),
    )
    async def list_dimensions(
        model_name: Annotated[str, "Name of the LookML model"],
        explore_name: Annotated[str, "Name of the explore"],
    ) -> str:
        ctx = client.build_context(
            "list_dimensions", "explore", {"model_name": model_name, "explore_name": explore_name}
        )
        try:
            async with client.session(ctx) as session:
                explore = await session.get(
                    f"/lookml_models/{model_name}/explores/{explore_name}",
                    params={"fields": "fields"},
                )
                dims = [
                    {
                        "name": d.get("name"),
                        "label": d.get("label"),
                        "type": d.get("type"),
                        "description": d.get("description"),
                    }
                    for d in (explore.get("fields", {}).get("dimensions") or [])
                    if not d.get("hidden")
                ]
                return json.dumps(dims, indent=2)
        except Exception as e:
            return format_api_error("list_dimensions", e)

    @server.tool(
        description=(
            "List measures in an explore. Convenience tool that returns "
            "only the measure fields (name, label, type, description)."
        ),
    )
    async def list_measures(
        model_name: Annotated[str, "Name of the LookML model"],
        explore_name: Annotated[str, "Name of the explore"],
    ) -> str:
        ctx = client.build_context(
            "list_measures", "explore", {"model_name": model_name, "explore_name": explore_name}
        )
        try:
            async with client.session(ctx) as session:
                explore = await session.get(
                    f"/lookml_models/{model_name}/explores/{explore_name}",
                    params={"fields": "fields"},
                )
                measures = [
                    {
                        "name": m.get("name"),
                        "label": m.get("label"),
                        "type": m.get("type"),
                        "description": m.get("description"),
                    }
                    for m in (explore.get("fields", {}).get("measures") or [])
                    if not m.get("hidden")
                ]
                return json.dumps(measures, indent=2)
        except Exception as e:
            return format_api_error("list_measures", e)

    @server.tool(
        description="List all database connections configured in the Looker instance.",
    )
    async def list_connections() -> str:
        ctx = client.build_context("list_connections", "explore")
        try:
            async with client.session(ctx) as session:
                connections = await session.get("/connections")
                result = [
                    {
                        "name": c.get("name"),
                        "dialect": c.get("dialect", {}).get("label") if c.get("dialect") else None,
                        "host": c.get("host"),
                        "database": c.get("database"),
                        "schema": c.get("schema"),
                    }
                    for c in (connections or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_connections", e)
