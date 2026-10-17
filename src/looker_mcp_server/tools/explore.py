"""Explore tool group — semantic model discovery.

Tools for browsing LookML models, explores, dimensions, and measures.
These are read-only and form the foundation for understanding what data
is available before querying.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from fastmcp import FastMCP

from ..client import LookerClient, format_api_error
from ._helpers import IncludeHidden, _filter_hidden

ModelName = Annotated[
    str,
    "LookML model name, for example 'ecommerce'. list_models returns the model names.",
]
ExploreName = Annotated[
    str,
    "Explore name within the model, for example 'orders'. get_model lists a model's explores.",
]

# The four field collections an explore carries, in the order returned.
_FIELD_KINDS = ("dimensions", "measures", "filters", "parameters")


def _field_view(field: dict[str, Any]) -> str | None:
    """Return the view a field belongs to.

    Looker sets ``view`` on every field; the ``view.field`` name prefix is the
    fallback for payloads that leave it out.
    """
    view = field.get("view")
    if view:
        return view
    name = field.get("name") or ""
    return name.split(".", 1)[0] if "." in name else None


def _concise_fields(
    items: list[dict[str, Any]],
    *,
    include_hidden: bool,
    include_sql: bool,
    include_descriptions: bool,
    wanted_fields: set[str] | None,
    wanted_views: set[str] | None,
) -> list[dict[str, Any]]:
    """Project explore fields onto the concise shape ``get_explore`` returns.

    Every field keeps its name, label and type. The description and SQL are
    opt-out and opt-in respectively, and ``hidden`` is reported only when
    hidden fields were asked for, since otherwise every returned field is
    visible. Empty values are dropped rather than sent as ``null``.
    """
    result: list[dict[str, Any]] = []
    for item in _filter_hidden(items, include_hidden):
        if wanted_fields is not None and item.get("name") not in wanted_fields:
            continue
        if wanted_views is not None and _field_view(item) not in wanted_views:
            continue
        entry: dict[str, Any] = {
            "name": item.get("name"),
            "label": item.get("label"),
            "type": item.get("type"),
        }
        if include_descriptions and item.get("description"):
            entry["description"] = item["description"]
        if include_sql and item.get("sql"):
            entry["sql"] = item["sql"]
        if include_hidden:
            entry["hidden"] = bool(item.get("hidden"))
        result.append({k: v for k, v in entry.items() if v is not None})
    return result


def _field_listing(explore: dict[str, Any], kind: str, *, include_hidden: bool) -> str:
    """Render one kind of an explore's fields the way ``get_explore`` does by default.

    Shared by ``list_dimensions`` and ``list_measures``: compact JSON with each
    field's name, label, type and description, no SQL, and no empty values.
    """
    fields = (explore.get("fields") or {}).get(kind) or []
    listing = _concise_fields(
        fields,
        include_hidden=include_hidden,
        include_sql=False,
        include_descriptions=True,
        wanted_fields=None,
        wanted_views=None,
    )
    return json.dumps(listing, separators=(",", ":"))


def register_explore_tools(server: FastMCP, client: LookerClient) -> None:
    @server.tool(
        description=(
            "List the LookML models the current user can access, each with the names "
            "and labels of its explores. Use it first, to find which model and explore "
            "hold the data for a question. It returns names only: use get_model for "
            "explore descriptions and get_explore or list_dimensions for fields."
        ),
    )
    async def list_models(include_hidden: IncludeHidden = False) -> str:
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
                            {
                                "name": e.get("name"),
                                "label": e.get("label"),
                                "hidden": e.get("hidden"),
                            }
                            for e in _filter_hidden(m.get("explores") or [], include_hidden)
                        ],
                    }
                    for m in (models or [])
                ]
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("list_models", e)

    @server.tool(
        description=(
            "Describe one LookML model: its explores, with each explore's label, "
            "description and group label. Use it to choose the right explore when a "
            "model has several. It does not list fields; use list_dimensions, "
            "list_measures or get_explore for those."
        ),
    )
    async def get_model(
        model_name: ModelName,
        include_hidden: IncludeHidden = False,
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
                        for e in _filter_hidden(model.get("explores") or [], include_hidden)
                    ],
                }
                return json.dumps(result, indent=2)
        except Exception as e:
            return format_api_error("get_model", e)

    @server.tool(
        description=(
            "Describe an explore's fields: its dimensions, measures, filter-only fields "
            "and parameters, each with its name, label, type and description. Use it to "
            "learn which fully qualified field names a query can use. A large explore "
            "has hundreds of fields, so narrow the response with `views` or `fields`, or "
            "use list_dimensions or list_measures when you need only one kind. Field SQL "
            "is left out unless include_sql is true."
        ),
    )
    async def get_explore(
        model_name: ModelName,
        explore_name: ExploreName,
        include_hidden: IncludeHidden = False,
        fields: Annotated[
            list[str] | None,
            "Return only these fields, as fully qualified names, for example "
            "['orders.region', 'orders.count']. Default: every field.",
        ] = None,
        views: Annotated[
            list[str] | None,
            "Return only the fields of these views, for example ['customers'] for "
            "the fields named 'customers.*'. Default: every view.",
        ] = None,
        include_sql: Annotated[
            bool,
            "Set true to include each field's LookML SQL. Default false.",
        ] = False,
        include_descriptions: Annotated[
            bool,
            "Set false to leave out field descriptions for a shorter response. Default true.",
        ] = True,
    ) -> str:
        ctx = client.build_context(
            "get_explore", "explore", {"model_name": model_name, "explore_name": explore_name}
        )
        try:
            async with client.session(ctx) as session:
                explore = await session.get(f"/lookml_models/{model_name}/explores/{explore_name}")
                explore_fields = explore.get("fields") or {}
                wanted_fields = set(fields) if fields else None
                wanted_views = set(views) if views else None
                result: dict[str, Any] = {
                    "name": explore.get("name"),
                    "label": explore.get("label"),
                    "description": explore.get("description"),
                }
                result = {k: v for k, v in result.items() if v is not None}
                for kind in _FIELD_KINDS:
                    result[kind] = _concise_fields(
                        explore_fields.get(kind) or [],
                        include_hidden=include_hidden,
                        include_sql=include_sql,
                        include_descriptions=include_descriptions,
                        wanted_fields=wanted_fields,
                        wanted_views=wanted_views,
                    )
                return json.dumps(result, separators=(",", ":"))
        except Exception as e:
            return format_api_error("get_explore", e)

    @server.tool(
        description=(
            "List an explore's dimensions, each with its name, label, type and "
            "description. Use it to find the fields a query can group or filter by. "
            "It leaves out measures (use list_measures) and field SQL (use get_explore "
            "with include_sql)."
        ),
    )
    async def list_dimensions(
        model_name: ModelName,
        explore_name: ExploreName,
        include_hidden: IncludeHidden = False,
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
                return _field_listing(explore, "dimensions", include_hidden=include_hidden)
        except Exception as e:
            return format_api_error("list_dimensions", e)

    @server.tool(
        description=(
            "List an explore's measures, each with its name, label, type and "
            "description. Use it to find the aggregates a query can return, such as "
            "counts, sums and averages. It leaves out dimensions (use list_dimensions) "
            "and field SQL (use get_explore with include_sql)."
        ),
    )
    async def list_measures(
        model_name: ModelName,
        explore_name: ExploreName,
        include_hidden: IncludeHidden = False,
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
                return _field_listing(explore, "measures", include_hidden=include_hidden)
        except Exception as e:
            return format_api_error("list_measures", e)

    @server.tool(
        description=(
            "List the database connections configured in Looker, with each one's name, "
            "dialect, host, database and schema. Use it to find the connection name "
            "that the schema tools, such as list_tables and list_columns, take. It does "
            "not test whether a connection works."
        ),
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
