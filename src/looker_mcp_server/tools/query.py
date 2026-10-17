"""Query tool group — semantic layer queries and content search.

Tools for running queries through Looker's semantic model, executing
saved Looks and dashboards, and searching across all content.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, Any, Literal

from fastmcp import FastMCP

from ..client import LookerClient, LookerSession, format_api_error
from ._helpers import (
    ACT_AS_USER_ARG,
    DEV_ARGS,
    ActAsUser,
    DevBranch,
    DevMode,
    DevProjectId,
    _maybe_use_branch,
    _path_seg,
    _validate_branch_args,
    tool_with_hidden_args,
)
from ._query_spec import (
    QueryExplore,
    QueryFields,
    QueryFilters,
    QueryLimit,
    QueryModel,
    QuerySorts,
    build_query_body,
    create_query,
)

# Looker returns these formats as text (``text/plain``, ``text/html`` or
# ``text/markdown``) rather than JSON; calling ``session.get`` on them would
# raise ``Expecting value`` from the JSON decoder. ``sql`` is the same trap
# that motivated #29 / ``get_text`` — kept here so any future format
# additions stay co-located.
_TEXT_PLAIN_FORMATS = frozenset({"csv", "txt", "sql", "html", "md"})


async def _run_path(
    session: LookerSession,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    result_format: str = "json",
) -> Any:
    """Execute a Looker ``/run`` request, routing text/plain formats correctly.

    Looker returns ``text/plain`` for the ``_TEXT_PLAIN_FORMATS`` (csv, txt,
    sql); calling ``session.get`` on those routes through ``response.json()``
    and raises ``Expecting value``. JSON formats go through ``session.get``.
    Single source of truth for every ``/run`` caller so the routing can't drift.
    """
    if result_format in _TEXT_PLAIN_FORMATS:
        return await session.get_text(path, params=params)
    return await session.get(path, params=params)


async def _execute_saved_query(
    session: LookerSession,
    query_id: str,
    result_format: str = "json",
    *,
    limit: int | None = None,
    apply_formatting: bool | None = None,
    apply_vis: bool | None = None,
    server_table_calcs: bool | None = None,
    cache: bool | None = None,
) -> Any:
    """Run an existing Looker ``Query`` via ``GET /queries/{id}/run/{format}``.

    Shared by ``run_query`` and ``run_dashboard``'s per-element loop so the
    two paths can't drift on path shape, param serialization, or the
    JSON-vs-text response routing.

    Booleans are serialized as lowercase ``true``/``false`` because httpx's
    default ``str(True)`` → ``"True"`` is not what Looker's query-string
    parser accepts. ``None`` values are omitted entirely so the caller sees
    Looker's documented defaults.
    """
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = str(limit)
    for key, value in (
        ("apply_formatting", apply_formatting),
        ("apply_vis", apply_vis),
        ("server_table_calcs", server_table_calcs),
        ("cache", cache),
    ):
        if value is not None:
            params[key] = "true" if value else "false"

    return await _run_path(
        session,
        f"/queries/{query_id}/run/{result_format}",
        params=params or None,
        result_format=result_format,
    )


def _run_payload(result: Any, result_format: str = "json") -> dict[str, Any]:
    """Build the data portion of a run response from a ``/run`` result.

    Shared by every run tool so the data envelope can't drift: JSON list
    results carry a ``row_count``; text/plain results (csv, txt) carry the
    ``format`` they were requested in; ``json_detail`` dicts pass straight
    through under ``data``. The metadata block (``query`` / ``look``) is merged
    in by each tool on top of this.
    """
    if isinstance(result, list):
        return {"row_count": len(result), "data": result}
    if isinstance(result, str):
        return {"format": result_format, "data": result}
    return {"data": result}


# Every Looker ``result_format`` that this path can return as JSON or text.
# The binary formats (``xlsx``, ``png``, ``jpg``) are left out: the response
# is decoded as JSON or text, so they never worked here, and the render group
# covers images. ``json_fe`` is not in Looker's current reference but is still
# accepted by Looker and was forwarded before this was an enum.
ResultFormat = Annotated[
    Literal["json", "json_bi", "json_detail", "json_fe", "csv", "txt", "html", "md", "sql"],
    "Format of the result: 'json' (rows; the default), 'json_bi' (rows plus field, "
    "pivot and table-calculation metadata), 'json_detail' (an older form of json_bi), "
    "'json_fe', 'csv', 'txt', 'html', 'md', or 'sql' (the generated SQL instead of rows).",
]

# The content types Looker's Search Content endpoint (``GET /content/{terms}``)
# accepts in its ``types`` filter.
ContentType = Literal["dashboard", "look", "lookml_dashboard"]


def register_query_tools(
    server: FastMCP,
    client: LookerClient,
    *,
    dev_params: bool = False,
    act_as_user: bool = False,
) -> None:
    """Register the query group.

    ``dev_params`` offers the developer arguments (``DEV_ARGS``) and
    ``act_as_user`` offers per-call sudo. Both default to off, and an argument
    that is off is absent from the tool schemas (see ``tool_with_hidden_args``).
    The server factory turns them on from the deployment's configuration.
    """
    hidden_args: set[str] = set()
    if not dev_params:
        hidden_args.update(DEV_ARGS)
    if not act_as_user:
        hidden_args.add(ACT_AS_USER_ARG)

    def tool(description: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return tool_with_hidden_args(server, description=description, hidden_args=hidden_args)

    @tool(
        description=(
            "Run a query against a Looker explore and return its rows, a row count "
            "and a link that opens the query in Looker. Use it to answer a question "
            "with data: give the model, the explore (the `view` argument), fully "
            "qualified field names and optional filters, and Looker writes and runs "
            "the SQL. Look up field names with list_dimensions or list_measures first. "
            "To see the SQL without running it use query_sql, and to run a saved Look "
            "or dashboard use run_look or run_dashboard."
        ),
    )
    async def query(
        model: QueryModel,
        view: QueryExplore,
        fields: QueryFields,
        filters: QueryFilters = None,
        sorts: QuerySorts = None,
        limit: QueryLimit = 500,
        result_format: ResultFormat = "json",
        dev_mode: DevMode = False,
        branch: DevBranch = None,
        project_id: DevProjectId = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    query_def = await create_query(
                        session,
                        build_query_body(
                            model=model,
                            view=view,
                            fields=fields,
                            filters=filters,
                            sorts=sorts,
                            limit=limit,
                        ),
                    )
                    query_id = query_def["id"]

                    result = await _run_path(
                        session,
                        f"/queries/{query_id}/run/{result_format}",
                        result_format=result_format,
                    )
                    # ``query_def`` is the full Query object Looker returns from
                    # POST /queries — it carries share_url / expanded_share_url /
                    # url (the explore link). Surface it instead of dropping it.
                    return json.dumps(
                        {"query": query_def, **_run_payload(result, result_format)},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("query", e)

    @tool(
        description=(
            "Return the SQL that Looker would run for a query, without running it. "
            "Use it to check how Looker translates fields and filters into SQL, for "
            "example when a result looks wrong. It takes the same arguments as query "
            "and returns the SQL text with the query's Looker link, but no rows; use "
            "query for the data."
        ),
    )
    async def query_sql(
        model: QueryModel,
        view: QueryExplore,
        fields: QueryFields,
        filters: QueryFilters = None,
        sorts: QuerySorts = None,
        limit: QueryLimit = 500,
        dev_mode: DevMode = False,
        branch: DevBranch = None,
        project_id: DevProjectId = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query_sql",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    query_def = await create_query(
                        session,
                        build_query_body(
                            model=model,
                            view=view,
                            fields=fields,
                            filters=filters,
                            sorts=sorts,
                            limit=limit,
                        ),
                    )
                    query_id = query_def["id"]

                    # Looker returns the compiled SQL as text/plain; calling
                    # session.get would route through response.json() and raise
                    # ``Expecting value: line 1 column 1 (char 0)``. Mirrors the
                    # pattern used by the git deploy-key tools.
                    result = await session.get_text(f"/queries/{query_id}/run/sql")
                    # Surface the Query object (explore link + slug) alongside
                    # the compiled SQL rather than dropping it.
                    return json.dumps({"query": query_def, "sql": result}, indent=2)
        except Exception as e:
            return format_api_error("query_sql", e)

    @tool(
        description=(
            "Run a saved Look by its ID and return its rows with the Look's title and "
            "links. Use it when a saved Look already answers the question; find Look "
            "IDs with search_content, whose 'look' results carry the ID as content_id. "
            "The Look runs with its saved fields and filters, so to change them build "
            "a new query with query instead."
        ),
    )
    async def run_look(
        look_id: Annotated[
            str,
            "ID of the saved Look, for example '42'. search_content returns it as a "
            "look's content_id.",
        ],
        result_format: ResultFormat = "json",
        limit: QueryLimit = 500,
        dev_mode: DevMode = False,
        branch: DevBranch = None,
        project_id: DevProjectId = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "run_look",
            "query",
            {
                "look_id": look_id,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    # The /run endpoint returns only rows; fetch the Look object
                    # for its metadata (short_url / public_url + the embedded
                    # Query's explore link).
                    look_def = await session.get(f"/looks/{look_id}")
                    result = await _run_path(
                        session,
                        f"/looks/{look_id}/run/{result_format}",
                        params={"limit": limit},
                        result_format=result_format,
                    )
                    return json.dumps(
                        {"look": look_def, **_run_payload(result, result_format)},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("run_look", e)

    @tool(
        description=(
            "Run an existing Looker query by its ID and return its rows, keeping "
            "everything saved with it, such as table calculations, custom fields and "
            "visualization settings. Use it to re-run a query whose ID you already "
            "have, for example a dashboard tile's query.id or the query_id that "
            "query_url returns. To run a new or changed query use query, which takes "
            "fields and filters instead of an ID."
        ),
    )
    async def run_query(
        query_id: Annotated[str, "ID of the saved query, for example '1234'."],
        result_format: ResultFormat = "json",
        limit: Annotated[
            int | None,
            "Row limit for this run, for example 100. Default: the limit saved with the query.",
        ] = None,
        apply_formatting: Annotated[
            bool,
            "Set true to return values formatted for display, such as currency "
            "symbols and date formats. Default false.",
        ] = False,
        apply_vis: Annotated[
            bool,
            "Set true to apply the query's saved visualization settings to the "
            "values. Default false.",
        ] = False,
        server_table_calcs: Annotated[
            bool,
            "Set true to compute the query's table calculations and include them "
            "in the rows. Default false.",
        ] = False,
        cache: Annotated[
            bool,
            "Set false to skip Looker's result cache and run against the database. Default true.",
        ] = True,
        dev_mode: DevMode = False,
        branch: DevBranch = None,
        project_id: DevProjectId = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "run_query",
            "query",
            {
                "query_id": query_id,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    # run_query is given an existing id, so the explore link
                    # isn't in hand — fetch the Query object to recover it.
                    # The /run endpoint returns only data rows.
                    query_def = await session.get(f"/queries/{query_id}")
                    result = await _execute_saved_query(
                        session,
                        query_id,
                        result_format,
                        limit=limit,
                        apply_formatting=apply_formatting,
                        apply_vis=apply_vis,
                        server_table_calcs=server_table_calcs,
                        cache=cache,
                    )
                    return json.dumps(
                        {"query": query_def, **_run_payload(result, result_format)},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("run_query", e)

    @tool(
        description=(
            "Run every tile on a dashboard and return the dashboard's details with "
            "each tile's title, query and rows. Use it when the question is about a "
            "whole dashboard; find dashboard IDs with search_content, whose 'dashboard' "
            "and 'lookml_dashboard' results carry the ID as content_id. The response "
            "carries every tile's data and can be large, so for a single tile use "
            "run_query with that tile's query ID. A tile whose query fails reports an "
            "error instead of rows."
        ),
    )
    async def run_dashboard(
        dashboard_id: Annotated[
            str,
            "ID of the dashboard, for example '17', or 'model::slug' for a LookML "
            "dashboard. search_content returns it as a dashboard's content_id.",
        ],
    ) -> str:
        ctx = client.build_context("run_dashboard", "query", {"dashboard_id": dashboard_id})
        try:
            async with client.session(ctx) as session:
                dashboard = await session.get(f"/dashboards/{dashboard_id}")
                elements = dashboard.get("dashboard_elements") or []

                results: list[dict[str, Any]] = []
                for elem in elements:
                    elem_info: dict[str, Any] = {
                        "title": elem.get("title") or elem.get("title_text"),
                        "type": elem.get("type"),
                    }
                    # The tile's embedded Query carries its explore link
                    # (share_url) — surface it instead of dropping it.
                    if elem.get("query"):
                        elem_info["query"] = elem["query"]
                    query_id = (elem.get("query") or {}).get("id") if elem.get("query") else None
                    result_maker = elem.get("result_maker")
                    if result_maker:
                        query_id = query_id or (result_maker.get("query") or {}).get("id")

                    if query_id:
                        try:
                            data = await _execute_saved_query(session, query_id, "json")
                            elem_info["row_count"] = len(data) if isinstance(data, list) else 0
                            elem_info["data"] = data
                        except Exception:
                            elem_info["error"] = "Failed to execute element query"
                    results.append(elem_info)

                # Pass the dashboard's own metadata through (minus the elements,
                # which are returned enriched above) so it isn't dropped.
                dashboard_meta = {k: v for k, v in dashboard.items() if k != "dashboard_elements"}
                return json.dumps(
                    {
                        "title": dashboard.get("title"),
                        "description": dashboard.get("description"),
                        "dashboard": dashboard_meta,
                        "element_count": len(results),
                        "elements": results,
                    },
                    indent=2,
                )
        except Exception as e:
            return format_api_error("run_dashboard", e)

    @tool(
        description=(
            "Build a link that opens a query in Looker's Explore page, without "
            "running it. Use it to hand a person a link where they can see or adjust "
            "the query themselves. It returns the URL and the query's ID, not data; "
            "use query for the rows."
        ),
    )
    async def query_url(
        model: QueryModel,
        view: QueryExplore,
        fields: QueryFields,
        filters: QueryFilters = None,
        sorts: QuerySorts = None,
        dev_mode: DevMode = False,
        branch: DevBranch = None,
        project_id: DevProjectId = None,
        act_as_user: ActAsUser = None,
    ) -> str:
        ctx = client.build_context(
            "query_url",
            "query",
            {
                "model": model,
                "view": view,
                "branch": branch,
                "project_id": project_id,
                "act_as_user": act_as_user,
            },
        )
        try:
            _validate_branch_args(branch, project_id)
            effective_dev_mode = dev_mode or branch is not None
            async with client.session(ctx, dev_mode=effective_dev_mode) as session:
                async with _maybe_use_branch(session, project_id, branch):
                    query_def = await create_query(
                        session,
                        build_query_body(
                            model=model,
                            view=view,
                            fields=fields,
                            filters=filters,
                            sorts=sorts,
                        ),
                    )
                    share_url = query_def.get("share_url") or query_def.get("url")
                    return json.dumps(
                        {"url": share_url, "query_id": query_def.get("id")},
                        indent=2,
                    )
        except Exception as e:
            return format_api_error("query_url", e)

    @tool(
        description=(
            "Search saved Looks, dashboards and LookML dashboards by text and return "
            "the closest matches, each with its content_id and type, plus its title "
            "and folder when Looker returns them. "
            "Use it to find existing content about a topic, then pass a "
            "'look' result's content_id to run_look and a 'dashboard' or "
            "'lookml_dashboard' result's content_id to run_dashboard. It matches "
            "titles and descriptions, not the data inside the content, and does not "
            "return folders or explores."
        ),
    )
    async def search_content(
        query_string: Annotated[str, "Text to search for, for example 'revenue'."],
        types: Annotated[
            list[ContentType] | None,
            "Content types to include: 'dashboard', 'look' or 'lookml_dashboard', for "
            "example ['dashboard', 'look']. Default: all three.",
        ] = None,
        limit: Annotated[int, "Maximum results to return, for example 10. Default 20."] = 20,
    ) -> str:
        ctx = client.build_context("search_content", "query", {"query_string": query_string})
        try:
            if not query_string.strip():
                raise ValueError("query_string must contain text to search for.")
            async with client.session(ctx) as session:
                # Looker's Search Content endpoint takes the terms as a path
                # segment, so they are percent-encoded (a '/' included).
                params: dict[str, Any] = {"limit": limit}
                if types:
                    params["types"] = ",".join(types)
                results = await session.get(f"/content/{_path_seg(query_string)}", params=params)
                return json.dumps(results, indent=2)
        except Exception as e:
            return format_api_error("search_content", e)
