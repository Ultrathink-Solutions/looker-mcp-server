"""Shared Looker ``WriteQuery`` construction and materialization.

Looker's ``POST /looks`` will not accept an inline query — it requires a
``query_id`` naming a Query that already exists.  Saving a Look is therefore
always two steps: ``POST /queries`` to materialize, then ``POST /looks``.

The same WriteQuery body shape is needed by every ad-hoc query tool and by
``create_look``, so it is assembled in exactly one place here.  Before this
module the body was hand-rolled at three separate sites in ``query.py`` and
a fourth (incorrect) shape in ``content.py``; that divergence is what let
``create_look`` ship 422-ing on every call.

Deliberately its own module rather than an addition to ``_helpers``: that
module is scoped to one-liners and says so.
"""

from __future__ import annotations

from typing import Annotated, Any

from ..client import LookerSession

# ── Shared argument descriptions ─────────────────────────────────────
# Defined once so create_look and the query tools describe identical
# concepts identically. For an LLM caller the Annotated description IS
# the interface, so wording drift is a real defect.

QueryModel = Annotated[
    str,
    "LookML model name, for example 'ecommerce'. list_models returns the model names.",
]
QueryExplore = Annotated[
    str,
    "Name of the explore to query, for example 'orders'. Looker's API calls this "
    "field 'view', but it takes the explore name that list_models or get_model returns.",
]
QueryFields = Annotated[
    list[str],
    "Fields to return, as fully qualified names from the explore, for example "
    "['orders.region', 'orders.total_revenue']. Dimensions group the rows and "
    "measures aggregate them.",
]
QueryFilters = Annotated[
    dict[str, str] | None,
    "Filters, as a map from a fully qualified field name to a Looker filter "
    "expression, for example {'orders.created_date': '90 days'} (the last 90 days), "
    "{'orders.amount': '>=100'}, {'orders.region': '-NULL'} (not null) or "
    "{'orders.status': 'foo,bar'} (either value). A leading '-' negates a value, and "
    "filters on different fields combine with AND. Default: no filters.",
]
QuerySorts = Annotated[
    list[str] | None,
    "Sort order as field names, each optionally followed by ' desc', for example "
    "['orders.total_revenue desc']. Default: Looker's default order.",
]
QueryLimit = Annotated[int, "Maximum rows to return, for example 100. Default 500."]

Pivots = Annotated[
    list[str] | None,
    "Fields to pivot on, which become the column axis, for example ['orders.status']. "
    "Default: no pivot.",
]
FillFields = Annotated[
    list[str] | None,
    "Dimensions to fill gaps in, so a sparse time series shows empty periods instead "
    "of skipping them, for example ['orders.created_date']. Default: no filling.",
]
FilterExpression = Annotated[
    str | None,
    "A Looker custom filter expression, needed for OR conditions, which `filters` "
    "cannot express, for example '${orders.total} > 100 OR ${orders.status} = "
    '"complete"\'. Default: none.',
]
ColumnLimit = Annotated[
    int | None,
    "Maximum number of pivoted columns to return, for example 20. Default: Looker's own limit.",
]
Total = Annotated[
    bool | None,
    "Set true to add a column-totals row, for example to total revenue across all "
    "rows. Default: no totals row.",
]
RowTotal = Annotated[
    str | None,
    "Where to place row totals on a pivoted query, as a string rather than a boolean, "
    "for example 'right'. Default: no row totals.",
]
Subtotals = Annotated[
    list[str] | None,
    "Dimensions to compute subtotals on, for example ['orders.region']. Default: no subtotals.",
]
VisConfig = Annotated[
    dict[str, Any] | None,
    "Visualization settings for the saved content. The 'type' key selects the "
    "visualization, for example {'type': 'looker_grid'} or {'type': 'looker_line'}; "
    "other keys are visualization-specific properties that Looker recognizes for "
    "that type, and unknown keys are ignored. It changes how saved content displays, "
    "not the data. Default: Looker's default visualization.",
]
QueryTimezone = Annotated[
    str | None,
    "Timezone to run the query in, as an IANA name, for example 'America/Chicago'. "
    "Default: Looker's configured timezone.",
]
DynamicFields = Annotated[
    str | None,
    "Table calculations and custom fields, as a JSON-encoded string, for example "
    '\'[{"category": "table_calculation", "table_calculation": "share", '
    '"label": "Share", "expression": "${orders.count} / sum(${orders.count})"}]\'. '
    "Default: none.",
]


def build_query_body(
    *,
    model: str,
    view: str,
    fields: list[str],
    filters: dict[str, str] | None = None,
    sorts: list[str] | None = None,
    limit: int | None = None,
    pivots: list[str] | None = None,
    fill_fields: list[str] | None = None,
    filter_expression: str | None = None,
    column_limit: int | None = None,
    total: bool | None = None,
    row_total: str | None = None,
    subtotals: list[str] | None = None,
    vis_config: dict[str, Any] | None = None,
    query_timezone: str | None = None,
    dynamic_fields: str | None = None,
) -> dict[str, Any]:
    """Assemble a Looker ``WriteQuery`` payload, omitting unset fields.

    ``None`` values are dropped rather than forwarded so Looker applies its
    documented defaults — sending an explicit ``null`` reads as "clear this
    field", which is a different instruction.  Empty collections are dropped
    for the same reason: ``{}`` and ``[]`` carry no instruction.  ``False``
    is *kept*, because ``total=False`` is a meaningful instruction.

    ``limit`` and ``column_limit`` are stringified: Looker types both as
    strings on the wire even though the tool surface takes ints.  ``total``
    is a genuine bool and ``row_total`` a genuine string — the asymmetry is
    Looker's, not ours.

    ``filter_config`` is intentionally absent and must stay that way; the
    Looker spec requires it to be null on create.
    """
    body: dict[str, Any] = {"model": model, "view": view, "fields": fields}

    # Stringified-on-the-wire numerics.
    if limit is not None:
        body["limit"] = str(limit)
    if column_limit is not None:
        body["column_limit"] = str(column_limit)

    # Collections and mappings: omit when unset OR empty.
    for key, value in (
        ("filters", filters),
        ("sorts", sorts),
        ("pivots", pivots),
        ("fill_fields", fill_fields),
        ("subtotals", subtotals),
        ("vis_config", vis_config),
    ):
        if value:
            body[key] = value

    # Scalars: omit only when None, so False and "" survive as instructions.
    for key, value in (
        ("filter_expression", filter_expression),
        ("total", total),
        ("row_total", row_total),
        ("query_timezone", query_timezone),
        ("dynamic_fields", dynamic_fields),
    ):
        if value is not None:
            body[key] = value

    return body


async def create_query(session: LookerSession, body: dict[str, Any]) -> dict[str, Any]:
    """Materialize a Query via ``POST /queries`` and return the Query object.

    The returned object carries ``id`` (needed by ``POST /looks`` and by the
    run endpoints) plus ``share_url`` / ``url`` — the explore links surfaced
    by the query tools.  Callers get the whole object rather than a bare id
    so nothing has to re-fetch it.
    """
    return await session.post("/queries", body=body)
