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

Pivots = Annotated[
    list[str] | None,
    "Fields to pivot on — becomes the column axis (e.g. ['orders.status'])",
]
FillFields = Annotated[
    list[str] | None,
    "Dimensions to fill gaps in (e.g. ['orders.created_date']) so sparse "
    "time series show empty periods instead of skipping them",
]
FilterExpression = Annotated[
    str | None,
    "Looker filter expression. Required for OR conditions — the `filters` "
    "argument structurally cannot express them "
    "(e.g. '${orders.total} > 100 OR ${orders.status} = \"complete\"')",
]
ColumnLimit = Annotated[int | None, "Maximum number of pivoted columns to return"]
Total = Annotated[bool | None, "Include a column totals row"]
RowTotal = Annotated[
    str | None,
    "Row-total placement for pivoted queries — 'right' or 'left'. A string, not a boolean",
]
Subtotals = Annotated[list[str] | None, "Fields to compute subtotals on"]
VisConfig = Annotated[
    dict[str, Any] | None,
    "Visualization configuration. Opaque to Looker except the 'type' key, "
    "which selects the visualization "
    "(e.g. {'type': 'looker_grid'}, {'type': 'looker_line'}). Unknown keys "
    "are ignored. Only meaningful on saved content — inert for raw data",
]
QueryTimezone = Annotated[str | None, "Timezone to run the query in (e.g. 'America/Chicago')"]
DynamicFields = Annotated[
    str | None,
    "JSON-encoded string defining table calculations and custom fields",
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
