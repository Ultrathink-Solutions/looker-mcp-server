"""Tests for the shared WriteQuery builder.

``build_query_body`` is the single owner of the Looker WriteQuery payload
shape. These tests pin the wire contract — particularly the fields Looker
types as strings even though the tool surface takes ints/bools.
"""

from looker_mcp_server.tools._query_spec import build_query_body


class TestBuildQueryBody:
    def test_minimal_body_has_only_required_keys(self):
        body = build_query_body(model="ecommerce", view="orders", fields=["orders.id"])
        assert body == {"model": "ecommerce", "view": "orders", "fields": ["orders.id"]}

    def test_omits_none_so_looker_defaults_apply(self):
        body = build_query_body(
            model="m", view="v", fields=["f"], filters=None, sorts=None, limit=None
        )
        assert "filters" not in body
        assert "sorts" not in body
        assert "limit" not in body

    def test_limit_is_stringified(self):
        # Looker types Query.limit as a string on the wire.
        body = build_query_body(model="m", view="v", fields=["f"], limit=500)
        assert body["limit"] == "500"

    def test_column_limit_is_stringified(self):
        # Same string-typed trap as limit.
        body = build_query_body(model="m", view="v", fields=["f"], column_limit=10)
        assert body["column_limit"] == "10"

    def test_limit_negative_one_survives_for_unlimited(self):
        # Looker documents limit=-1 as "download unlimited results".
        body = build_query_body(model="m", view="v", fields=["f"], limit=-1)
        assert body["limit"] == "-1"

    def test_total_stays_boolean(self):
        body = build_query_body(model="m", view="v", fields=["f"], total=True)
        assert body["total"] is True

    def test_row_total_stays_string(self):
        # row_total is a string in the Looker spec, NOT a bool.
        body = build_query_body(model="m", view="v", fields=["f"], row_total="right")
        assert body["row_total"] == "right"

    def test_vis_config_passes_through_opaque(self):
        vis = {"type": "looker_grid", "show_row_numbers": True}
        body = build_query_body(model="m", view="v", fields=["f"], vis_config=vis)
        assert body["vis_config"] == vis

    def test_full_surface_round_trips(self):
        body = build_query_body(
            model="m",
            view="v",
            fields=["f"],
            filters={"v.d": "30 days"},
            sorts=["f desc"],
            pivots=["v.p"],
            fill_fields=["v.d"],
            filter_expression="${v.a} > 1 OR ${v.b} < 2",
            subtotals=["v.p"],
            query_timezone="America/Chicago",
            dynamic_fields='[{"table_calculation":"tc"}]',
        )
        assert body["pivots"] == ["v.p"]
        assert body["fill_fields"] == ["v.d"]
        assert body["filter_expression"] == "${v.a} > 1 OR ${v.b} < 2"
        assert body["subtotals"] == ["v.p"]
        assert body["query_timezone"] == "America/Chicago"
        assert body["dynamic_fields"] == '[{"table_calculation":"tc"}]'

    def test_never_emits_filter_config(self):
        # Looker: "When creating a query ... filter_config should be set to
        # null. Setting it to any other value could cause unexpected
        # filtering behavior." The builder must not offer it at all.
        body = build_query_body(model="m", view="v", fields=["f"], filters={"v.d": "30 days"})
        assert "filter_config" not in body

    def test_falsy_but_present_values_are_kept(self):
        # total=False is a meaningful instruction, not an omission.
        body = build_query_body(model="m", view="v", fields=["f"], total=False)
        assert body["total"] is False

    def test_empty_filters_dict_is_omitted(self):
        # An empty dict carries no instruction; omit rather than send {}.
        body = build_query_body(model="m", view="v", fields=["f"], filters={})
        assert "filters" not in body
