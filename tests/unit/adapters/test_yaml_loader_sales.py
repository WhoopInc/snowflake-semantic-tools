"""Focused checks for the Jaffle Sales authoring surface."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_column_diagnostics, _dbt_model_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.expressions import (
    _expression_reference_diagnostics,
    _filter_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.metrics import _metric_cycles, _metric_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.defs import (
    FilterDef,
    MetricDef,
    NonAdditiveDef,
    VerifiedQueryDef,
    WindowDef,
    WindowOrderDef,
)
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import (
    _multipath_diagnostics,
    _relationship_cycle_diagnostics,
    _relationship_diagnostics,
)
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, SemanticView
from snowflake_semantic_tools.domain.validate.expression import is_aggregate_expression
from tests.helpers.projects import load_views

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


@pytest.fixture(scope="module")
def sales() -> SemanticView:
    views = load_views(FIXTURE, manifest_path=MANIFEST)
    return next(view for view in views if view.fqn.endswith(".JAFFLE_SALES"))


def test_loads_table_grain_synonyms_and_equality_relationships(sales: SemanticView) -> None:
    customers = next(table for table in sales.tables if table.logical_name == "CUSTOMERS")
    assert customers.unique_keys == (("CUSTOMER_NAME",),)
    assert customers.synonyms == ("buyer", "guest")
    assert [relationship.name for relationship in sales.relationships] == [
        "ORDERS_TO_CUSTOMERS",
        "ORDERS_TO_LOCATIONS",
    ]


def test_loads_view_variables_and_substitutes_their_names(sales: SemanticView) -> None:
    assert [(variable.name, str(variable.data_type), str(variable.default)) for variable in sales.variables] == [
        ("LARGE_ORDER_CENTS", "NUMBER", "1000"),
        ("TAX_INCLUSIVE", "BOOLEAN", "FALSE"),
    ]
    large_filter = next(column for column in sales.columns if column.name == "IS_LARGE_ORDER")
    assert large_filter.expr.text == "ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS"


def test_resolves_project_vars_and_derived_metric_dependencies(sales: SemanticView) -> None:
    returned = next(metric for metric in sales.metrics if metric.name == "RETURNED_ORDER_COUNT")
    per_customer = next(metric for metric in sales.metrics if metric.name == "REVENUE_PER_CUSTOMER")
    assert "'returned'" in returned.expr.text
    assert per_customer.expr.text == "DIV0(ORDERS.TOTAL_REVENUE, CUSTOMERS.CUSTOMER_COUNT)"


def test_composes_instruction_channels_and_standalone_filter_prose(sales: SemanticView) -> None:
    assert sales.ai_sql_generation is not None
    assert "Round every monetary amount" in sales.ai_sql_generation
    assert "For ORDERS, high_value_threshold_cents is large_order_cents." in sales.ai_sql_generation
    assert sales.ai_question_categorization is not None
    assert "A question about staffing" in sales.ai_question_categorization
    assert sales.custom_instruction_names == ("jaffle_sql_conventions", "jaffle_question_scope")


def test_loads_verified_queries_dates_tags_and_staleness(sales: SemanticView) -> None:
    queries = {query.name: query for query in sales.verified_queries}
    assert queries["ORDER_COUNT_BY_STATE"].verified_at == 1769904000
    assert queries["REVENUE_BY_LOCATION"].verified_at == 1767225600
    assert queries["TOTAL_REVENUE_ALL_TIME"].verified_at is None
    assert sales.max_staleness == "300 seconds"
    assert [tag.name for tag in sales.tags] == [
        "SST_REF_DEV.JAFFLE.COST_CENTER",
        "SST_REF_DEV.JAFFLE.DATA_DOMAIN",
    ]


def test_attaches_metrics_whose_dependencies_exist(sales: SemanticView) -> None:
    names = {metric.name for metric in sales.metrics}
    assert {"TOTAL_REVENUE", "REVENUE_PER_ORDER", "REVENUE_PER_CUSTOMER"}.issubset(names)


def test_metric_cycle_is_reported_once_with_a_stable_path() -> None:
    metrics = (
        MetricDef("cycle_a", "{{ metric('cycle_b') }}", None, ()),
        MetricDef("cycle_b", "{{ metric('cycle_c') }}", None, ()),
        MetricDef("cycle_c", "{{ metric('cycle_a') }}", None, ()),
    )
    assert _metric_cycles(metrics) == (("cycle_a", "cycle_b", "cycle_c", "cycle_a"),)


def test_every_metric_on_a_cycle_is_in_a_reported_cycle() -> None:
    # The walk finishes cyc_b inside cyc_a -> cyc_b -> cyc_a and never re-enters it from cyc_c,
    # which sits only on cyc_a -> cyc_c -> cyc_b -> cyc_a; tail reaches a cycle without being on one.
    metrics = (
        MetricDef("cyc_a", "{{ metric('cyc_b') }} + {{ metric('cyc_c') }}", None, ()),
        MetricDef("cyc_b", "{{ metric('cyc_a') }}", None, ()),
        MetricDef("cyc_c", "{{ metric('cyc_b') }}", None, ()),
        MetricDef("self_ref", "{{ metric('self_ref') }}", None, ()),
        MetricDef("tail", "{{ metric('cyc_a') }} + {{ metric('missing') }}", None, ()),
    )
    assert _metric_cycles(metrics) == (
        ("cyc_a", "cyc_b", "cyc_a"),
        ("self_ref", "self_ref"),
        ("cyc_a", "cyc_c", "cyc_b", "cyc_a"),
    )


def test_metric_diagnostics_cover_unknown_columns_empty_tables_and_duplicates() -> None:
    model = DbtModel(
        unique_id="model.fixture.orders",
        name="orders",
        relation_name="DB.SCH.ORDERS",
        primary_key=("order_id",),
        unique_keys=(),
        columns=(DbtColumn("order_id", None, None, "dimension"),),
    )
    metrics = (
        MetricDef(
            "duplicate",
            "COUNT({{ ref('orders', 'missing') }})",
            None,
            (),
            ("orders",),
            False,
            (),
            (),
            has_tables_key=True,
        ),
        MetricDef("duplicate", "COUNT(1)", None, (), ("orders",), False, (), (), has_tables_key=True),
        MetricDef("empty", "COUNT(1)", None, ()),
    )
    assert [diagnostic.code for diagnostic in _metric_diagnostics(metrics, {"orders": model})] == [
        "SST-VAL001",
        "SST-VAL109",
    ]


def test_table_scoped_metrics_require_an_aggregate_expression() -> None:
    assert is_aggregate_expression("SUM({{ ref('orders', 'amount') }})")
    assert is_aggregate_expression("(COUNT(DISTINCT {{ ref('orders', 'id') }}))")
    assert not is_aggregate_expression("{{ ref('orders', 'amount') }}")
    assert not is_aggregate_expression("SUM({{ ref('orders', 'amount') }}) OVER (ORDER BY 1)")
    metric = MetricDef(
        "raw_amount",
        "{{ ref('orders', 'amount') }}",
        "Raw amount.",
        (),
        ("orders",),
        False,
        (),
        (),
        has_tables_key=True,
    )
    assert [diagnostic.code for diagnostic in _metric_diagnostics((metric,), {})] == [
        "SST-VAL101",
        "SST-REF001",
    ]


def test_dbt_column_and_key_validation_cover_semantic_metadata() -> None:
    model = DbtModel(
        unique_id="model.fixture.orders",
        name="orders",
        relation_name="DB.SCH.ORDERS",
        primary_key=("missing", "id"),
        unique_keys=(("id",),),
        columns=(
            DbtColumn("id", None, "VARCHAR", "dimension"),
            DbtColumn("amount", "Amount.", "VARCHAR", "fact"),
            DbtColumn("occurred_at", "Occurred at.", "VARCHAR", "time_dimension"),
            DbtColumn("untyped", "Untyped.", None, None),
            DbtColumn("unknown_type", "Unknown type.", None, "dimension"),
        ),
    )
    assert [diagnostic.code for diagnostic in _dbt_model_diagnostics({"orders": model})] == [
        "SST-VAL310",
        "SST-VAL223",
    ]
    assert [diagnostic.code for diagnostic in _dbt_column_diagnostics({"orders": model})] == [
        "SST-VAL003",
        "SST-VAL305",
        "SST-VAL306",
        "SST-VAL308",
        "SST-VAL309",
        "SST-VAL309",
    ]

    keyless = DbtModel("model.fixture.keyless", "keyless", "DB.SCH.KEYLESS", (), (), ())
    assert [diagnostic.code for diagnostic in _dbt_model_diagnostics({"keyless": keyless})] == [
        "SST-VAL312",
    ]

    legacy = DbtModel(
        "model.fixture.legacy", "legacy", "DB.SCH.LEGACY", ("id",), (), (DbtColumn("id", "Key.", "VARCHAR", None),)
    )
    legacy = dataclasses.replace(legacy, legacy_key_fields=("primary_key", "unique_keys"))
    assert [item.code for item in _dbt_model_diagnostics({"legacy": legacy})] == ["SST-DBT005", "SST-DBT005"]
    # Keys written in the 0.3 form are unread, so the model is keyless -- but that is
    # the same fault, and reporting it twice would bury the fix.
    unread = dataclasses.replace(legacy, primary_key=(), legacy_key_fields=("primary_key",))
    assert [item.code for item in _dbt_model_diagnostics({"legacy": unread})] == ["SST-DBT005"]
    assert _dbt_model_diagnostics({"legacy": legacy}, frozenset()) == ()


def test_derived_window_and_unattached_relationship_diagnostics() -> None:
    metric = MetricDef(
        "bad_window",
        "LAG({{ metric('order_count') }}) OVER (ORDER BY 1)",
        None,
        (),
        (),
        True,
    )
    assert [diagnostic.code for diagnostic in _metric_diagnostics((metric,), {})] == ["SST-VAL102", "SST-REF006"]
    relationship = Relationship("CUSTOMERS_TO_LOCATIONS", "CUSTOMERS", ("ID",), "LOCATIONS", ("ID",))
    diagnostics = _relationship_diagnostics(
        (relationship,),
        (("semantic_view:sales", frozenset(("customers", "orders"))),),
    )
    assert [(diagnostic.code, diagnostic.severity.name, diagnostic.caused_by) for diagnostic in diagnostics] == [
        ("SST-VAL205", "ERROR", None),
        ("SST-MEM005", "WARNING", "SST-VAL205"),
    ]


def test_relationship_target_key_must_cover_join_columns() -> None:
    relationship = Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("ID",))
    target = DbtModel(
        "model.fixture.customers",
        "customers",
        "DB.SCH.CUSTOMERS",
        ("OTHER_ID",),
        (),
        (DbtColumn("id", "Customer key.", "VARCHAR", "dimension"),),
    )
    diagnostics = _relationship_diagnostics(
        (relationship,),
        (("semantic_view:sales", frozenset(("orders", "customers"))),),
        models={"customers": target},
    )
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-VAL210"]

    covered = DbtModel(
        "model.fixture.customers",
        "customers",
        "DB.SCH.CUSTOMERS",
        (),
        (("id",),),
        target.columns,
    )
    assert not _relationship_diagnostics(
        (relationship,),
        (("semantic_view:sales", frozenset(("orders", "customers"))),),
        models={"customers": covered},
    )


def test_multi_path_warns_on_view_and_errors_on_unpinned_metric() -> None:
    relationships = (
        Relationship("PATH_A", "ORDER_ITEMS", ("ORDER_ID",), "ORDERS", ("ORDER_ID",)),
        Relationship("PATH_B", "ORDER_ITEMS", ("ORDER_ID",), "ORDERS", ("ORDER_ID",)),
    )
    metrics = (
        MetricDef("pinned", "COUNT(1)", None, (), ("order_items",), False, ("PATH_A",), (), has_tables_key=True),
        MetricDef("unpinned", "COUNT(1)", None, (), ("order_items",), False, (), (), has_tables_key=True),
    )
    diagnostics = _multipath_diagnostics(
        relationships,
        metrics,
        (("semantic_view:menu", frozenset(("order_items", "orders"))),),
    )
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-VAL209", "SST-VAL116"]


def test_expression_reference_diagnostics_cover_filters_and_verified_queries() -> None:
    model = DbtModel(
        unique_id="model.fixture.orders",
        name="orders",
        relation_name="DB.SCH.ORDERS",
        primary_key=("order_id",),
        unique_keys=(),
        columns=(DbtColumn("order_id", None, None, "dimension"),),
    )
    filter_def = FilterDef(
        "bad_filter",
        "{{ ref('orders', 'missing') }} = 1",
        None,
        ("orders",),
        True,
    )
    query = VerifiedQueryDef(
        "bad_query",
        "q",
        "SELECT * FROM {{ ref('missing') }}",
        ("missing",),
        None,
        None,
        None,
    )
    diagnostics = _expression_reference_diagnostics(
        (filter_def, query),
        {"orders": model},
        metric_names=frozenset(),
        variables={},
    )
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-REF002", "SST-REF001"]


def test_excluded_columns_are_diagnosed_in_metric_and_filter_references() -> None:
    model = DbtModel(
        "model.fixture.orders",
        "orders",
        "DB.SCH.ORDERS",
        ("id",),
        (),
        (DbtColumn("secret", "Secret.", "VARCHAR", "dimension", excluded=True),),
    )
    metric = MetricDef(
        "secret_count",
        "COUNT({{ ref('orders', 'secret') }})",
        "Secret count.",
        (),
        ("orders",),
        False,
        (),
        (),
        "public_access",
        True,
    )
    filter_def = FilterDef(
        "secret_filter",
        "{{ ref('orders', 'secret') }} IS NOT NULL",
        None,
        ("orders",),
        True,
    )

    assert "SST-VAL318" in {diagnostic.code for diagnostic in _metric_diagnostics((metric,), {"orders": model})}
    assert [
        diagnostic.code
        for diagnostic in _expression_reference_diagnostics(
            (filter_def,),
            {"orders": model},
            metric_names=frozenset(),
            variables={},
        )
    ] == ["SST-VAL318"]


def test_entity_filters_require_boolean_expressions() -> None:
    invalid = FilterDef("bad", "{{ ref('orders', 'amount') }} + 1", None, ("orders",), True)
    valid = tuple(
        FilterDef(f"good_{index}", expression, None, ("orders",), True)
        for index, expression in enumerate(
            (
                "{{ ref('orders', 'amount') }} > 1",
                "TRUE",
                "NOT {{ ref('orders', 'flag') }}",
                "COALESCE({{ ref('orders', 'flag') }}, FALSE)",
                "EXISTS (SELECT 1)",
                "REGEXP_LIKE({{ ref('orders', 'value') }}, 'x')",
            )
        )
    )
    assert [diagnostic.code for diagnostic in _filter_diagnostics((invalid, *valid))] == ["SST-VAL401"]


def test_unknown_metric_reference_is_a_diagnostic() -> None:
    metric = MetricDef("derived", "{{ metric('missing') }}", None, (), derived=True)
    diagnostics = _metric_diagnostics((metric,), {})
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-REF006"]


def test_metric_relationship_and_expression_diagnostics_use_public_codes() -> None:
    metric = MetricDef(
        "bad_path",
        "SUM(amount)",
        "Bad path.",
        (),
        ("orders",),
        False,
        ("MISSING", "SECOND"),
        (),
        "hidden",
        True,
    )
    orders_with_amount = DbtModel(
        "model.fixture.orders",
        "orders",
        "DB.SCH.ORDERS",
        ("id",),
        (),
        (DbtColumn("amount", "Amount.", "NUMBER", "fact"),),
    )
    codes = [
        diagnostic.code
        for diagnostic in _metric_diagnostics(
            (metric,),
            {"orders": orders_with_amount},
        )
    ]
    assert codes == ["SST-VAL115", "SST-VAL121", "SST-VAL110"]

    outside = MetricDef(
        "outside",
        "SUM({{ ref('customers', 'id') }})",
        "Outside.",
        (),
        ("orders",),
        False,
        (),
        (),
        "public_access",
        True,
    )
    orders = DbtModel("model.fixture.orders", "orders", "DB.SCH.ORDERS", ("id",), (), ())
    customers = DbtModel(
        "model.fixture.customers",
        "customers",
        "DB.SCH.CUSTOMERS",
        ("id",),
        (),
        (DbtColumn("id", "ID.", "VARCHAR", "dimension"),),
    )
    assert [
        diagnostic.code
        for diagnostic in _metric_diagnostics(
            (outside,),
            {"orders": orders, "customers": customers},
        )
    ] == ["SST-VAL112"]


def test_bare_identifier_warning_uses_manifest_columns_not_sql_tokens() -> None:
    metric = MetricDef(
        "variable_metric",
        "SUM(CAST({{ ref('orders', 'amount') }} AS NUMBER)) + threshold_value + raw_amount",
        "Variable metric.",
        (),
        ("orders",),
        False,
        (),
        (),
        "public_access",
        True,
    )
    orders = DbtModel(
        "model.fixture.orders",
        "orders",
        "DB.SCH.ORDERS",
        ("id",),
        (),
        (
            DbtColumn("id", "ID.", "VARCHAR", "dimension"),
            DbtColumn("amount", "Amount.", "NUMBER", "fact"),
            DbtColumn("raw_amount", "Raw amount.", "NUMBER", "fact"),
        ),
    )
    warnings = [
        diagnostic
        for diagnostic in _metric_diagnostics(
            (metric,),
            {"orders": orders},
            {"threshold_value": 1},
        )
        if diagnostic.code == "SST-VAL110"
    ]
    assert [diagnostic.context["column"] for diagnostic in warnings] == ["raw_amount"]


def test_critical_metric_restrictions_are_non_demotable_diagnostics() -> None:
    base = MetricDef(
        "base",
        "SUM({{ ref('orders', 'order_id') }})",
        None,
        (),
        ("orders",),
        False,
        (),
        (NonAdditiveDef("ordered_at", "orders"),),
        has_tables_key=True,
    )
    derived = MetricDef("derived", "{{ metric('base') }}", None, (), derived=True)
    metrics = (
        base,
        derived,
        MetricDef("agg_metric", "SUM({{ metric('base') }})", None, (), derived=True),
        MetricDef("physical", "{{ ref('orders', 'order_id') }}", None, (), derived=True),
        MetricDef("member", "{{ fact('orders.total') }}", None, (), derived=True),
        MetricDef(
            "regular_derived", "{{ metric('derived') }}", None, (), ("orders",), False, (), (), has_tables_key=True
        ),
        MetricDef("regular_nonadd", "{{ metric('base') }}", None, (), ("orders",), False, (), (), has_tables_key=True),
    )
    model = DbtModel(
        unique_id="model.fixture.orders",
        name="orders",
        relation_name="DB.SCH.ORDERS",
        primary_key=(),
        unique_keys=(),
        columns=(DbtColumn("order_id", None, None, "dimension"),),
    )
    codes = [diagnostic.code for diagnostic in _metric_diagnostics(metrics, {"orders": model})]
    assert {"SST-VAL103", "SST-VAL104", "SST-VAL105", "SST-VAL106", "SST-VAL107"} <= set(codes)


def _supplies() -> DbtModel:
    return DbtModel(
        unique_id="model.fixture.supplies",
        name="supplies",
        relation_name="DB.SCH.SUPPLIES",
        primary_key=("supply_id",),
        unique_keys=(),
        columns=(
            DbtColumn("supply_id", None, "VARCHAR", "dimension"),
            DbtColumn("snapshot_month", None, "TIMESTAMP_NTZ", "time_dimension"),
            DbtColumn("supply_cost", None, "NUMBER", "fact"),
            DbtColumn("retired_month", None, "TIMESTAMP_NTZ", "dimension", excluded=True),
        ),
    )


def test_non_additive_dimensions_resolve_to_a_dimension_of_their_table() -> None:
    def unresolved(*entries: NonAdditiveDef) -> list[object]:
        metric = MetricDef(
            "m", "SUM({{ ref('supplies', 'supply_cost') }})", None, (), ("supplies",), non_additive=entries
        )
        return [
            item.context["value"]
            for item in _metric_diagnostics((metric,), {"supplies": _supplies()})
            if item.code == "SST-VAL118"
        ]

    assert unresolved(NonAdditiveDef("snapshot_month"), NonAdditiveDef("SNAPSHOT_MONTH", "supplies", True, True)) == []
    assert unresolved(NonAdditiveDef("missing")) == ["missing"]
    # A fact is not a dimension, and an excluded column is not in the view.
    assert unresolved(NonAdditiveDef("supply_cost"), NonAdditiveDef("retired_month")) == [
        "supply_cost",
        "retired_month",
    ]
    assert unresolved(NonAdditiveDef("snapshot_month", "calendar")) == ["calendar.snapshot_month"]


SUPPLY_COST = "{{ ref('supplies', 'supply_cost') }}"
SNAPSHOT = "{{ ref('supplies', 'snapshot_month') }}"


def _window_codes(*metrics: MetricDef) -> list[tuple[str, str, object]]:
    other_model = dataclasses.replace(_supplies(), unique_id="model.fixture.products", name="products")
    diagnostics = _metric_diagnostics(metrics, {"supplies": _supplies(), "products": other_model})
    window_codes = ("SST-VAL101", "SST-VAL102", "SST-VAL129", "SST-VAL126", "SST-VAL127", "SST-VAL128")
    return [
        (item.code, item.context["metric"], item.context.get("field") or item.context.get("function"))
        for item in diagnostics
        if item.code in window_codes
    ]


def _supply_metric(name: str, expr: str, window: WindowDef | None = None, **changes: Any) -> MetricDef:
    metric = MetricDef(name, expr, None, (), ("supplies",), window=window)
    return dataclasses.replace(metric, **changes) if changes else metric


def test_a_well_formed_window_metric_is_clean() -> None:
    base = _supply_metric("supply_total", f"SUM({SUPPLY_COST})")
    window = WindowDef(
        partition_excluding=(SNAPSHOT,),
        order_by=(WindowOrderDef(SNAPSHOT, False, False), WindowOrderDef("{{ metric('supply_total') }}")),
        frame="ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    )
    assert _window_codes(base, _supply_metric("running", "AVG({{ metric('supply_total') }})", window)) == []
    # An aggregate is a valid argument too: SUM(SUM(x)) OVER (...) is a window metric.
    assert _window_codes(_supply_metric("nested", f"SUM(SUM({SUPPLY_COST}))", WindowDef())) == []


def test_a_window_must_apply_to_a_metric_or_an_aggregate() -> None:
    assert _window_codes(
        _supply_metric("row_level", f"SUM({SUPPLY_COST})", WindowDef()),
        _supply_metric("not_a_call", f"SUM({SUPPLY_COST}) / 2", WindowDef()),
    ) == [("SST-VAL126", "row_level", "SUM"), ("SST-VAL126", "not_a_call", "the expression")]


def test_window_entries_resolve_to_a_reachable_dimension_or_a_sibling_metric() -> None:
    derived = MetricDef("derived_total", "{{ metric('supply_total') }}", None, (), derived=True)
    elsewhere = MetricDef("product_total", f"SUM({SUPPLY_COST})", None, (), ("products",))
    windowed = _supply_metric("windowed", "SUM({{ metric('supply_total') }})", WindowDef())
    window = WindowDef(
        partition_by=(SUPPLY_COST, "{{ ref('missing', 'x') }}", "{{ ref('supplies', 'retired_month') }}"),
        partition_excluding=("{{ metric('supply_total') }}", "{{ ref('supplies' }}"),
        order_by=(
            WindowOrderDef("{{ metric('derived_total') }}"),
            WindowOrderDef("{{ metric('product_total') }}"),
            WindowOrderDef("{{ metric('windowed') }}"),
            WindowOrderDef("supplies.snapshot_month"),
        ),
    )
    found = _window_codes(
        _supply_metric("supply_total", f"SUM({SUPPLY_COST})"),
        derived,
        elsewhere,
        windowed,
        _supply_metric("bad_entries", "SUM({{ metric('supply_total') }})", window),
    )
    assert [field for code, metric, field in found if code == "SST-VAL129"] == [
        "partition_by[0]",
        "partition_by[1]",
        "partition_by[2]",
        "partition_by_excluding[0]",
        "partition_by_excluding[1]",
        "order_by[0]",
        "order_by[1]",
        "order_by[2]",
        "order_by[3]",
    ]


def test_window_placement_frame_and_reference_rules() -> None:
    base = _supply_metric("supply_total", f"SUM({SUPPLY_COST})")
    running = _supply_metric(
        "running", "SUM({{ metric('supply_total') }})", WindowDef(order_by=(WindowOrderDef(SNAPSHOT),))
    )
    found = _window_codes(
        base,
        running,
        dataclasses.replace(
            base,
            name="unordered_frame",
            expr="SUM({{ metric('supply_total') }})",
            window=WindowDef(frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
        ),
        MetricDef("derived_window", "SUM({{ metric('supply_total') }})", None, (), derived=True, window=WindowDef()),
        dataclasses.replace(running, name="two_tables", tables=("supplies", "products"), window=WindowDef()),
        MetricDef("uses_window", "{{ metric('running') }} + 1", None, (), derived=True),
        _supply_metric("regular_uses_window", "SUM({{ metric('running') }})"),
    )
    assert found == [
        ("SST-VAL127", "unordered_frame", None),
        ("SST-VAL102", "derived_window", "SUM"),
        ("SST-VAL102", "two_tables", "SUM"),
        ("SST-VAL128", "uses_window", None),
        ("SST-VAL128", "regular_uses_window", None),
    ]


def test_two_windows_over_one_expression_are_not_duplicates() -> None:
    base = _supply_metric("supply_total", f"SUM({SUPPLY_COST})")
    weekly = _supply_metric(
        "weekly",
        "SUM({{ metric('supply_total') }})",
        WindowDef(order_by=(WindowOrderDef(SNAPSHOT),), frame="ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"),
    )
    monthly = dataclasses.replace(
        weekly,
        name="monthly",
        window=WindowDef(order_by=(WindowOrderDef(SNAPSHOT),), frame="ROWS BETWEEN 29 PRECEDING AND CURRENT ROW"),
    )
    diagnostics = _metric_diagnostics((base, weekly, monthly), {"supplies": _supplies()})
    assert [item.code for item in diagnostics if item.code == "SST-VAL124"] == []


def test_the_same_sum_at_another_snapshot_is_not_a_duplicate_metric() -> None:
    latest = MetricDef(
        "latest",
        "SUM({{ ref('supplies', 'supply_cost') }})",
        None,
        (),
        ("supplies",),
        non_additive=(NonAdditiveDef("snapshot_month"),),
    )
    earliest = dataclasses.replace(
        latest, name="earliest", non_additive=(NonAdditiveDef("snapshot_month", descending=True),)
    )
    twin = dataclasses.replace(latest, name="twin")
    diagnostics = _metric_diagnostics((latest, earliest, twin), {"supplies": _supplies()})
    assert [(item.code, item.context["metric"]) for item in diagnostics if item.code == "SST-VAL124"] == [
        ("SST-VAL124", "twin")
    ]


def test_relationship_targets_need_a_key_and_the_graph_no_cycle() -> None:
    keyless = DbtModel("model.fixture.regions", "regions", "DB.SCH.REGIONS", (), (), ())
    to_regions = Relationship("LOCATIONS_TO_REGIONS", "LOCATIONS", ("REGION_ID",), "REGIONS", ("REGION_ID",))
    diagnostics = _relationship_diagnostics(
        (to_regions,), (("semantic_view:geo", frozenset(("locations", "regions"))),), models={"regions": keyless}
    )
    assert [(item.code, item.context["name"]) for item in diagnostics] == [("SST-VAL311", "regions")]

    back = Relationship("REGIONS_TO_LOCATIONS", "REGIONS", ("HQ_ID",), "LOCATIONS", ("LOCATION_ID",))
    itself = Relationship("LOCATIONS_TO_LOCATIONS", "LOCATIONS", ("PARENT_ID",), "LOCATIONS", ("LOCATION_ID",))
    views = (
        ("semantic_view:loop", frozenset(("locations", "regions"))),
        ("semantic_view:self", frozenset(("locations",))),
        ("semantic_view:acyclic", frozenset(("regions", "orders"))),
    )
    cycles = _relationship_cycle_diagnostics((to_regions, back, itself), views)
    assert [(item.subject, item.context["cycle"]) for item in cycles] == [
        ("semantic_view:loop", "locations -> locations"),
        ("semantic_view:self", "locations -> locations"),
    ]
    assert _relationship_cycle_diagnostics((to_regions, back), views[:1])[0].context["cycle"] == (
        "locations -> regions -> locations"
    )


def test_verified_query_tables_skip_ctes_and_string_literals() -> None:
    from snowflake_semantic_tools.adapters.yaml.semantic.checks.expressions import _sql_tables

    sql = (
        "WITH flow AS (SELECT * FROM orders WHERE source IN ('Join Flow')),\n"
        "     recent AS (SELECT * FROM flow)\n"
        "SELECT * FROM recent JOIN customers ON TRUE\n"
        "-- FROM commented_out\n"
    )
    assert _sql_tables(sql) == ("orders", "customers")
