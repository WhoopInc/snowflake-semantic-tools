"""Focused checks for the Jaffle Sales authoring surface."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.loader import (
    FilterDef,
    MetricDef,
    VerifiedQueryDef,
    _dbt_column_diagnostics,
    _dbt_model_diagnostics,
    _expression_reference_diagnostics,
    _filter_diagnostics,
    _metric_cycles,
    _metric_diagnostics,
    _multipath_diagnostics,
    _relationship_diagnostics,
    load_semantic_views,
)
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.expression import is_aggregate_expression
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, SemanticView

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


@pytest.fixture(scope="module")
def sales() -> SemanticView:
    views = load_semantic_views(FIXTURE, manifest_path=MANIFEST, invoke_dbt=False)
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
    assert [(variable.name, variable.data_type, variable.default) for variable in sales.variables] == [
        ("LARGE_ORDER_CENTS", "NUMBER", "1000"),
        ("TAX_INCLUSIVE", "BOOLEAN", "FALSE"),
    ]
    large_filter = next(column for column in sales.columns if column.name == "IS_LARGE_ORDER")
    assert large_filter.expr == "ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS"


def test_resolves_project_vars_and_derived_metric_dependencies(sales: SemanticView) -> None:
    returned = next(metric for metric in sales.metrics if metric.name == "RETURNED_ORDER_COUNT")
    per_customer = next(metric for metric in sales.metrics if metric.name == "REVENUE_PER_CUSTOMER")
    assert "'returned'" in returned.expr
    assert per_customer.expr == "DIV0(ORDERS.TOTAL_REVENUE, CUSTOMERS.CUSTOMER_COUNT)"


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
        MetricDef("duplicate", "COUNT({{ ref('orders', 'missing') }})", None, (), ("orders",), False, (), (), True),
        MetricDef("duplicate", "COUNT(1)", None, (), ("orders",), False, (), (), True),
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
        ("SST-VAL203", "ERROR", None),
        ("SST-MEM005", "WARNING", "SST-VAL203"),
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
        MetricDef("pinned", "COUNT(1)", None, (), ("order_items",), False, ("PATH_A",), (), True),
        MetricDef("unpinned", "COUNT(1)", None, (), ("order_items",), False, (), (), True),
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
        ("orders.ordered_at",),
        True,
    )
    derived = MetricDef("derived", "{{ metric('base') }}", None, (), derived=True)
    metrics = (
        base,
        derived,
        MetricDef("agg_metric", "SUM({{ metric('base') }})", None, (), derived=True),
        MetricDef("physical", "{{ ref('orders', 'order_id') }}", None, (), derived=True),
        MetricDef("member", "{{ fact('orders.total') }}", None, (), derived=True),
        MetricDef("regular_derived", "{{ metric('derived') }}", None, (), ("orders",), False, (), (), True),
        MetricDef("regular_nonadd", "{{ metric('base') }}", None, (), ("orders",), False, (), (), True),
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


def test_verified_query_tables_skip_ctes_and_string_literals() -> None:
    from snowflake_semantic_tools.adapters.yaml.loader import _endpoint, _sql_tables

    sql = (
        "WITH flow AS (SELECT * FROM orders WHERE source IN ('Join Flow')),\n"
        "     recent AS (SELECT * FROM flow)\n"
        "SELECT * FROM recent JOIN customers ON TRUE\n"
        "-- FROM commented_out\n"
    )
    assert _sql_tables(sql) == ("orders", "customers")
    assert _endpoint("{{ ref('orders') }}") == "orders"
    assert _endpoint(" orders ") == "orders"
