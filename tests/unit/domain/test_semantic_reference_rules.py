"""The semantic reference and dbt rules called directly: template calls, filter shape, models, columns."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.authored import FilterDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.validate.semantic.dbt import (
    dbt_column_diagnostics,
    dbt_model_diagnostics,
    description_diagnostics,
)
from snowflake_semantic_tools.domain.validate.semantic.expressions import (
    bare_column_identifiers,
    expression_reference_diagnostics,
    filter_diagnostics,
    scan_expression,
    sql_tables,
)
from tests.helpers.authored_documents import ORIGIN
from tests.helpers.semantic_members import BALANCES, MODELS, ORDERS, column


def _filter(
    expr: str, *, tables: tuple[str, ...] = ("orders",), entity: bool = True, labeled: bool = True
) -> FilterDef:
    return FilterDef("f", expr, None, tables, entity, origin=ORIGIN, labeled=labeled)


def _query(sql: str, tables: tuple[str, ...] = ("orders",)) -> VerifiedQueryDef:
    return VerifiedQueryDef("q", "?", sql, tables, None, None, None, origin=ORIGIN)


def test_each_template_call_of_a_filter_or_query_is_checked_by_its_function() -> None:
    members = (
        _filter("{{ ref('orders', 'state') }} = '{{ var('done') }}' AND {{ table('orders') }} IS NOT NULL"),
        _filter("{{ metric('m') }} > 0"),
        _filter("{{ ref('nothing', 'x') }} OR {{ ref('orders', 'gone') }} OR {{ ref('orders', 'secret') }}"),
        _filter("{{ var('missing') }} OR {{ var('empty') }}"),
        _filter("{{ ref('orders'", tables=()),
        _query("SELECT {{ metric('m') }}, {{ metric('nope') }} FROM {{ ref('balances') }} JOIN items"),
        _query("SELECT 1 FROM {{ ref('balances') }}", tables=()),
    )
    found = expression_reference_diagnostics(
        members, dict(MODELS), metric_names=frozenset(("m",)), variables={"done": "complete", "empty": ""}
    )
    assert [item.code for item in found] == [
        "SST-REF041",
        "SST-REF001",
        "SST-REF002",
        "SST-VAL318",
        "SST-CFG029",
        "SST-REF009",
        "SST-LOD004",
        "SST-VAL414",
        "SST-REF006",
        "SST-REF043",
    ]
    assert found[7].context["relation"] == "items"
    named = expression_reference_diagnostics(
        (_query("SELECT 1 FROM balances"),), dict(MODELS), metric_names=frozenset(), variables={}
    )
    assert [item.context["relation"] for item in named] == ["DB.S.BALANCES"]


def test_a_template_that_does_not_parse_is_named_by_its_file() -> None:
    found = scan_expression("{{ ref('x'", None, "filter:f")
    assert not isinstance(found, tuple) and found.origin is not None and found.origin.file == "<expression>"
    assert scan_expression("{{ ref('x') }}", ORIGIN, "filter:f") != ()


def test_a_filter_is_boolean_when_labelled_and_labelled_when_boolean_and_names_no_column_bare() -> None:
    filters = (
        _filter("{{ ref('orders', 'total') }}"),
        _filter("{{ ref('orders', 'total') }} > 0", entity=False, labeled=False),
        _filter("{{ ref('orders', 'total') }} > 0", entity=False, labeled=True),
        _filter("state = 'x' AND STATE <> 'state' AND total(1) = \"total\" AND cost > threshold"),
    )
    found = filter_diagnostics(filters, MODELS, {"Cost": 1})
    assert [(item.code, item.context.get("column")) for item in found] == [
        ("SST-VAL401", None),
        ("SST-VAL405", None),
        ("SST-VAL404", "state"),
        ("SST-VAL404", "STATE"),
    ]
    assert bare_column_identifiers("total", ("nowhere",), MODELS, {}) == ()


def test_a_verified_query_reads_the_tables_it_names_after_from_or_join_except_its_ctes() -> None:
    sql = (
        "-- FROM ignored\n"
        "WITH recent AS (SELECT 1), other AS (SELECT 2)\n"
        "SELECT * FROM Orders JOIN recent ON TRUE JOIN items WHERE x IN ('Join Flow') JOIN orders"
    )
    assert sql_tables(sql) == ("orders", "items")


def test_every_column_a_view_uses_declares_its_role_type_samples_and_description() -> None:
    columns = (
        column("no_role", "NUMBER", None),
        column("no_type", None, "dimension"),  # type: ignore[arg-type]
        column("text_fact", "VARCHAR", "fact"),
        column("text_time", "VARCHAR", "time_dimension"),
        column("enum", "VARCHAR", "dimension", is_enum=True),
        column("many", "VARCHAR", "dimension", sample_values=("a", "b", "c", "d", "e")),
        column("many_facts", "NUMBER", "fact", sample_values=("1", "2", "3", "4", "5")),
        replace(column("bare", "VARCHAR", "dimension"), description=None),
        column(
            "private",
            "VARCHAR",
            "dimension",
            access_modifier="private_access",
            unknown_meta_keys=("access_modifier", "odd"),
        ),
        column("typed", "NUMBER", "fact", declared_data_type="FLOAT", access_modifier="private_access"),
        column("sentinel", "VARCHAR", "dimension", sample_values=("NaN", "ok"), is_enum=False),
    )
    model = DbtModel("model.t.wide", "wide", "DB.S.WIDE", ("no_role",), (), columns)
    found = dbt_column_diagnostics({"wide": model})
    assert [(item.code, item.subject.rsplit(".", 1)[1]) for item in found] == [  # type: ignore[union-attr]
        ("SST-VAL308", "no_role"),
        ("SST-VAL309", "no_type"),
        ("SST-VAL305", "text_fact"),
        ("SST-VAL306", "text_time"),
        ("SST-VAL314", "enum"),
        ("SST-VAL315", "many"),
        ("SST-VAL003", "bare"),
        ("SST-VAL222", "private"),
        ("SST-PRS004", "private"),
        ("SST-DBT004", "typed"),
        ("SST-VAL316", "sentinel"),
    ]
    unused = dbt_column_diagnostics({"wide": model}, frozenset(("orders",)))
    assert [item.code for item in unused] == ["SST-VAL316"]


def test_every_model_a_view_uses_declares_valid_keys_in_the_1_0_form() -> None:
    legacy = replace(ORDERS, primary_key=(), legacy_key_fields=("primary_key",), forbidden_location_keys=("database",))
    keyless = replace(BALANCES, name="keyless", primary_key=(), unique_keys=(), unknown_meta_keys=("odd",))
    crossed = replace(
        BALANCES, name="crossed", primary_key=("account_id", "ghost"), unique_keys=(("ACCOUNT_ID",), ("as_of",))
    )
    unused = replace(BALANCES, name="unused", primary_key=(), forbidden_location_keys=("schema",))
    models = {"orders": legacy, "keyless": keyless, "crossed": crossed, "unused": unused}
    found = dbt_model_diagnostics(models, frozenset(("orders", "keyless", "crossed")))
    assert [(item.code, item.subject) for item in found] == [
        ("SST-VAL310", "dbt_model:crossed"),
        ("SST-VAL223", "dbt_model:crossed"),
        ("SST-PRS004", "dbt_model:keyless"),
        ("SST-VAL312", "dbt_model:keyless"),
        ("SST-DBT030", "dbt_model:orders"),
        ("SST-DBT032", "dbt_model:orders"),
        ("SST-DBT030", "dbt_model:unused"),
    ]
    assert len(dbt_model_diagnostics(models)) == len(found) + 1


def test_each_view_and_metric_needs_a_description() -> None:
    views = (
        ParsedView("v", ORIGIN, "v.yml", {"description": "  "}, ()),
        ParsedView("w", ORIGIN, "v.yml", {"description": "Use this view."}, ()),
    )
    metrics = (MetricDef("m", "1", None, ()), MetricDef("n", "1", "Counted.", ()))
    assert [item.subject for item in description_diagnostics(views, metrics)] == ["semantic_view:v", "metric:m"]
