"""Connected validation types each column an expression reads, and reports a member's finding once.

The fake warehouse describes tables as a test lists them and refuses the statements Snowflake
would, so these tests pin what SST sends: the typed projection each EXPLAIN reads, where each
type comes from, and how a finding every view repeats becomes one.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.explain_projection import column_types, describe_columns, typed_null
from snowflake_semantic_tools.app.validate import ValidateArtifacts, _once_per_member
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    SemanticView,
    Table,
    Variable,
    VerifiedQuery,
)
from snowflake_semantic_tools.domain.sql import datatype
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import coded
from tests.helpers.projects import load_project
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, authored_query, statement
from tests.helpers.val_codes import TypedWarehouse

MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"
ORDERS = (("ORDER_ID", "VARCHAR(16777216)"), ("ORDERED_AT", "TIMESTAMP_NTZ(9)"), ("ORDER_TOTAL", "NUMBER(38,2)"))
TYPE_ERROR = (
    "002139 (42000): {qid}: SQL compilation error: Invalid argument types for function 'SUM': (TIMESTAMP_NTZ(9))"
)


def _view(name: str = "SALES", *, metric: str = "SUM(ORDERS.ORDERED_AT)", data_type: str | None = None) -> SemanticView:
    return SemanticView(
        f"DB.S.{name}",
        (Table("ORDERS", "DB.S.ORDERS"),),
        columns=(
            Column("ORDERS", "ORDERED_AT", ColumnKind.DIMENSION, authored("ORDERS.ORDERED_AT"), data_type=data_type),
        ),
        metrics=(Metric("LATEST", authored(metric), "ORDERS"),),
        member_origins=MappingProxyType({"metric:latest": Origin("semantic_models/metrics.yml", 4, 5)}),
    )


def _validated(port: FakeSnowflake, *views: SemanticView) -> list[Diagnostic]:
    compiled = CompileResult(tuple(CompiledView(view, statement(f"CREATE SEMANTIC VIEW {view.fqn}")) for view in views))
    return list(ValidateArtifacts(port, clock=FixedClock()).run(compiled, strict=False, connected=True).diagnostics)


def _explained(port: FakeSnowflake) -> list[str]:
    return [query for query, _params in port.queries if query.startswith("EXPLAIN")]


def test_a_described_column_is_projected_as_a_null_of_its_type() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS})
    assert _validated(port, _view()) == []
    assert [query for query, _params in port.queries][0] == "DESCRIBE TABLE DB.S.ORDERS"
    assert _explained(port) == [
        'EXPLAIN SELECT SUM("ORDERS.ORDERED_AT") FROM (SELECT NULL AS ORDERED_AT, '
        'CAST(NULL AS TIMESTAMP_NTZ(9)) AS "ORDERS.ORDERED_AT" WHERE FALSE) AS SST_VALIDATE',
        'EXPLAIN SELECT "ORDERS.ORDERED_AT" FROM (SELECT NULL AS ORDERED_AT, '
        'CAST(NULL AS TIMESTAMP_NTZ(9)) AS "ORDERS.ORDERED_AT" WHERE FALSE) AS SST_VALIDATE',
    ]


def test_an_argument_type_snowflake_refuses_is_sst_val418_at_the_members_origin() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS}, {'SUM("ORDERS.ORDERED_AT")': TYPE_ERROR})
    [found] = coded(_validated(port, _view()), "SST-VAL418")
    assert found.message == (
        "metric 'ORDERS.LATEST' is not valid SQL: 002139 (42000): 01bc0000-0000-0000-0000-000000000001: "
        "SQL compilation error: Invalid argument types for function 'SUM': (TIMESTAMP_NTZ(9)) (in sales)"
    )
    assert found.origin == Origin("semantic_models/metrics.yml", 4, 5)
    assert found.subject == "semantic_view:sales"
    # Untyped, as before, the same reference reads SUM(NULL), which Snowflake compiles.
    untyped = TypedWarehouse({}, {'SUM("ORDERS.ORDERED_AT")': TYPE_ERROR})
    assert coded(_validated(untyped, _view()), "SST-VAL418") == []
    assert _explained(untyped)[0].startswith("EXPLAIN SELECT SUM(NULL) FROM")


def test_a_table_the_target_cannot_describe_is_typed_as_the_manifest_declares() -> None:
    port = TypedWarehouse({})
    _validated(port, _view(data_type="timestamp_ntz"))
    assert 'CAST(NULL AS timestamp_ntz) AS "ORDERS.ORDERED_AT"' in _explained(port)[0]


def test_the_described_type_wins_over_the_declared_one() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS})
    _validated(port, _view(data_type="NUMBER(38,0)"))
    assert 'CAST(NULL AS TIMESTAMP_NTZ(9)) AS "ORDERS.ORDERED_AT"' in _explained(port)[0]


def test_each_table_is_described_once_however_many_views_read_it() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS})
    _validated(port, _view("MENU"), _view("SALES"))
    assert [query for query, _params in port.queries if query.startswith("DESCRIBE")] == ["DESCRIBE TABLE DB.S.ORDERS"]


@pytest.mark.parametrize(
    ("described", "projected"),
    [
        ("NUMBER(38,2)", "CAST(NULL AS NUMBER(38,2))"),
        ("TIMESTAMP_TZ(9)", "CAST(NULL AS TIMESTAMP_TZ(9))"),
        ("VARIANT", "CAST(NULL AS VARIANT)"),
        ("VARCHAR(10) COLLATE 'en-ci'", "CAST(NULL AS VARCHAR(10))"),
        ("ARRAY(NUMBER(38,0))", "CAST(NULL AS ARRAY)"),
        ("OBJECT(a NUMBER(38,0))", "CAST(NULL AS OBJECT)"),
        ("VECTOR(FLOAT, 3)", "CAST(NULL AS VECTOR(FLOAT, 3))"),
        ("GEOGRAPHY", "TO_GEOGRAPHY(CAST(NULL AS VARCHAR))"),
        ("geometry", "TO_GEOMETRY(CAST(NULL AS VARCHAR))"),
        ("MAP(VARCHAR, NUMBER)", None),
        ("NUMBER); DROP TABLE x", None),
    ],
)
def test_a_type_is_written_as_a_null_snowflake_compiles(described: str, projected: str | None) -> None:
    null = typed_null(described)
    assert (str(null) if null is not None else None) == projected


def test_a_describe_snowflake_answers_unexpectedly_types_nothing() -> None:
    port = FakeSnowflake(query_results=(QueryResult(("column_name",), (("X",),)),))
    assert describe_columns(port, "DB.S.ORDERS") is None
    short = FakeSnowflake(query_results=(QueryResult(("name", "type"), (("X", "DATE"), ("Y",))),))
    assert describe_columns(short, "DB.S.ORDERS") == {"X": "DATE"}


def test_a_metric_filter_variable_or_untyped_name_stays_an_untyped_null() -> None:
    view = SemanticView(
        "DB.S.SALES",
        (Table("ORDERS", "DB.S.ORDERS"),),
        variables=(Variable("FLOOR", datatype("NUMBER"), statement("1")),),
        columns=(
            Column("ORDERS", "ORDER_TOTAL", ColumnKind.FACT, authored("ORDERS.ORDER_TOTAL"), data_type="NUMBER"),
            Column("ORDERS", "BIG", ColumnKind.FILTER, authored("ORDERS.ORDER_TOTAL > 100")),
        ),
        metrics=(
            # ORDER_TOTAL is also a metric's name, bare: the swapped-in column name is not rewritten again.
            Metric("ORDER_TOTAL", authored("SUM(ORDERS.ORDER_TOTAL) + FLOOR + order_total"), None),
            Metric("BIG", authored("COUNT(ORDERS.BIG) + ORDERS.MISSING"), "ORDERS"),
        ),
    )
    types = column_types(view, {"DB.S.ORDERS": {"ORDER_TOTAL": "NUMBER(38,2)", "BIG": "BOOLEAN"}})
    assert {key: str(value) for key, value in types.items()} == {"ORDERS.ORDER_TOTAL": "CAST(NULL AS NUMBER(38,2))"}
    port = TypedWarehouse({"DB.S.ORDERS": (("ORDER_TOTAL", "NUMBER(38,2)"), ("BIG", "BOOLEAN"))})
    assert _validated(port, view) == []
    projection = 'FROM (SELECT NULL AS BIG, NULL AS ORDER_TOTAL, CAST(NULL AS NUMBER(38,2)) AS "ORDERS.ORDER_TOTAL"'
    assert _explained(port)[:2] == [
        f'EXPLAIN SELECT SUM("ORDERS.ORDER_TOTAL") + NULL + NULL {projection} WHERE FALSE) AS SST_VALIDATE',
        "EXPLAIN SELECT COUNT(NULL) + NULL FROM (SELECT NULL AS BIG, NULL AS ORDER_TOTAL WHERE FALSE) AS SST_VALIDATE",
    ]


def test_a_partition_by_excluding_list_collapses_whatever_its_references_type() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS})
    view = _view(metric="SUM(ORDERS.ORDER_TOTAL) OVER (PARTITION BY EXCLUDING ORDERS.ORDERED_AT, orders . order_id)")
    _validated(port, view)
    assert _explained(port)[0].startswith(
        'EXPLAIN SELECT SUM("ORDERS.ORDER_TOTAL") OVER (PARTITION BY NULL) FROM (SELECT NULL AS ORDERED_AT, '
        'CAST(NULL AS NUMBER(38,2)) AS "ORDERS.ORDER_TOTAL" WHERE FALSE)'
    )


def test_a_finding_every_view_repeats_is_reported_once_naming_them() -> None:
    port = TypedWarehouse({"DB.S.ORDERS": ORDERS}, {'SUM("ORDERS.ORDERED_AT")': TYPE_ERROR})
    [found] = coded(_validated(port, _view("MENU"), _view("SALES")), "SST-VAL418")
    assert found.subject == "semantic_view:menu"
    assert found.context["view"] == "menu, sales"
    assert str(found.context["detail"]).endswith("(TIMESTAMP_NTZ(9)) (in menu, sales)")


def test_a_verified_query_two_views_hold_is_reported_once_when_it_returns_nothing() -> None:
    query = VerifiedQuery("orders_today", "How many orders today?", authored_query("SELECT COUNT(*) FROM DB.S.ORDERS"))
    views = [replace(_view(name, metric="COUNT(*)"), verified_queries=(query,)) for name in ("MENU", "SALES")]
    [found] = coded(_validated(TypedWarehouse({"DB.S.ORDERS": ORDERS}), *views), "SST-VAL415")
    assert found.message == "verified_query 'orders_today' executed and returned 0 rows in 1ms (in menu, sales)"
    assert found.subject == "semantic_view:menu"


def test_findings_that_differ_beyond_view_query_id_and_timing_stay_apart() -> None:
    def failed(view: str, detail: str, name: str = "ORDERS.LATEST") -> Diagnostic:
        return D("SST-VAL418", type="metric", name=name, detail=detail, view=view, subject=f"semantic_view:{view}")

    other = D("SST-VAL212", relationship="r", found="many-to-one", value="1 rows of 'orders' repeat a join key")
    merged = _once_per_member(
        [
            failed("menu", "01bc0000-0000-0000-0000-000000000001: bad"),
            other,
            failed("sales", "01bc0000-0000-0000-0000-000000000002: bad"),
            failed("sales", "worse"),
            failed("sales", "bad", name="ORDERS.OTHER"),
        ]
    )
    assert [(item.code, item.context.get("detail")) for item in merged] == [
        ("SST-VAL418", "01bc0000-0000-0000-0000-000000000001: bad (in menu, sales)"),
        ("SST-VAL212", None),
        ("SST-VAL418", "worse (in sales)"),
        ("SST-VAL418", "bad (in sales)"),
    ]


def test_the_loader_records_declared_types_and_member_origins(tmp_path: Path) -> None:
    loaded = load_project(project_copy(tmp_path), manifest_path=MANIFEST)
    view = next(view for view in loaded.views if any(metric.name == "ORDER_COUNT" for metric in view.metrics))
    ordered_at = next(column for column in view.columns if column.qualified_name == "ORDERS.ORDERED_AT")
    assert ordered_at.data_type == "TIMESTAMP_NTZ"
    origin = view.member_origins["metric:order_count"]
    assert (origin.file, origin.line) == ("semantic_models/metrics/metrics.yml", 12)
