"""SST-VAL419: Snowflake cannot read a published sample value as its column's type.

Connected validation casts each table's sample values, in one statement, to the types `DESCRIBE
TABLE` reports, as Snowflake casts them when it creates the view; the fake answers that statement
by refusing the values a test names, as `TRY_CAST` would.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.diagnostic_filters import coded
from tests.helpers.sql_values import authored
from tests.helpers.val_codes import TypedWarehouse

_ROW = re.compile(r"\((\d+), '((?:[^']|'')*)'\)")


class CastingWarehouse(TypedWarehouse):
    """Answers a sample-value cast with the rows whose value `refused` lists, as `TRY_CAST` would."""

    def __init__(
        self, tables: Mapping[str, tuple[tuple[str, str], ...]], refused: frozenset[str], *, fails: bool = False
    ) -> None:
        super().__init__(tables)
        self._refused_values = refused
        self._fails = fails
        self.casts: list[str] = []

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        if not text.startswith("SELECT K, V FROM (VALUES"):
            return super().query(sql, params)
        self.casts.append(text)
        if self._fails:
            raise SnowflakePortError("002003 (02000): SQL compilation error: something")
        rows = tuple(
            (int(index), value.replace("''", "'"))
            for index, value in _ROW.findall(text)
            if value.replace("''", "'") in self._refused_values
        )
        return QueryResult(("K", "V"), rows)


def _column(name: str, data_type: str, *values: str) -> Column:
    return Column(
        "ORDERS",
        name,
        ColumnKind.DIMENSION,
        authored(f"ORDERS.{name}"),
        sample_values=values,
        data_type=data_type,
    )


def _run(port: CastingWarehouse, *columns: Column, views: tuple[str, ...] = ("SALES",)) -> list[Diagnostic]:
    compiled = CompileResult(
        tuple(
            CompiledView(view, render(view))
            for view in (
                SemanticView(f"DB.S.{name}", (Table("ORDERS", "DB.S.ORDERS"),), columns=columns) for name in views
            )
        )
    )
    return coded(ValidateArtifacts(port).run(compiled, strict=False, connected=True).diagnostics, "SST-VAL419")


TABLES = {"DB.S.ORDERS": (("ENDED_AT", "TIMESTAMP_NTZ(9)"), ("AMOUNT", "FLOAT"), ("STATUS", "VARCHAR(16)"))}


def test_sst_val419_fires() -> None:
    port = CastingWarehouse(TABLES, frozenset({"2024-13-45"}))
    [found] = _run(port, _column("ENDED_AT", "timestamp_ntz", "2024-01-01", "2024-13-45"))
    assert found.severity is Severity.ERROR
    assert found.message == (
        "model 'orders': column 'ended_at' has sample value '2024-13-45', which Snowflake cannot read as "
        "TIMESTAMP_NTZ(9), so creating sales would fail"
    )
    assert found.subject == "dbt_column:orders.ended_at"


def test_sst_val419_casts_to_the_described_type_not_a_stale_declared_one() -> None:
    port = CastingWarehouse(TABLES, frozenset({"0,2"}))
    [found] = _run(port, _column("AMOUNT", "TEXT", "0,2", "1.5"))
    assert found.context["type"] == "FLOAT"
    assert "TRY_CAST(V AS FLOAT)" in port.casts[0]


def test_sst_val419_once_for_a_column_two_views_hold_in_one_read_per_table() -> None:
    port = CastingWarehouse(TABLES, frozenset({"NaT", "0,2"}))
    found = _run(
        port,
        _column("ENDED_AT", "TIMESTAMP_NTZ", "NaT"),
        _column("AMOUNT", "FLOAT", "0,2"),
        views=("MENU", "SALES"),
    )
    assert [item.context["value"] for item in found] == ["0,2", "NaT"]
    assert {item.context["view"] for item in found} == {"menu, sales"}
    assert len(port.casts) == 1


def test_sst_val419_silent() -> None:
    port = CastingWarehouse(TABLES, frozenset())
    assert _run(port, _column("ENDED_AT", "TIMESTAMP_NTZ", "2024-01-01")) == []


def test_sst_val419_silent_on_a_text_column_which_reads_any_value() -> None:
    port = CastingWarehouse(TABLES, frozenset({"NaT"}))
    assert _run(port, _column("STATUS", "VARCHAR", "NaT")) == []
    assert port.casts == []


def test_sst_val419_silent_when_the_cast_itself_is_refused() -> None:
    port = CastingWarehouse(TABLES, frozenset({"NaT"}), fails=True)
    assert _run(port, _column("ENDED_AT", "TIMESTAMP_NTZ", "NaT")) == []


def test_sst_val419_skipped_offline() -> None:
    view = SemanticView("DB.S.SALES", (Table("ORDERS", "DB.S.ORDERS"),), columns=(_column("AMOUNT", "FLOAT", "x"),))
    compiled = CompileResult((CompiledView(view, render(view)),))
    result = ValidateArtifacts(CastingWarehouse(TABLES, frozenset())).run(compiled, strict=False, connected=False)
    skipped = [item.context["rule_id"] for item in result.diagnostics if item.code == "SST-VAL020"]
    assert "SST-VAL419" in skipped
