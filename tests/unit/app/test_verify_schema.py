"""`verify_columns`: the columns a compiled view reads, looked up once per table in the warehouse."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.verify_schema import _plain_reference, _references, _unquoted, verify_columns
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind
from snowflake_semantic_tools.domain.sql.authored import guard_expression
from tests.helpers.cli_projects import DBT_MANIFEST, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake


def _column(expr: str, kind: ColumnKind = ColumnKind.DIMENSION) -> Column:
    return Column("ORDERS", "X", kind, guard_expression(expr))


def test_only_a_plain_reference_is_a_column_to_look_up() -> None:
    assert _plain_reference(_column("ORDERS.ORDER_ID")) == ("ORDERS", "ORDER_ID")
    assert _plain_reference(_column('"Orders"."Order Id"')) == ("Orders", "Order Id")
    assert _plain_reference(_column("SUM(ORDERS.AMOUNT)")) is None
    assert _plain_reference(_column("ORDERS.STATE = 'x'", ColumnKind.FILTER)) is None
    assert _unquoted('"a""b"') == 'a"b'


def _tables(project: Path) -> tuple[CompileResult, dict[str, tuple[tuple[str, str], ...]]]:
    from snowflake_semantic_tools.adapters.locations import locate_project

    compiled = compiling.compile_result(locate_project(project), None, DBT_MANIFEST)
    tables: dict[str, set[str]] = {}
    for item in compiled.compiled:
        if isinstance(item, CompiledView):
            for fqn, _model, column in _references(item):
                tables.setdefault(fqn, set()).add(column.upper())
    return compiled, {fqn: tuple((name, "TEXT") for name in sorted(names)) for fqn, names in tables.items()}


def test_verify_columns_reports_absent_tables_and_columns_only(tmp_path: Path) -> None:
    compiled, tables = _tables(project_copy(tmp_path))
    port = RecordedSnowflake()
    port.tables = {QualifiedName.parse(fqn).sql: columns for fqn, columns in tables.items()}
    assert verify_columns(port, compiled) == ()
    first = sorted(tables)[0]
    port.tables[QualifiedName.parse(first).sql] = tables[first][1:]
    second = sorted(tables)[1]
    del port.tables[QualifiedName.parse(second).sql]
    found = verify_columns(port, compiled)
    codes = sorted({item.code for item in found})
    assert codes == ["SST-PRT005", "SST-VAL325"]
    [absent] = [item for item in found if item.code == "SST-PRT005"]
    assert absent.context["value"] == second
    assert any(item.context["column"].upper() == tables[first][0][0] for item in found if item.code == "SST-VAL325")
