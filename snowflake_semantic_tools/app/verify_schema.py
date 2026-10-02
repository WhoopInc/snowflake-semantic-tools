"""`validate --verify-schema`: confirm each column a semantic view reads exists in the warehouse.

For every compiled semantic view, each fact and dimension that is a plain `TABLE.COLUMN` reference
is looked up on the physical table its logical table resolves to. Each table is described once,
however many views read it. A computed expression is not a column, and is left to the syntax
check; a filter is a predicate, and is left alone too.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort

# A plain column reference as the loader writes one: `TABLE.COLUMN`, each part one identifier.
_PLAIN = re.compile(r'^\s*("(?:[^"]|"")+"|[A-Za-z_][\w$]*)\.("(?:[^"]|"")+"|[A-Za-z_][\w$]*)\s*$')


def verify_columns(catalog: CatalogPort, compiled: CompileResult) -> tuple[Diagnostic, ...]:
    """Report each column a compiled view reads that its table lacks, and each table that is absent.

    Never writes. Tables are described in name order, once each.

    Raises:
        SnowflakePortError: a table could not be described.

    Diagnostics:
        SST-PRT005: a table a view reads does not exist, once per table.
        SST-VAL325: a column a view reads is absent from its table, once per view and column.
    """
    wanted: dict[str, list[tuple[str, str, str]]] = {}
    for item in compiled.compiled:
        if isinstance(item, CompiledView):
            for fqn, model, column in _references(item):
                wanted.setdefault(fqn, []).append((item.artifact_key, model, column))
    diagnostics: list[Diagnostic] = []
    for fqn in sorted(wanted):
        columns = catalog.table_columns(QualifiedName.parse(fqn))
        if columns is None:
            diagnostics.append(D("SST-PRT005", value=fqn, detail="a semantic view reads it as a table"))
            continue
        present = {name.casefold() for name, _ in columns}
        diagnostics.extend(
            D("SST-VAL325", model=model, column=column, subject=key)
            for key, model, column in dict.fromkeys(wanted[fqn])
            if column.casefold() not in present
        )
    return tuple(diagnostics)


def _references(item: CompiledView) -> Iterable[tuple[str, str, str]]:
    """Yield `(table FQN, logical table, column)` for each plain column reference of one view."""
    tables = {table.logical_name.casefold(): table for table in item.view.tables}
    for column in (*item.view.facts, *item.view.dimensions):
        reference = _plain_reference(column)
        if reference is None:
            continue
        table = tables.get(reference[0].casefold())
        if table is not None:
            yield table.fqn, table.logical_name.casefold(), reference[1]


def _plain_reference(column: Column) -> tuple[str, str] | None:
    """Return a column's `(table, column)` when its expression is one plain reference; else None."""
    if column.kind is ColumnKind.FILTER:
        return None
    match = _PLAIN.match(column.expr.text)
    if match is None:
        return None
    return _unquoted(match.group(1)), _unquoted(match.group(2))


def _unquoted(part: str) -> str:
    return part[1:-1].replace('""', '"') if part.startswith('"') else part
