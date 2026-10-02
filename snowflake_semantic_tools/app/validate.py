"""Validation orchestration over compiled artifacts.

The offline checks live in `domain.validate`. The connected checks stay here because they
run SQL through the `ExecutionPort`: the syntax check (SST-VAL418) compiles each expression,
the verified-query row count (SST-VAL415) runs each query that compiles, and the spot checks
(SST-VAL212, SST-VAL218) read the joined tables. SST-VAL020 reports each one skipped. The
live-object checks of agents and tools are in `app.compile.agents.observe`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.agents.observe import ObserveLiveObjects
from snowflake_semantic_tools.app.verify_schema import verify_columns
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, resolve_severities
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, SemanticView, Table, VerifiedQuery
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import (
    AuthoredExpression,
    Sql,
    expr,
    guard_expression,
    ident,
    join,
    qname,
    query_text,
    sql,
)
from snowflake_semantic_tools.domain.validate.shared import reference_cycle


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """What validation found: the compile's rendered artifacts and every diagnostic, strictness applied.

    Attributes:
        rendered: Every rendered artifact of the compile, in compile order, whatever was found.
        diagnostics: The compile's diagnostics, then validation's own, after strict promotion.
        promoted: How many warnings strict mode made errors; 0 without it.
    """

    rendered: tuple[RenderedArtifact, ...]
    diagnostics: DiagnosticBag
    promoted: int = 0

    @property
    def success(self) -> bool:
        """Report whether validation passed: no diagnostic is an error once strict promotion applied."""
        return not self.diagnostics.has_errors


class ValidateArtifacts:
    """Validate a compile result offline or, with a port, against Snowflake.

    The connected checks only EXPLAIN, count a verified query's rows, and read what exists,
    so they never write.

    Args:
        catalog: Reads the live objects the compiled agents and tools publish over or call;
            None skips those checks.
        target: The `profiles.yml` target being validated, which a missing object names.
        clock: Times each verified query's count; None reports it as taking 0ms.
    """

    def __init__(
        self,
        port: ExecutionPort | None = None,
        *,
        catalog: CatalogPort | None = None,
        target: str = "",
        clock: ClockPort | None = None,
    ) -> None:
        self._port = port
        self._catalog = catalog
        self._target = target
        self._clock = clock

    def run(
        self,
        compiled: CompileResult,
        *,
        strict: bool,
        connected: bool,
        verify_schema: bool = False,
    ) -> ValidationResult:
        """Validate a compile result, asking Snowflake to check each semantic view when connected.

        Offline, the compiled artifacts' dependencies are checked for a cycle. Connected, each
        compiled semantic view's metrics, its dimensions (time dimensions and filters included,
        facts not), and its verified queries are EXPLAINed: an expression over a projection of
        NULL columns, with qualified references, variables, and metric names replaced by NULL;
        a verified query as written. A verified query that compiles is then run under a row
        count. Then each equality relationship's target is read for a repeated join key, and
        each distinct range for overlapping ranges. With a catalog, the compiled agents and
        tools are checked against their live objects. Strict mode then promotes every warning
        to an error.

        Args:
            strict: Promote every warning, the compile's included, to an error.
            connected: Run the Snowflake checks; False skips them even with a port.
            verify_schema: With a catalog, also look each column a view reads up in the
                warehouse, as `verify_columns` does.

        Diagnostics:
            SST-VAL010: the compiled artifacts depend on one another in a cycle.
            SST-VAL020: once per connected rule, when the Snowflake checks were skipped:
                disabled, or no port was given.
            SST-VAL418: Snowflake would not compile an expression or a verified query.
            SST-VAL415: a verified query ran and returned no rows.
            SST-VAL212: a relationship's target holds more than one row for a join key.
            SST-VAL218: a distinct range's rows overlap.
            Those of `ObserveLiveObjects`, with a catalog.
            Those of `verify_columns`, with `verify_schema` and a catalog.
        """
        found = [*compiled.diagnostics, *_cycle_diagnostics(compiled.rendered)]
        if not connected or self._port is None:
            detail = (
                "Snowflake syntax checking was disabled" if not connected else "no Snowflake connection was provided"
            )
            found.extend(D("SST-VAL020", rule_id=rule, detail=detail) for rule in CONNECTED_RULES)
        else:
            for compiled_view in compiled.compiled:
                if isinstance(compiled_view, CompiledView):
                    found.extend(self._compile_checks(compiled_view))
                    found.extend(self._data_checks(compiled_view))
            if self._catalog is not None:
                found.extend(ObserveLiveObjects(self._catalog, target=self._target).run(compiled))
        if verify_schema and self._catalog is not None:
            found.extend(verify_columns(self._catalog, compiled))
        resolved, promoted = resolve_severities(DiagnosticBag(found), strict=strict)
        return ValidationResult(compiled.rendered, resolved, promoted)

    def _compile_checks(self, compiled_view: CompiledView) -> list[Diagnostic]:
        """EXPLAIN each metric, dimension and verified query of one view (SST-VAL418)."""
        assert self._port is not None
        view = compiled_view.view
        projection = _validation_projection(view)
        checks: list[tuple[str, str, Sql]] = [
            ("metric", metric.qualified_name, _explain_expression(metric.expr, view, projection))
            for metric in view.metrics
        ]
        checks.extend(
            (
                "filter" if column.kind.value == "filter" else "dimension",
                column.qualified_name,
                _explain_expression(column.expr, view, projection),
            )
            for column in view.dimensions
        )
        checks.extend(
            ("verified_query", query.name, sql("EXPLAIN {query}", query=query_text(query.sql)))
            for query in view.verified_queries
        )
        # A verified query is authored SQL that may name objects relative to the view's schema,
        # so it alone is explained in that scope.
        view_scope = SchemaScope.from_qualified_name(QualifiedName.parse(view.fqn))
        diagnostics: list[Diagnostic] = []
        for kind, name, statement in checks:
            try:
                if kind == "verified_query":
                    self._port.query_in_context(view_scope, statement)
                else:
                    self._port.query(statement)
            except SnowflakePortError as exc:
                diagnostics.append(
                    D("SST-VAL418", type=kind, name=name, detail=str(exc), subject=compiled_view.artifact_key)
                )
        failed = {item.context["name"] for item in diagnostics if item.context["type"] == "verified_query"}
        for query in view.verified_queries:
            if query.name not in failed:
                diagnostics.extend(self._row_count(self._port, compiled_view, query))
        return diagnostics

    def _row_count(self, port: ExecutionPort, compiled_view: CompiledView, query: VerifiedQuery) -> list[Diagnostic]:
        """Run a verified query under a row count, reporting one that returns nothing.

        A query whose count fails is left to its EXPLAIN, which already passed; the failure is
        not reported a second time.
        """
        started = self._clock.monotonic_ms() if self._clock is not None else 0
        try:
            result = port.query(
                sql("SELECT COUNT(*) AS ROW_COUNT FROM ({query}) AS SST_VQ", query=query_text(query.sql))
            )
        except SnowflakePortError:
            return []
        elapsed = (self._clock.monotonic_ms() - started) if self._clock is not None else 0
        count = result.rows[0][0] if result.rows and result.rows[0] else None
        if not isinstance(count, (int, float)) or count:
            return []
        return [
            D(
                "SST-VAL415",
                member=query.name,
                row_count=int(count),
                elapsed_ms=elapsed,
                subject=compiled_view.artifact_key,
            )
        ]

    def _data_checks(self, compiled_view: CompiledView) -> list[Diagnostic]:
        """Read each equality join's target for repeated keys, and each distinct range for overlaps.

        A read that fails is not reported here: the EXPLAIN checks report a relation they
        cannot reach.
        """
        assert self._port is not None
        view = compiled_view.view
        tables = {table.logical_name: table for table in view.tables}
        diagnostics: list[Diagnostic] = []
        for relationship in sorted(view.relationships, key=lambda item: item.name):
            target = tables.get(relationship.to_table)
            if target is None or not view.scope.admits_relationship(relationship.name):
                continue
            try:
                found = self._spot_check(relationship, target)
            except SnowflakePortError:
                continue
            if found is not None:
                diagnostics.append(replace(found, subject=compiled_view.artifact_key))
        return diagnostics

    def _spot_check(self, relationship: Relationship, target: Table) -> Diagnostic | None:
        """Run one relationship's spot check: duplicate keys, or overlapping ranges for a range join."""
        assert self._port is not None
        name = relationship.name.casefold()
        if relationship.range_bounds is not None:
            if target.distinct_range is None:
                return None
            start, end = target.distinct_range
            rows = self._port.query(_overlap_query(target)).rows
            if not rows:
                return None
            example = f"[{rows[0][0]}, {rows[0][1]}) and [{rows[0][2]}, {rows[0][3]})"
            return D("SST-VAL218", relationship=name, name=target.logical_name, a=start, b=end, value=example)
        if relationship.asof_index is not None:
            return None
        rows = self._port.query(_duplicate_key_query(relationship, target)).rows
        repeated = int(str(rows[0][0])) if rows and rows[0] and rows[0][0] is not None else 0
        if repeated <= 0:
            return None
        return D(
            "SST-VAL212",
            relationship=name,
            found="many-to-one",
            value=f"{repeated} rows of '{target.logical_name}' repeat a join key",
        )


# The connected rules, which offline validation skips and reports as skipped.
CONNECTED_RULES = ("SST-VAL418", "SST-VAL415", "SST-VAL212", "SST-VAL218")


def _cycle_diagnostics(rendered: tuple[RenderedArtifact, ...]) -> tuple[Diagnostic, ...]:
    """Report a cycle among the compiled artifacts' dependencies, which no publish order satisfies."""
    types = {artifact.key: artifact.artifact_type for artifact in rendered}
    cycle = reference_cycle(
        {artifact.key: tuple(key for key in artifact.depends_on if key in types) for artifact in rendered}
    )
    if not cycle:
        return ()
    return (D("SST-VAL010", subject=cycle[0], type=types[cycle[0]], cycle=" -> ".join(cycle)),)


def _duplicate_key_query(relationship: Relationship, target: Table) -> Sql:
    """Count the target rows beyond one per join key: zero when the join is many-to-one."""
    columns = join(", ", (ident(Identifier.parse(column)) for column in relationship.to_columns))
    return sql(
        "SELECT COUNT(*) - COUNT(DISTINCT {columns}) FROM {table}",
        columns=columns,
        table=qname(QualifiedName.parse(target.fqn)),
    )


def _overlap_query(target: Table) -> Sql:
    """Find one pair of distinct ranges of the target that overlap; no row when none do."""
    assert target.distinct_range is not None
    start, end = (ident(Identifier.parse(name)) for name in target.distinct_range)
    return sql(
        "SELECT A.{start}, A.{end}, B.{start}, B.{end} FROM {table} AS A JOIN {table} AS B"
        " ON A.{start} < B.{end} AND B.{start} < A.{end}"
        " AND (A.{start} < B.{start} OR (A.{start} = B.{start} AND A.{end} < B.{end})) LIMIT 1",
        start=start,
        end=end,
        table=qname(QualifiedName.parse(target.fqn)),
    )


def _explain_expression(expression: AuthoredExpression, view: SemanticView, projection: Sql) -> Sql:
    """EXPLAIN one member expression over the view's NULL projection.

    The rewritten expression is guarded again, so a statement it builds still holds one
    expression; a rewrite cannot fail that guard, since it only swaps words for NULL.
    """
    test = guard_expression(_test_expression(expression.text, view))
    return sql(
        "EXPLAIN SELECT {expression} FROM ({projection}) AS SST_VALIDATE", expression=expr(test), projection=projection
    )


def _test_expression(expression: str, view: SemanticView) -> str:
    qualified = re.sub(
        r"\b[A-Za-z_][A-Za-z0-9_$]*\s*\.\s*[A-Za-z_][A-Za-z0-9_$]*\b",
        "NULL",
        expression,
    )
    variable_names = tuple(variable.name for variable in view.variables)
    member_names = tuple(metric.name for metric in view.metrics)
    for name in (*variable_names, *member_names):
        qualified = re.sub(rf"\b{re.escape(name)}\b", "NULL", qualified, flags=re.IGNORECASE)
    qualified = re.sub(
        r"PARTITION\s+BY\s+EXCLUDING\s+NULL(?:\s*,\s*NULL)*",
        "PARTITION BY NULL",
        qualified,
        flags=re.IGNORECASE,
    )
    return qualified


def _validation_projection(view: SemanticView) -> Sql:
    columns = sorted({column.name.upper() for column in view.columns})
    if not columns:
        return sql("SELECT 1 AS SST_VALUE WHERE FALSE")
    nulls = join(", ", (sql("NULL AS {column}", column=ident(Identifier.parse(column))) for column in columns))
    return sql("SELECT {columns} WHERE FALSE", columns=nulls)
