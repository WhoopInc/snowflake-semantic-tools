"""Validation orchestration over compiled artifacts.

The offline checks live in `domain.validate`. The connected syntax check (SST-VAL020,
SST-VAL418) and the verified-query row count (SST-VAL415) stay here because they run SQL
through the `ExecutionPort`; the live-object checks of agents and tools are in
`app.compile.agents.observe`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.agents.observe import ObserveLiveObjects
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, resolve_severities
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView, VerifiedQuery
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
    query_text,
    sql,
)


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
    ) -> ValidationResult:
        """Validate a compile result, asking Snowflake to compile each semantic view's SQL when connected.

        Connected, each compiled semantic view's metrics, its dimensions (time dimensions and
        filters included, facts not), and its verified queries are EXPLAINed: an expression
        over a projection of NULL columns, with qualified references, variables, and metric
        names replaced by NULL; a verified query as written. A verified query that compiles
        is then run under a row count. With a catalog, the compiled agents and tools are
        checked against their live objects. Strict mode then promotes every warning to an error.

        Args:
            strict: Promote every warning, the compile's included, to an error.
            connected: Run the Snowflake checks; False skips them even with a port.

        Diagnostics:
            SST-VAL020: the Snowflake checks were skipped: disabled, or no port was given.
            SST-VAL418: Snowflake would not compile an expression or a verified query.
            SST-VAL415: a verified query ran and returned no rows.
            Those of `ObserveLiveObjects`, with a catalog.
        """
        diagnostics = compiled.diagnostics
        if not connected:
            diagnostics = DiagnosticBag(
                (*diagnostics, D("SST-VAL020", detail="Snowflake syntax checking was disabled"))
            )
        elif self._port is None:
            diagnostics = DiagnosticBag((*diagnostics, D("SST-VAL020", detail="no Snowflake connection was provided")))
        else:
            connected_diagnostics = [
                found
                for compiled_view in compiled.compiled
                if isinstance(compiled_view, CompiledView)
                for found in self._view_checks(self._port, compiled_view)
            ]
            if self._catalog is not None:
                connected_diagnostics.extend(ObserveLiveObjects(self._catalog, target=self._target).run(compiled))
            diagnostics = DiagnosticBag((*diagnostics, *connected_diagnostics))
        resolved, promoted = resolve_severities(diagnostics, strict=strict)
        return ValidationResult(compiled.rendered, resolved, promoted)

    def _view_checks(self, port: ExecutionPort, compiled_view: CompiledView) -> list[Diagnostic]:
        """EXPLAIN each of a view's metrics, dimensions and verified queries; count each query's rows."""
        checks: list[tuple[str, str, Sql]] = []
        projection = _validation_projection(compiled_view.view)
        checks.extend(
            ("metric", metric.qualified_name, _explain_expression(metric.expr, compiled_view.view, projection))
            for metric in compiled_view.view.metrics
        )
        checks.extend(
            (
                "filter" if column.kind.value == "filter" else "dimension",
                column.qualified_name,
                _explain_expression(column.expr, compiled_view.view, projection),
            )
            for column in compiled_view.view.dimensions
        )
        checks.extend(
            ("verified_query", query.name, sql("EXPLAIN {query}", query=query_text(query.sql)))
            for query in compiled_view.view.verified_queries
        )
        found: list[Diagnostic] = []
        for kind, name, statement in checks:
            try:
                port.query(statement)
            except SnowflakePortError as exc:
                found.append(D("SST-VAL418", type=kind, name=name, detail=str(exc), subject=compiled_view.artifact_key))
        failed = {item.context["name"] for item in found if item.context["type"] == "verified_query"}
        for query in compiled_view.view.verified_queries:
            if query.name not in failed:
                found.extend(self._row_count(port, compiled_view, query))
        return found

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
