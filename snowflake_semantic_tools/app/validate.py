"""Validation orchestration over compiled artifacts.

The offline checks live in `domain.validate`. The connected syntax check (SST-VAL020,
SST-VAL418) stays here because it compiles each expression through the `ExecutionPort`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, resolve_severities
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
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

    The connected checks only EXPLAIN, so they never write.
    """

    def __init__(self, port: ExecutionPort | None = None) -> None:
        self._port = port

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
        names replaced by NULL; a verified query as written. No other artifact type gets a
        connected check. Strict mode then promotes every warning to an error.

        Args:
            strict: Promote every warning, the compile's included, to an error.
            connected: Run the Snowflake checks; False skips them even with a port.

        Diagnostics:
            SST-VAL020: the Snowflake checks were skipped: disabled, or no port was given.
            SST-VAL418: Snowflake would not compile an expression or a verified query.
        """
        diagnostics = compiled.diagnostics
        if not connected:
            diagnostics = DiagnosticBag(
                (*diagnostics, D("SST-VAL020", detail="Snowflake syntax checking was disabled"))
            )
        else:
            if self._port is None:
                diagnostics = DiagnosticBag(
                    (*diagnostics, D("SST-VAL020", detail="no Snowflake connection was provided"))
                )
            else:
                connected_diagnostics = []
                for compiled_view in compiled.compiled:
                    if not isinstance(compiled_view, CompiledView):
                        continue
                    checks: list[tuple[str, str, Sql]] = []
                    projection = _validation_projection(compiled_view.view)
                    checks.extend(
                        (
                            "metric",
                            metric.qualified_name,
                            _explain_expression(metric.expr, compiled_view.view, projection),
                        )
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
                    for kind, name, statement in checks:
                        try:
                            self._port.query(statement)
                        except SnowflakePortError as exc:
                            connected_diagnostics.append(
                                D(
                                    "SST-VAL418",
                                    type=kind,
                                    name=name,
                                    detail=str(exc),
                                    subject=compiled_view.artifact_key,
                                )
                            )
                diagnostics = DiagnosticBag((*diagnostics, *connected_diagnostics))
        resolved, promoted = resolve_severities(diagnostics, strict=strict)
        return ValidationResult(compiled.rendered, resolved, promoted)


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
