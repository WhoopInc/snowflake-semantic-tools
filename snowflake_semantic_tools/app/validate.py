"""Validation orchestration over compiled artifacts."""

from __future__ import annotations

import re
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag, resolve_severities
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort, SnowflakePortError


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

    def __init__(self, port: SnowflakePort | None = None) -> None:
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
                    checks: list[tuple[str, str, str]] = []
                    checks.extend(
                        (
                            "metric",
                            metric.qualified_name,
                            f"EXPLAIN SELECT {_test_expression(metric.expr, compiled_view.view)} "
                            f"FROM ({_validation_projection(compiled_view.view)}) AS SST_VALIDATE",
                        )
                        for metric in compiled_view.view.metrics
                    )
                    checks.extend(
                        (
                            "filter" if column.kind.value == "filter" else "dimension",
                            column.qualified_name,
                            f"EXPLAIN SELECT {_test_expression(column.expr, compiled_view.view)} "
                            f"FROM ({_validation_projection(compiled_view.view)}) AS SST_VALIDATE",
                        )
                        for column in compiled_view.view.dimensions
                    )
                    checks.extend(
                        (
                            "verified_query",
                            query.name,
                            f"EXPLAIN {query.sql.rstrip(';')}",
                        )
                        for query in compiled_view.view.verified_queries
                    )
                    for kind, name, sql in checks:
                        try:
                            self._port.query(sql)
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


def _validation_projection(view: SemanticView) -> str:
    columns = sorted({column.name.upper() for column in view.columns})
    if not columns:
        return "SELECT 1 AS SST_VALUE WHERE FALSE"
    return "SELECT " + ", ".join(f"NULL AS {column}" for column in columns) + " WHERE FALSE"
