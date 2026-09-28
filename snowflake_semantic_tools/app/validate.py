"""Validation orchestration over compiled artifacts."""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..domain.model.diagnostic import D, DiagnosticBag, resolve_severities
from ..domain.model.lifecycle import RenderedArtifact
from ..domain.model.semantic_view import SemanticView
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError
from .compile import CompiledView, CompileResult


@dataclass(frozen=True, slots=True)
class ValidationResult:
    rendered: tuple[RenderedArtifact, ...]
    diagnostics: DiagnosticBag
    promoted: int = 0

    @property
    def success(self) -> bool:
        return not self.diagnostics.has_errors


class ValidateArtifacts:
    def __init__(self, port: SnowflakePort | None = None) -> None:
        self._port = port

    def run(
        self,
        compiled: CompileResult,
        *,
        strict: bool,
        connected: bool,
    ) -> ValidationResult:
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
