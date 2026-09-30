"""Check the template calls in filter expressions and verified-query SQL, and each filter's shape."""

from __future__ import annotations

import re
from collections.abc import Mapping

from .....domain.model.artifact_key import artifact_key
from .....domain.model.dbt import DbtModel
from .....domain.model.diagnostic import D, Diagnostic, Origin
from .....domain.model.expression import is_boolean_expression as _is_boolean_expression
from .....domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls
from ..defs import FilterDef, VerifiedQueryDef


def _expression_reference_diagnostics(
    members: tuple[FilterDef | VerifiedQueryDef, ...],
    models: dict[str, DbtModel],
    *,
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for member in members:
        text = member.expr if isinstance(member, FilterDef) else member.sql
        subject = artifact_key("filter" if isinstance(member, FilterDef) else "verified_query", member.name)
        declared = {table.casefold() for table in member.tables}
        if isinstance(member, VerifiedQueryDef):
            for table_name in _sql_tables(member.sql):
                if declared and table_name not in declared:
                    diagnostics.append(
                        D("SST-VAL413", origin=member.origin, subject=subject, member=member.name, name=table_name)
                    )
        try:
            calls = scan_template_calls(text)
        except TemplateSyntaxError as exc:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=member.origin,
                    file=member.origin.file if member.origin else "<expression>",
                    line=exc.line,
                    col=exc.col,
                    reason=exc.reason,
                    subject=subject,
                )
            )
            continue
        for call in calls:
            if call.function == "metric" and isinstance(member, FilterDef):
                diagnostics.append(
                    D(
                        "SST-REF041",
                        origin=member.origin,
                        subject=subject,
                        artifact=subject,
                        function="metric",
                        field="a filter expression",
                    )
                )
                continue
            if call.function == "metric" and (len(call.args) != 1 or call.args[0].casefold() not in metric_names):
                if len(call.args) != 1:
                    diagnostics.append(
                        D(
                            "SST-REF042",
                            origin=member.origin,
                            subject=subject,
                            artifact=subject,
                            detail=f"metric() takes one name, found {len(call.args)} in {call.raw}",
                        )
                    )
                else:
                    diagnostics.append(D("SST-REF006", origin=member.origin, subject=subject, name=call.args[0]))
                continue
            if call.function == "var" and (len(call.args) != 1 or call.args[0] not in variables):
                diagnostics.append(_var_diagnostic(call, member.origin, subject))
                continue
            if call.function in ("table", "column"):
                # SST-REF034/SST-REF035 already name the legacy global at its position.
                continue
            if call.function not in ("ref", "metric", "var"):
                diagnostics.append(
                    D(
                        "SST-REF041",
                        origin=member.origin,
                        subject=subject,
                        artifact=subject,
                        function=call.function,
                        field="an expression",
                    )
                )
                continue
            if call.function != "ref" or len(call.args) not in (1, 2):
                continue
            model_name = call.args[0]
            model = models.get(model_name.casefold())
            if model is None:
                diagnostics.append(
                    D(
                        "SST-REF001",
                        origin=member.origin,
                        subject=subject,
                        model=model_name,
                    )
                )
            elif len(call.args) == 2:
                column_name = call.args[1]
                column = model.column(column_name)
                if column is None:
                    diagnostics.append(
                        D(
                            "SST-REF002",
                            origin=member.origin,
                            subject=subject,
                            model=model_name,
                            column=column_name,
                        )
                    )
                elif column.excluded:
                    diagnostics.append(
                        D(
                            "SST-VAL318",
                            origin=member.origin,
                            subject=subject,
                            artifact=subject,
                            member=member.name,
                            column=column_name,
                        )
                    )
            elif declared and model_name.casefold() not in declared:
                diagnostics.append(
                    D("SST-REF043", origin=member.origin, subject=subject, artifact=subject, model=model_name)
                )
    return tuple(diagnostics)


def _filter_diagnostics(filters: tuple[FilterDef, ...]) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for filter_def in filters:
        boolean = _is_boolean_expression(filter_def.expr)
        if filter_def.entity_level and not boolean:
            code = "SST-VAL401"
        elif not filter_def.entity_level and not filter_def.labeled and boolean:
            # A boolean predicate with no labels: key has no native home; it
            # must carry labels: [filter] to render as a LABELS = (FILTER) dimension.
            code = "SST-VAL405"
        else:
            continue
        diagnostics.append(
            D(code, member=filter_def.name, subject=artifact_key("filter", filter_def.name), origin=filter_def.origin)
        )
    return tuple(diagnostics)


def _bare_column_identifiers(
    expression: str,
    tables: tuple[str, ...],
    models: Mapping[str, DbtModel],
    variables: Mapping[str, object],
) -> tuple[str, ...]:
    text = re.sub(r"\{\{.*?\}\}", " ", expression)
    text = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", " ", text)
    functions = {match.group(1).casefold() for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(", text)}
    known_variables = {name.casefold() for name in variables}
    known_columns = {
        column.name.casefold()
        for table in tables
        for model in (models.get(table.casefold()),)
        if model is not None
        for column in model.columns
    }
    return tuple(
        dict.fromkeys(
            identifier
            for identifier in re.findall(r"\b[A-Za-z_][A-Za-z0-9_$]*\b", text)
            if identifier.casefold() in known_columns
            and identifier.casefold() not in functions
            and identifier.casefold() not in known_variables
        )
    )


def _sql_tables(sql: str) -> tuple[str, ...]:
    """The logical tables a verified query reads: FROM/JOIN names that are not its own CTEs.

    String literals are masked first, so `IN ('Join Flow')` is not read as a join.
    """
    lines = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    statement = re.sub(r"'(?:[^']|'')*'", "''", lines)
    names = re.findall(r"(?i)\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$]*)", statement)
    ctes = {
        name.casefold()
        for name in re.findall(r"(?i)(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*)([A-Za-z_][A-Za-z0-9_$]*)\s+AS\s*\(", statement)
    }
    return tuple(dict.fromkeys(name.casefold() for name in names if name.casefold() not in ctes))


def _var_diagnostic(call: TemplateCall, origin: Origin | None, subject: str) -> Diagnostic:
    if len(call.args) != 1:
        return D(
            "SST-REF042",
            origin=origin,
            subject=subject,
            artifact=subject,
            detail=f"var() takes one name, found {len(call.args)} in {call.raw}",
        )
    return D("SST-REF038", origin=origin, subject=subject, artifact=subject, name=call.args[0])
