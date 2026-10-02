"""Check the template calls in filter expressions and verified-query SQL, and each filter's shape."""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.validate.expression import is_boolean_expression as _is_boolean_expression


def _expression_reference_diagnostics(
    members: tuple[FilterDef | VerifiedQueryDef, ...],
    models: dict[str, DbtModel],
    *,
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> tuple[Diagnostic, ...]:
    """Check the template calls of every filter expression and verified-query SQL, member by member.

    A verified query's undeclared tables come first, then each call in source order. A malformed
    template (SST-LOD004) ends that member's checks, since no call in it can be read.

    Args:
        metric_names: Every metric's name, casefolded.
        variables: The project's `vars:`.
    """
    diagnostics: list[Diagnostic] = []
    for member in members:
        text = member.expr if isinstance(member, FilterDef) else member.sql
        subject = artifact_key("filter" if isinstance(member, FilterDef) else "verified_query", member.name)
        declared = {table.casefold() for table in member.tables}
        if isinstance(member, VerifiedQueryDef):
            diagnostics.extend(_undeclared_table_diagnostics(member, subject, declared))
        calls = _scan_expression(text, member.origin, subject)
        if isinstance(calls, Diagnostic):
            diagnostics.append(calls)
            continue
        for call in calls:
            diagnostics.extend(_call_diagnostics(call, member, subject, declared, models, metric_names, variables))
    return tuple(diagnostics)


def _undeclared_table_diagnostics(member: VerifiedQueryDef, subject: str, declared: set[str]) -> list[Diagnostic]:
    """Report each table a verified query's SQL reads that its `tables:` leaves out.

    Nothing is reported when the query declares no tables at all.

    Diagnostics:
        SST-VAL413: when the SQL reads a table outside the query's non-empty `tables:`.
    """
    return [
        D("SST-VAL413", origin=member.origin, subject=subject, member=member.name, name=table_name)
        for table_name in _sql_tables(member.sql)
        if declared and table_name not in declared
    ]


def _scan_expression(text: str, origin: Origin | None, subject: str) -> tuple[TemplateCall, ...] | Diagnostic:
    """Return an expression's template calls, or the diagnostic for a malformed template.

    Diagnostics:
        SST-LOD004: when a template call in the expression does not parse.
    """
    try:
        return scan_template_calls(text)
    except TemplateSyntaxError as exc:
        return D(
            "SST-LOD004",
            origin=origin,
            file=origin.file if origin else "<expression>",
            line=exc.line,
            col=exc.col,
            reason=exc.reason,
            subject=subject,
        )


def _call_diagnostics(
    call: TemplateCall,
    member: FilterDef | VerifiedQueryDef,
    subject: str,
    declared: set[str],
    models: Mapping[str, DbtModel],
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> list[Diagnostic]:
    """Check one template call of a filter expression or verified-query SQL, by the function it calls.

    `metric()` and `ref()` calls have rules of their own; a legacy `table()` or `column()` call is
    left to SST-REF034/SST-REF035, which name it at its position.

    Diagnostics:
        SST-REF041: when the call's function is none of `ref()`, `metric()` and `var()`.
        SST-REF042: when a `var()` call does not take exactly one name.
        SST-REF038: when a `var()` call names no project variable.
    """
    if call.function == "metric":
        return _metric_call_diagnostics(call, member, subject, metric_names)
    if call.function == "var":
        return [_var_diagnostic(call, member.origin, subject)] if _is_bad_var(call, variables) else []
    if call.function in ("table", "column"):
        # SST-REF034/SST-REF035 already name the legacy global at its position.
        return []
    if call.function != "ref":
        return [
            D(
                "SST-REF041",
                origin=member.origin,
                subject=subject,
                artifact=subject,
                function=call.function,
                field="an expression",
            )
        ]
    if len(call.args) not in (1, 2):
        return []
    return _ref_call_diagnostics(call, member, subject, declared, models)


def _metric_call_diagnostics(
    call: TemplateCall, member: FilterDef | VerifiedQueryDef, subject: str, metric_names: frozenset[str]
) -> list[Diagnostic]:
    """Check one `metric()` call: a filter may not make one, and a verified query's must resolve.

    Diagnostics:
        SST-REF041: when a filter expression calls `metric()`.
        SST-REF042: when the call does not name exactly one metric.
        SST-REF006: when the call names no metric.
    """
    if isinstance(member, FilterDef):
        return [
            D(
                "SST-REF041",
                origin=member.origin,
                subject=subject,
                artifact=subject,
                function="metric",
                field="a filter expression",
            )
        ]
    if len(call.args) != 1:
        return [
            D(
                "SST-REF042",
                origin=member.origin,
                subject=subject,
                artifact=subject,
                detail=f"metric() takes one name, found {len(call.args)} in {call.raw}",
            )
        ]
    if call.args[0].casefold() not in metric_names:
        return [D("SST-REF006", origin=member.origin, subject=subject, name=call.args[0])]
    return []


def _ref_call_diagnostics(
    call: TemplateCall,
    member: FilterDef | VerifiedQueryDef,
    subject: str,
    declared: set[str],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Check one `ref()` call of one or two arguments against the dbt models and the member's tables.

    Diagnostics:
        SST-REF001: when the call names no dbt model.
        SST-REF002: when the call names a column its model does not have.
        SST-VAL318: when the call names a column its model excludes.
        SST-REF043: when a table reference names a model outside the member's non-empty `tables:`.
    """
    model_name = call.args[0]
    model = models.get(model_name.casefold())
    if model is None:
        return [D("SST-REF001", origin=member.origin, subject=subject, model=model_name)]
    if len(call.args) == 2:
        column_name = call.args[1]
        column = model.column(column_name)
        if column is None:
            return [D("SST-REF002", origin=member.origin, subject=subject, model=model_name, column=column_name)]
        if column.excluded:
            return [
                D(
                    "SST-VAL318",
                    origin=member.origin,
                    subject=subject,
                    artifact=subject,
                    member=member.name,
                    column=column_name,
                )
            ]
        return []
    if declared and model_name.casefold() not in declared:
        return [D("SST-REF043", origin=member.origin, subject=subject, artifact=subject, model=model_name)]
    return []


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
    """Find the columns of `tables` that `expression` names as bare identifiers, not by `ref()`.

    Template calls, string literals and double-quoted identifiers are masked first, and an
    identifier is left out when the expression also calls a function of that name or it names
    a project variable. Names compare casefolded; a table that is not a dbt model has no columns.

    Returns:
        Each identifier as written, once per spelling, in first-seen order.
    """
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


def _is_bad_var(call: TemplateCall, variables: Mapping[str, object] | None) -> bool:
    """Report whether a `var()` call is malformed or names no variable; None checks only its shape."""
    return len(call.args) != 1 or variables is not None and call.args[0] not in variables


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
