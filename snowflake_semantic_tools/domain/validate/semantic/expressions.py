"""Check the template calls in filter expressions and verified-query SQL, and each filter's shape."""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import FilterDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import call_problem, syntax_problem, variable_problem
from snowflake_semantic_tools.domain.resolve.template import FILTER_EXPR, VQR_SQL
from snowflake_semantic_tools.domain.validate.column_metadata import is_boolean_type
from snowflake_semantic_tools.domain.validate.expression import is_boolean_expression as _is_boolean_expression


def expression_reference_diagnostics(
    members: tuple[FilterDef | VerifiedQueryDef, ...],
    models: dict[str, DbtModel],
    *,
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> tuple[Diagnostic, ...]:
    """Check the template calls of every filter expression and verified-query SQL, member by member.

    A verified query's tables read without `ref()` come first, then each call in source order. A
    template that does not parse ends that member's checks, since no call in it can be read.

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
            diagnostics.extend(_unreferenced_table_diagnostics(member, subject, declared, models))
        calls = scan_expression(text, member.origin, subject)
        if isinstance(calls, Diagnostic):
            diagnostics.append(calls)
            continue
        for call in calls:
            diagnostics.extend(_call_diagnostics(call, member, subject, models, metric_names, variables))
    return tuple(diagnostics)


def _unreferenced_table_diagnostics(
    member: VerifiedQueryDef, subject: str, declared: set[str], models: Mapping[str, DbtModel]
) -> list[Diagnostic]:
    """Report each table a verified query's SQL reads by name rather than with `ref()`.

    Its tables are the models its `ref()` calls name, so a table read by name alone is not one
    of them, and the query may attach to a view that lacks it. The table is named by the
    relation its dbt model resolves to; a table that is no dbt model is named as written.

    Diagnostics:
        SST-VAL414: when the SQL reads a table that it does not name with `ref()`.
    """
    return [
        D(
            "SST-VAL414",
            origin=member.origin,
            subject=subject,
            member=member.name,
            relation=model.relation_name if (model := models.get(table_name)) is not None else table_name,
        )
        for table_name in sql_tables(member.sql)
        if table_name not in declared
    ]


def scan_expression(text: str, origin: Origin | None, subject: str) -> tuple[TemplateCall, ...] | Diagnostic:
    """Return an expression's template calls, or the diagnostic for a template that does not parse.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: when a template span in the expression does not parse.
    """
    try:
        return scan_template_calls(text)
    except TemplateSyntaxError as exc:
        return syntax_problem(exc, origin.file if origin else "<expression>", subject=subject)


def _call_diagnostics(
    call: TemplateCall,
    member: FilterDef | VerifiedQueryDef,
    subject: str,
    models: Mapping[str, DbtModel],
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> list[Diagnostic]:
    """Check one template call of a filter expression or verified-query SQL, by the function it calls.

    A call must first be legal in its field (`call_problem`); a legacy `table()` or `column()`
    call is left to SST-REF034/SST-REF035 from the legacy check, which names it at its position.

    Diagnostics:
        SST-REF004, SST-REF041, SST-REF015: when the call is not legal in the field.
        SST-REF006: when a `metric()` call names no metric.
        SST-CFG029, SST-REF009: when a `var()` call names no project variable, or an empty one.
    """
    if call.function in ("table", "column"):
        # SST-REF034/SST-REF035 already name the legacy global at its position.
        return []
    is_filter = isinstance(member, FilterDef)
    problem = call_problem(
        call,
        (FILTER_EXPR if is_filter else VQR_SQL).allowed,
        member.origin,
        field="filter.expression" if is_filter else "verified_query.sql",
        artifact=subject,
        subject=subject,
    )
    if problem is not None:
        return [problem]
    if call.function == "metric":
        if call.args[0].casefold() not in metric_names:
            return [D("SST-REF006", origin=member.origin, subject=subject, name=call.args[0])]
        return []
    if call.function == "var":
        found = variable_problem(call.args[0], variables, member.origin, subject=subject)
        return [found] if found is not None else []
    return _ref_call_diagnostics(call, member, subject, models)


def _ref_call_diagnostics(
    call: TemplateCall,
    member: FilterDef | VerifiedQueryDef,
    subject: str,
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Check one `ref()` call of one or two arguments against the dbt models.

    Diagnostics:
        SST-REF001: when the call names no dbt model.
        SST-REF002: when the call names a column its model does not have.
        SST-VAL318: when the call names a column its model excludes.
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


def _is_boolean_column(expression: str, models: Mapping[str, DbtModel]) -> bool:
    """Report whether `expression` is exactly one `ref()` to a column dbt declares BOOLEAN."""
    text = expression.strip()
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return False
    if len(calls) != 1 or calls[0].raw != text or calls[0].function != "ref" or len(calls[0].args) != 2:
        return False
    model = models.get(calls[0].args[0].casefold())
    column = model.column(calls[0].args[1]) if model is not None else None
    return column is not None and is_boolean_type(column.data_type)


def filter_diagnostics(
    filters: tuple[FilterDef, ...], models: Mapping[str, DbtModel], variables: Mapping[str, object]
) -> tuple[Diagnostic, ...]:
    """Check each filter's shape, then the columns its expression names bare, filter by filter.

    An expression is boolean when its shape says so, or when it is one `ref()` to a column dbt
    declares BOOLEAN.

    Diagnostics:
        SST-VAL401: when a filter labeled `filter` has an expression that is not boolean.
        SST-VAL405: when a filter with no `labels:` key has a boolean expression.
        SST-VAL404: for each column of the filter's tables its expression names without `ref()`.
    """
    diagnostics: list[Diagnostic] = []
    for filter_def in filters:
        subject = artifact_key("filter", filter_def.name)
        boolean = _is_boolean_expression(filter_def.expr) or _is_boolean_column(filter_def.expr, models)
        code: str | None = None
        if filter_def.entity_level and not boolean:
            code = "SST-VAL401"
        elif not filter_def.entity_level and not filter_def.labeled and boolean:
            # A boolean predicate with no labels: key has no native home; it
            # must carry labels: [filter] to render as a LABELS = (FILTER) dimension.
            code = "SST-VAL405"
        if code is not None:
            diagnostics.append(D(code, member=filter_def.name, subject=subject, origin=filter_def.origin))
        diagnostics.extend(
            D("SST-VAL404", member=filter_def.name, column=column, subject=subject, origin=filter_def.origin)
            for column in bare_column_identifiers(filter_def.expr, filter_def.tables, models, variables)
        )
    return tuple(diagnostics)


def bare_column_identifiers(
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


# The functions whose arguments take a `FROM` that reads no table, as `EXTRACT(YEAR FROM d)`.
_FROM_FUNCTIONS = frozenset(("extract", "trim", "substring", "position", "overlay"))
_INNER_GROUP = re.compile(r"(\b[A-Za-z_][A-Za-z0-9_$]*)?(\s*)\(([^()]*)\)")
_FROM = re.compile(r"(?i)\bFROM\b")


def _fold_group(match: re.Match[str]) -> str:
    """Rewrite one innermost `(...)` as `[...]`, dropping its FROM when a FROM-taking function owns it."""
    name, gap, body = match.group(1) or "", match.group(2), match.group(3)
    if name.casefold() in _FROM_FUNCTIONS:
        body = _FROM.sub(" ", body)
    return f"{name}{gap}[{body}]"


def sql_tables(sql: str) -> tuple[str, ...]:
    """The logical tables a verified query reads: FROM/JOIN names that are not its own CTEs.

    String literals are masked first, so `IN ('Join Flow')` is not read as a join, then the
    `FROM` of `IS [NOT] DISTINCT FROM` and of a function such as `EXTRACT(YEAR FROM d)`.
    """
    lines = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    statement = re.sub(r"'(?:[^']|'')*'", "''", lines)
    ctes = {
        name.casefold()
        for name in re.findall(r"(?i)(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*)([A-Za-z_][A-Za-z0-9_$]*)\s+AS\s*\(", statement)
    }
    statement = re.sub(r"(?i)\bDISTINCT\s+FROM\b", "DISTINCT ", statement)
    while (folded := _INNER_GROUP.sub(_fold_group, statement)) != statement:
        statement = folded
    names = re.findall(r"(?i)\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$]*)", statement)
    return tuple(dict.fromkeys(name.casefold() for name in names if name.casefold() not in ctes))
