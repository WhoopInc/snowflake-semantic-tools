"""Build the members attached to one view: metrics and their windows, filters, instructions, queries.

Every member expression resolves through the view's `_Resolver`, which knows how the view names
its tables and its metrics. Each builder raises `ProjectError` at the first member that does not
resolve, so a view reports one build problem at a time.
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import mapping
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SortKey,
    Variable,
    VerifiedQuery,
    Window,
)
from snowflake_semantic_tools.domain.parse.template import scan_template_calls, single_template_call
from snowflake_semantic_tools.domain.resolve.template import (
    FILTER_EXPR,
    METRIC_EXPR,
    VQR_SQL,
    RefPolicy,
    ResolveContext,
    resolve_scalar,
)
from snowflake_semantic_tools.domain.sql import AuthoredExpression, AuthoredQuery
from snowflake_semantic_tools.domain.validate.sql import checked_expression, checked_query, name_problem


def _require(found: Diagnostic | None) -> None:
    """Raise the diagnostic a check returned, if it returned one.

    Raises:
        ProjectError: `found` is a diagnostic; the error carries it.
    """
    if found is not None:
        raise ProjectError(found.message, diagnostics=(found,))


def _expression(text: str, *, kind: str, name: str, subject: str, origin: Origin | None) -> AuthoredExpression:
    """Guard one resolved member expression before any model holds it.

    Raises:
        ProjectError: the guard refused the expression (SST-VAL418).
    """
    guarded = checked_expression(text, kind=kind, name=name, subject=subject, origin=origin)
    if isinstance(guarded, Diagnostic):
        raise ProjectError(guarded.message, diagnostics=(guarded,))
    return guarded


def _names(values: tuple[str, ...], subject: str, origin: Origin | None) -> tuple[str, ...]:
    """Upper-case each name, once each is known to render as one identifier.

    Raises:
        ProjectError: a name is not a valid identifier (SST-PRS005).
    """
    for value in values:
        _require(name_problem(value, artifact=subject, subject=subject, origin=origin))
    return tuple(value.upper() for value in values)


def _resolve_expression(
    text: str,
    *,
    policy: RefPolicy,
    origin: Origin,
    catalog: DbtCatalog,
    logical_by_model: Mapping[str, str],
    metric_names: Mapping[str, str],
    instruction_names: frozenset[str],
    variables: Mapping[str, object],
    field: str,
) -> str:
    """Resolve every template call in one member expression, or raise with what did not resolve.

    A one-argument `ref()` becomes the table's logical name in the view, and a `metric()` the
    metric's name as the view qualifies it.

    Raises:
        ProjectError: `resolve_scalar` reported a diagnostic; the error carries every one.
    """
    context = ResolveContext(
        catalog,
        metric_names=frozenset(metric_names),
        metric_values=metric_names,
        instruction_names=instruction_names,
        variables=variables,
    )
    resolved, diagnostics = resolve_scalar(
        text,
        policy,
        origin,
        context,
        field=field,
        ref_value=lambda call: logical_by_model.get(call.args[0].casefold(), call.raw),
    )
    if diagnostics:
        raise ProjectError(
            "; ".join(diagnostic.message for diagnostic in diagnostics),
            diagnostics=tuple(diagnostics),
        )
    return resolved.text


@dataclass(frozen=True, slots=True)
class _Resolver:
    """How one view resolves its members' metric and filter expressions.

    Attributes:
        view_key: The view's artifact key, which a member's build diagnostic names.
        path: The view's file: the origin of an expression whose member records none.
        logical_by_model: Each table's logical name, by lower-cased model name.
        metric_names: Each attached metric's name as the view qualifies it, by casefolded name.
        instruction_names: The casefolded names of the custom instructions attached to the view.
    """

    view_key: str
    path: Path
    catalog: DbtCatalog
    logical_by_model: Mapping[str, str]
    metric_names: Mapping[str, str]
    instruction_names: frozenset[str]
    variables: Mapping[str, object]

    def resolve(self, text: str, policy: RefPolicy, origin: Origin | None, field: str) -> str:
        """Resolve one member expression; see `_resolve_expression`."""
        return _resolve_expression(
            text,
            policy=policy,
            origin=origin or Origin(str(self.path)),
            catalog=self.catalog,
            logical_by_model=self.logical_by_model,
            metric_names=self.metric_names,
            instruction_names=self.instruction_names,
            variables=self.variables,
            field=field,
        )


def _metric_table(metric: MetricDef, logical_by_model: Mapping[str, str]) -> str | None:
    """Return the logical name of the one table a metric renders under; None renders it at view level."""
    referenced = metric.tables or metric.referenced_models
    return logical_by_model[referenced[0]] if len(referenced) == 1 else None


def _metric_names(metrics: tuple[MetricDef, ...], logical_by_model: Mapping[str, str]) -> dict[str, str]:
    """Name each metric as the view's expressions reach it: `TABLE.METRIC`, or `METRIC` at view level.

    Every name is known before any expression resolves, so a metric may reference one attached
    after it.
    """
    names: dict[str, str] = {}
    for metric in metrics:
        owner = _metric_table(metric, logical_by_model)
        names[metric.name.casefold()] = metric.name.upper() if owner is None else f"{owner}.{metric.name.upper()}"
    return names


def _view_metrics(
    metrics: tuple[MetricDef, ...], relationships: tuple[Relationship, ...], resolver: _Resolver
) -> list[Metric]:
    """Build each attached metric in attachment order, stopping at the first that does not resolve."""
    return [_view_metric(metric, relationships, resolver) for metric in metrics]


def _view_metric(metric: MetricDef, relationships: tuple[Relationship, ...], resolver: _Resolver) -> Metric:
    """Build one metric: its non-additive tables first, then its expression, then its window.

    Diagnostics:
        SST-VAL118: when a non-additive dimension's `table:` is not one of the view's tables.

    Raises:
        ProjectError: A non-additive table is outside the view, or the expression or the window
            does not resolve.
    """
    owner = _metric_table(metric, resolver.logical_by_model)
    outside = next(
        (
            entry
            for entry in metric.non_additive
            if entry.table and entry.table.casefold() not in resolver.logical_by_model
        ),
        None,
    )
    if outside is not None:
        diagnostic = D(
            "SST-VAL118",
            metric=metric.name,
            value=f"{outside.table}.{outside.dimension}",
            subject=resolver.view_key,
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    subject = artifact_key("metric", metric.name.casefold())
    (name,) = _names((metric.name,), subject, metric.origin)
    resolved = resolver.resolve(metric.expr, METRIC_EXPR, metric.origin, "metric.expression")
    expr = _expression(resolved, kind="metric", name=metric.name, subject=subject, origin=metric.origin)
    window: Window | None = None
    if metric.window is not None and owner is not None:
        window = _view_window(metric, owner, relationships, resolver)
    return Metric(
        name=name,
        expr=expr,
        table=owner,
        comment=metric.description,
        synonyms=metric.synonyms,
        using_relationships=_names(metric.using_relationships, subject, metric.origin),
        non_additive_by=tuple(
            _sort_key(entry.names, entry.descending, entry.nulls_first, metric, subject)
            for entry in metric.non_additive
        ),
        access_modifier=metric.access_modifier,
        window=window,
    )


def _sort_key(
    names: tuple[str, ...], descending: bool | None, nulls_first: bool | None, metric: MetricDef, subject: str
) -> SortKey:
    """Build one `NON ADDITIVE BY` key from the names it is written from, each checked first.

    Raises:
        ProjectError: a name is not a valid identifier (SST-PRS005).
    """
    text = ".".join(_names(names, subject, metric.origin))
    return SortKey(
        _expression(text, kind="metric", name=metric.name, subject=subject, origin=metric.origin),
        descending,
        nulls_first,
    )


def _view_window(metric: MetricDef, owner: str, relationships: tuple[Relationship, ...], resolver: _Resolver) -> Window:
    """Resolve a window metric's window once every dimension it names is reachable from its table.

    Entries resolve in clause order: `partition_by`, `partition_by_excluding`, `order_by`.

    Raises:
        ProjectError: A dimension is unreachable (SST-VAL125), or an entry does not resolve.
    """
    window = metric.window
    assert window is not None
    _require_reachable(metric, owner, relationships, resolver.logical_by_model, resolver.view_key)

    subject = artifact_key("metric", metric.name.casefold())

    def resolve(text: str) -> AuthoredExpression:
        resolved = resolver.resolve(text, METRIC_EXPR, metric.origin, "metric.window")
        return _expression(resolved, kind="metric", name=metric.name, subject=subject, origin=metric.origin)

    return Window(
        partition_by=tuple(resolve(text) for text in window.partition_by),
        partition_excluding=tuple(resolve(text) for text in window.partition_excluding),
        order_by=tuple(SortKey(resolve(entry.ref), entry.descending, entry.nulls_first) for entry in window.order_by),
        frame=(
            _expression(window.frame, kind="metric", name=metric.name, subject=subject, origin=metric.origin)
            if window.frame
            else None
        ),
    )


def _require_reachable(
    metric: MetricDef,
    owner: str,
    relationships: tuple[Relationship, ...],
    logical_by_model: Mapping[str, str],
    subject: str,
) -> None:
    """Require each window dimension to be one the metric's table reaches through the view's joins.

    Diagnostics:
        SST-VAL125: when a window entry names a dimension of a table the metric's table cannot reach.

    Raises:
        ProjectError: A window dimension is unreachable.
    """
    reached = {owner}
    frontier = [owner]
    while frontier:
        table = frontier.pop()
        for relationship in relationships:
            if relationship.from_table == table and relationship.to_table not in reached:
                reached.add(relationship.to_table)
                frontier.append(relationship.to_table)
    assert metric.window is not None
    for field, text in metric.window.references():
        call = single_template_call(text, "ref")
        if call is None or len(call.args) != 2:
            continue
        if logical_by_model.get(call.args[0].casefold()) not in reached:
            diagnostic = D(
                "SST-VAL125",
                metric=metric.name,
                field=field,
                value=text,
                expected=f"a dimension {owner} reaches in this view",
                subject=subject,
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _view_filters(filters: tuple[FilterDef, ...], resolver: _Resolver) -> tuple[list[Column], list[FilterDef]]:
    """Build each labelled filter as a filter column, and set the unlabelled ones aside as prose.

    Raises:
        ProjectError: A labelled filter does not resolve to exactly one table, or its expression
            does not resolve.
    """
    entity_filters: list[Column] = []
    standalone_filters: list[FilterDef] = []
    for filter_def in filters:
        if not filter_def.entity_level:
            standalone_filters.append(filter_def)
            continue
        entity_filters.append(_entity_filter(filter_def, resolver))
    return entity_filters, standalone_filters


def _entity_filter(filter_def: FilterDef, resolver: _Resolver) -> Column:
    """Build one labelled filter as a filter column of its table: its `tables:` entry, else its ref.

    Raises:
        ProjectError: The filter does not resolve to exactly one table, or its expression does
            not resolve.
    """
    referenced = filter_def.tables or tuple(
        call.args[0] for call in scan_template_calls(filter_def.expr) if call.function == "ref" and call.args
    )
    if len(referenced) != 1:
        raise ProjectError(f"filter {filter_def.name!r} must resolve to exactly one table")
    model_name = referenced[0].casefold()
    subject = artifact_key("filter", filter_def.name.casefold())
    (name,) = _names((filter_def.name,), subject, filter_def.origin)
    resolved = resolver.resolve(filter_def.expr, FILTER_EXPR, filter_def.origin, "filter.expression")
    return Column(
        table=resolver.logical_by_model[model_name],
        name=name,
        kind=ColumnKind.FILTER,
        expr=_expression(resolved, kind="filter", name=filter_def.name, subject=subject, origin=filter_def.origin),
        comment=filter_def.description,
    )


def _instruction_parts(
    instructions: tuple[InstructionDef, ...],
    standalone_filters: list[FilterDef],
    logical_by_model: Mapping[str, str],
) -> tuple[list[str], list[str]]:
    """Collect the text of the view's two instruction channels: SQL generation and categorization.

    The SQL channel takes each instruction's text in attachment order, then one sentence per
    unlabelled filter.

    Raises:
        ProjectError: An unlabelled filter is not attached to exactly one table.
    """
    sql_parts = [item.ai_sql_generation for item in instructions if item.ai_sql_generation]
    sql_parts.extend(_standalone_filter_instruction(item, logical_by_model) for item in standalone_filters)
    question_parts = [item.ai_question_categorization for item in instructions if item.ai_question_categorization]
    return sql_parts, question_parts


def _standalone_filter_instruction(filter_def: FilterDef, logical_by_model: Mapping[str, str]) -> str:
    if len(filter_def.tables) != 1:
        raise ProjectError(f"standalone filter {filter_def.name!r} must attach to exactly one table")
    table = logical_by_model[filter_def.tables[0]]
    description = filter_def.description or ""
    return _wrap_text(f"For {table}, {filter_def.name} is {filter_def.expr}. {description}".strip())


def _wrap_text(text: str, width: int = 77) -> str:
    return "\n".join(textwrap.wrap(text, width=width, break_long_words=False, break_on_hyphens=False))


def _view_verified_queries(
    queries: tuple[VerifiedQueryDef, ...],
    logical_by_model: Mapping[str, str],
    metric_names: Mapping[str, str],
    config: dict[str, Any],
    catalog: DbtCatalog,
) -> tuple[VerifiedQuery, ...]:
    """Build each attached verified query, its SQL resolved against the view's tables and metrics.

    Raises:
        ProjectError: A query's SQL does not resolve in this view, such as a `metric()` of a metric
            the view does not hold; its name is not an identifier (SST-PRS005); or its resolved
            SQL is not one SELECT or WITH query (SST-VAL418).
    """
    built: list[VerifiedQuery] = []
    for query in queries:
        subject = artifact_key("verified_query", query.name.casefold())
        (name,) = _names((query.name,), subject, query.origin)
        resolved = _resolve_verified_query_sql(query, logical_by_model, metric_names, config, catalog)
        built.append(
            VerifiedQuery(
                name=name,
                question=query.question,
                sql=_query(resolved, name=query.name, subject=subject, origin=query.origin),
                verified_at=query.verified_at,
                verified_by=query.verified_by,
                onboarding_question=query.onboarding_question,
            )
        )
    return tuple(built)


def _query(text: str, *, name: str, subject: str, origin: Origin | None) -> AuthoredQuery:
    """Guard one resolved verified query before any model holds it.

    Raises:
        ProjectError: the guard refused the query (SST-VAL418).
    """
    guarded = checked_query(text, kind="verified_query", name=name, subject=subject, origin=origin)
    if isinstance(guarded, Diagnostic):
        raise ProjectError(guarded.message, diagnostics=(guarded,))
    return guarded


def _resolve_verified_query_sql(
    query: VerifiedQueryDef,
    logical_by_model: Mapping[str, str],
    resolved_metric_names: Mapping[str, str],
    config: dict[str, Any],
    catalog: DbtCatalog,
) -> str:
    variables: dict[str, object] = mapping(config.get("vars"))
    return _resolve_expression(
        query.sql,
        policy=VQR_SQL,
        origin=query.origin or Origin("<verified-query>"),
        catalog=catalog,
        logical_by_model=logical_by_model,
        metric_names=resolved_metric_names,
        instruction_names=frozenset(),
        variables=variables,
        field="verified_query.sql",
    )


def _with_variable_names(
    variables: tuple[Variable, ...], metrics: list[Metric], entity_filters: list[Column]
) -> tuple[list[Metric], list[Column]]:
    """Upper-case each view variable's name wherever a metric or a filter column's expression uses it.

    Variables apply in declaration order; a name matches case-insensitively, as a whole word.
    """
    for variable in variables:
        metrics = [_replace_metric_variable_name(metric, variable.name) for metric in metrics]
        entity_filters = [
            _replace_column_variable_name(entity_filter, variable.name) for entity_filter in entity_filters
        ]
    return metrics, entity_filters


def _replace_metric_variable_name(metric: Metric, variable_name: str) -> Metric:
    return replace(metric, expr=_upper_variable(metric.expr, variable_name, "metric", metric.name))


def _replace_column_variable_name(column: Column, variable_name: str) -> Column:
    return replace(column, expr=_upper_variable(column.expr, variable_name, column.kind.value, column.name))


def _upper_variable(expression: AuthoredExpression, variable_name: str, kind: str, name: str) -> AuthoredExpression:
    """Upper-case a variable's name where an expression uses it, and guard the result again.

    Changing a word's case cannot change what the guard decides, so the new guard always passes.
    """
    text = re.sub(rf"\b{re.escape(variable_name)}\b", variable_name.upper(), expression.text, flags=re.IGNORECASE)
    return _expression(text, kind=kind, name=name, subject=artifact_key(kind, name.casefold()), origin=None)
