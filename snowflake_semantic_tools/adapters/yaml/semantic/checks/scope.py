"""Read a view's scope -- the members it lists or excludes -- and check it before the view is built.

A view exposes every member its tables attach unless it narrows them. For each of three
kinds it may list what to keep or what to drop, never both:

    columns / exclude_columns               {{ ref('<model>', '<column>') }} entries
    metrics / exclude_metrics               {{ metric('<name>') }} entries, or bare names
    relationships / exclude_relationships   relationship names

A scope that names nothing, or that would leave a kept metric without the relationship or
the dimension it needs, is an error rather than a member quietly left out.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, ViewScope
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, single_template_call

# Each kind's include key, exclude key, and the word a diagnostic names its members by.
SCOPE_KINDS: tuple[tuple[str, str, str], ...] = (
    ("columns", "exclude_columns", "column"),
    ("metrics", "exclude_metrics", "metric"),
    ("relationships", "exclude_relationships", "relationship"),
)
SCOPE_KEYS = frozenset(key for include, exclude, _ in SCOPE_KINDS for key in (include, exclude))


@dataclass(frozen=True, slots=True)
class ScopeEntry:
    """One entry of a scope list, as written and as read.

    Attributes:
        field: The list it is in, such as `exclude_columns`.
        kind: `column`, `metric` or `relationship`.
        model: For a column, its model, casefolded; None otherwise.
        name: The column, metric or relationship it names, casefolded; for an entry that
            cannot be read, the entry as written.
        readable: False when the entry is not of its list's form.
    """

    field: str
    kind: str
    model: str | None
    name: str
    readable: bool = True

    @property
    def label(self) -> str:
        """The entry as a diagnostic names it: `<model>.<column>` for a column."""
        return f"{self.model}.{self.name}" if self.model else self.name


def scope_entries(node: Mapping[str, object]) -> tuple[ScopeEntry, ...]:
    """Every entry of the view's scope lists, list by list in `SCOPE_KINDS` order.

    A list that is not a list holds no entries; `_shape_diagnostics` reports it.
    """
    return tuple(
        _entry(field, kind, raw)
        for include, exclude, kind in SCOPE_KINDS
        for field in (include, exclude)
        for raw in _list(node.get(field))
    )


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _entry(field: str, kind: str, raw: object) -> ScopeEntry:
    """Read one scope-list entry in the form its kind takes.

    A column is a two-argument `ref()`, a metric a `metric()` call or a bare name, and a
    relationship a bare name. An entry of any other form is kept unreadable, as written, so
    the check can name it.
    """
    text = str(raw).strip()
    try:
        if kind == "column":
            call = single_template_call(text, "ref")
            if call is not None and len(call.args) == 2:
                return ScopeEntry(field, kind, call.args[0].casefold(), call.args[1].casefold())
            return ScopeEntry(field, kind, None, text, readable=False)
        if kind == "metric":
            call = single_template_call(text, "metric")
            if call is not None and len(call.args) == 1:
                return ScopeEntry(field, kind, None, call.args[0].casefold())
    except TemplateSyntaxError:
        return ScopeEntry(field, kind, None, text, readable=False)
    readable = bool(text) and "{" not in text and "." not in text
    return ScopeEntry(field, kind, None, text.casefold() if readable else text, readable=readable)


def view_scope(node: Mapping[str, object]) -> ViewScope:
    """The view's scope as the model holds it: each readable entry, upper-cased.

    A column is named under its logical table, which is its model's name upper-cased.
    """

    def names(field: str) -> tuple[str, ...]:
        return tuple(entry.label.upper() for entry in scope_entries(node) if entry.field == field and entry.readable)

    def included(field: str) -> tuple[str, ...] | None:
        return names(field) if isinstance(node.get(field), list) else None

    return ViewScope(
        columns=included("columns"),
        exclude_columns=names("exclude_columns"),
        metrics=included("metrics"),
        exclude_metrics=names("exclude_metrics"),
        relationships=included("relationships"),
        exclude_relationships=names("exclude_relationships"),
    )


@dataclass(frozen=True, slots=True)
class _ScopeContext:
    """What one view's scope is checked against.

    Attributes:
        tables: The view's tables, casefolded.
        metrics: Every metric of the project, by casefolded name.
        relationships: Every relationship of the project, by casefolded name.
    """

    view: ParsedView
    tables: frozenset[str]
    models: Mapping[str, DbtModel]
    metrics: Mapping[str, MetricDef]
    relationships: Mapping[str, Relationship]

    @property
    def key(self) -> str:
        return artifact_key("semantic_view", self.view.name)


def _scope_diagnostics(
    views: tuple[ParsedView, ...],
    metrics: tuple[MetricDef, ...],
    relationships: tuple[Relationship, ...],
    models: Mapping[str, DbtModel],
) -> tuple[Diagnostic, ...]:
    """Check every readable view's scope, view by view in declaration order.

    For each view: the lists' shape and modes, then each entry in list order, then each
    metric the scope keeps against the relationships and columns it removes.

    Diagnostics:
        SST-PRS003: a scope key is not a list.
        SST-VAL329: one kind declares both its include and its exclude list.
        SST-VAL330: an entry names no column, metric or relationship the view's tables
            provide, or `columns` names a column excluded globally.
        SST-VAL331: `exclude_columns` names a column already excluded globally.
        SST-VAL203: a listed relationship names a table the view does not hold.
        SST-VAL332: a kept metric's `using_relationships` names a relationship the scope
            removes, or one whose tables the view does not hold; a view with no scope
            lists is checked for the second.
        SST-VAL318: a kept window or non-additive metric names a column the scope removes.
    """
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    relationship_by_name = {relationship.name.casefold(): relationship for relationship in relationships}
    diagnostics: list[Diagnostic] = []
    for view in views:
        if view.poisoned:
            continue
        context = _ScopeContext(view, frozenset(view.declared_tables), models, metric_by_name, relationship_by_name)
        diagnostics.extend(_shape_diagnostics(context))
        diagnostics.extend(
            found for entry in scope_entries(view.source) if (found := _entry_problem(entry, context)) is not None
        )
        diagnostics.extend(_kept_metric_diagnostics(context))
    return tuple(diagnostics)


def _shape_diagnostics(context: _ScopeContext) -> Iterator[Diagnostic]:
    """Report each scope key that is not a list, then each kind that declares both modes."""
    node, key = context.view.source, context.key
    for field in sorted(SCOPE_KEYS & set(node), key=_key_order):
        if not isinstance(node[field], list):
            yield D(
                "SST-PRS003",
                artifact=key,
                field=field,
                expected="a list",
                found=type(node[field]).__name__,
                subject=key,
                origin=context.view.origin,
            )
    for include, exclude, kind in SCOPE_KINDS:
        if include in node and exclude in node:
            yield D(
                "SST-VAL329",
                artifact=key,
                field=include,
                other=exclude,
                kind=f"{kind}s",
                subject=key,
                origin=context.view.origin,
            )


def _key_order(field: str) -> int:
    return [key for include, exclude, _ in SCOPE_KINDS for key in (include, exclude)].index(field)


def _entry_problem(entry: ScopeEntry, context: _ScopeContext) -> Diagnostic | None:
    """Report what one entry names that the view cannot scope, or return None when it fits."""
    if not entry.readable:
        form = "two-argument ref() call" if entry.kind == "column" else f"{entry.kind} name"
        return _unknown(entry, context, f"is not a {form}")
    if entry.kind == "column":
        return _column_problem(entry, context)
    if entry.kind == "metric":
        metric = context.metrics.get(entry.name)
        if metric is None:
            return _unknown(entry, context, "does not exist")
        if not _metric_tables(metric, context.metrics) <= context.tables:
            return _unknown(entry, context, "does not attach to this view")
        return None
    relationship = context.relationships.get(entry.name)
    if relationship is None:
        return _unknown(entry, context, "does not exist")
    absent = sorted({relationship.from_table.casefold(), relationship.to_table.casefold()} - context.tables)
    if absent:
        return D(
            "SST-VAL203",
            relationship=entry.name,
            name=absent[0],
            artifact=context.key,
            subject=context.key,
            origin=context.view.origin,
        )
    return None


def _column_problem(entry: ScopeEntry, context: _ScopeContext) -> Diagnostic | None:
    """Report a column entry that is not one of the view's facts or dimensions."""
    assert entry.model is not None
    if entry.model not in context.tables:
        return _unknown(entry, context, "is not on a table of this view")
    model = context.models.get(entry.model)
    column = model.column(entry.name) if model is not None else None
    if column is None:
        return _unknown(entry, context, f"does not exist on '{entry.model}'")
    if column.excluded and entry.field == "exclude_columns":
        return D(
            "SST-VAL331",
            artifact=context.key,
            column=entry.label,
            subject=context.key,
            origin=context.view.origin,
        )
    if column.excluded:
        return _unknown(entry, context, "is excluded globally, so no view can include it")
    if column.column_type is None:
        return _unknown(entry, context, "declares no column_type, so it is not a member")
    return None


def _unknown(entry: ScopeEntry, context: _ScopeContext, reason: str) -> Diagnostic:
    return D(
        "SST-VAL330",
        artifact=context.key,
        field=entry.field,
        kind=entry.kind,
        name=entry.label,
        reason=reason,
        subject=context.key,
        origin=context.view.origin,
    )


def _metric_tables(
    metric: MetricDef, metrics: Mapping[str, MetricDef], seen: frozenset[str] = frozenset()
) -> frozenset[str]:
    """The tables a metric needs a view to hold: its own, and those of the metrics it reads."""
    tables = set(metric.tables or metric.referenced_models)
    for name in metric.referenced_metrics:
        referenced = metrics.get(name)
        if referenced is not None and name not in seen:
            tables |= _metric_tables(referenced, metrics, seen | {metric.name.casefold()})
    return frozenset(tables)


def _kept_metric_diagnostics(context: _ScopeContext) -> Iterator[Diagnostic]:
    """Report what each metric the scope keeps needs and the scope removes.

    A metric is kept when it attaches to the view and the scope admits it. Its
    `using_relationships` must name relationships the view holds and the relationship scope
    keeps, and the columns its window
    and non-additive dimensions name must survive the column scope: Snowflake resolves
    those against the view's dimensions, so a removed column would fail the view.
    """
    scope = view_scope(context.view.source)
    for name in sorted(context.metrics):
        metric = context.metrics[name]
        if not _metric_tables(metric, context.metrics) <= context.tables or not scope.admits_metric(metric.name):
            continue
        for relationship in metric.using_relationships:
            if not scope.admits_relationship(relationship) or not _held(relationship, context):
                yield D(
                    "SST-VAL332",
                    artifact=context.key,
                    metric=metric.name,
                    relationship=relationship.casefold(),
                    subject=context.key,
                    origin=context.view.origin,
                )
        for column in _dimension_columns(metric):
            if not scope.admits_column_name(column):
                yield D(
                    "SST-VAL318",
                    artifact=context.key,
                    member=metric.name,
                    column=column,
                    subject=context.key,
                    origin=context.view.origin,
                )


def _held(name: str, context: _ScopeContext) -> bool:
    """Report whether the view holds both tables of the relationship named `name`.

    A relationship that is not declared is held: SST-VAL214 reports it.
    """
    relationship = context.relationships.get(name.casefold())
    if relationship is None:
        return True
    return {relationship.from_table.casefold(), relationship.to_table.casefold()} <= context.tables


def _dimension_columns(metric: MetricDef) -> tuple[str, ...]:
    """The columns a metric's non-additive and window entries sort or partition by.

    Each is `<model>.<column>`, casefolded, once each in the order the metric names it.
    """
    found: list[str] = []
    owner = (metric.tables or metric.referenced_models or (None,))[0]
    for entry in metric.non_additive:
        model = (entry.table or owner or "").casefold()
        found.append(f"{model}.{entry.dimension.casefold()}")
    for _, text in metric.window.references() if metric.window is not None else ():
        try:
            call = single_template_call(text, "ref")
        except TemplateSyntaxError:
            continue
        if call is not None and len(call.args) == 2:
            found.append(f"{call.args[0].casefold()}.{call.args[1].casefold()}")
    return tuple(dict.fromkeys(found))
