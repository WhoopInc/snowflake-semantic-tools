"""Read a view's scope -- the members it lists or excludes -- and check it before the view is built.

A view exposes every member its tables attach unless it narrows them. For each of three
kinds it may list what to keep or what to drop, never both:

    columns / exclude_columns               {{ ref('<model>', '<column>') }} entries
    metrics / exclude_metrics               {{ metric('<name>') }} entries
    relationships / exclude_relationships   {{ relationship('<name>') }} entries

Each entry is one call of its kind's function; a bare name is refused, naming the function.

A scope that names nothing, or that would leave a kept metric without the relationship, the
dimension or the base metric it needs, is an error rather than a member quietly left out. So
is a kept metric, an attached filter or an attached verified query whose `ref()` reads a column
the scope removes: hiding a column must not leave its data reachable through another member.
Bare column names in a verified query's SQL are not read, so they are not checked.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, view_key
from snowflake_semantic_tools.domain.model.authored import FilterDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, ViewScope
from snowflake_semantic_tools.domain.parse.template import (
    TemplateCall,
    TemplateSyntaxError,
    scan_template_calls,
    single_template_call,
)
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.membership import ViewMembers
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions

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
        refusal: Why the entry is not one call of its kind's function, as `required_call`
            reports it; None when it is one, so an unreadable entry without one is a column
            `ref()` that names no column.
    """

    field: str
    kind: str
    model: str | None
    name: str
    readable: bool = True
    refusal: Diagnostic | None = None

    @property
    def label(self) -> str:
        """The entry as a diagnostic names it: `<model>.<column>` for a column."""
        return f"{self.model}.{self.name}" if self.model else self.name


def scope_entries(
    node: Mapping[str, object], origin: Origin | None = None, subject: str = ""
) -> tuple[ScopeEntry, ...]:
    """Every entry of the view's scope lists, list by list in `SCOPE_KINDS` order.

    A list that is not a list holds no entries; `_shape_diagnostics` reports it.

    Args:
        origin, subject: Where an entry's refusal is reported, and against which view.
    """
    return tuple(
        _entry(field, kind, raw, origin, subject)
        for include, exclude, kind in SCOPE_KINDS
        for field in (include, exclude)
        for raw in _list(node.get(field))
    )


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _entry(field: str, kind: str, raw: object, origin: Origin | None, subject: str) -> ScopeEntry:
    """Read one scope-list entry: one call of its kind's function.

    A column is a two-argument `ref()`, a metric a `metric()` call and a relationship a
    `relationship()` call. An entry of any other form is kept unreadable, as written, with why.
    """
    text = str(raw).strip()
    functions = reference_functions(f"semantic_view.{kind}s")
    found = required_call(text, functions, origin, field=field, artifact=subject, subject=subject)
    if isinstance(found, Diagnostic):
        return ScopeEntry(field, kind, None, text, readable=False, refusal=found)
    if kind != "column":
        return ScopeEntry(field, kind, None, found.args[0].casefold())
    if len(found.args) == 2:
        return ScopeEntry(field, kind, found.args[0].casefold(), found.args[1].casefold())
    return ScopeEntry(field, kind, None, text, readable=False)


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
        members: What member resolution attaches to each view, and what its scope keeps.
        readers: Every filter and verified query of the project, by member key, in parse order.
    """

    view: ParsedView
    tables: frozenset[str]
    models: Mapping[str, DbtModel]
    metrics: Mapping[str, MetricDef]
    relationships: Mapping[str, Relationship]
    members: ViewMembers
    readers: Mapping[str, FilterDef | VerifiedQueryDef]

    @property
    def key(self) -> str:
        return view_key(self.view.name)


def scope_diagnostics(
    views: tuple[ParsedView, ...],
    metrics: tuple[MetricDef, ...],
    relationships: tuple[Relationship, ...],
    models: Mapping[str, DbtModel],
    members: ViewMembers,
    readers: tuple[FilterDef | VerifiedQueryDef, ...] = (),
) -> tuple[Diagnostic, ...]:
    """Check every readable view's scope, view by view in declaration order.

    For each view: the lists' shape and modes, then each entry in list order, then each
    metric the scope keeps against the relationships, columns and base metrics it removes, then
    each attached filter and verified query against the columns it removes. A member attaches
    to a view, and is kept by it, as `members` says.

    Args:
        readers: The project's filters and verified queries, whose `ref()` columns are checked.

    Diagnostics:
        SST-PRS003: a scope key is not a list.
        SST-VAL329: one kind declares both its include and its exclude list.
        Every code `required_call` lists, for an entry that is not one call of its kind's
            function: `ref()`, `metric()` or `relationship()`.
        SST-VAL330: an entry names no column, metric or relationship the view's tables
            provide, a column `ref()` names no column, or `columns` names a column excluded
            globally.
        SST-VAL331: `exclude_columns` names a column already excluded globally.
        SST-VAL203: a listed relationship names a table the view does not hold.
        SST-VAL332: a kept metric's `using_relationships` names a relationship the scope
            removes, or one whose tables the view does not hold; a view with no scope
            lists is checked for the second.
        SST-VAL336: a kept metric is built, with `metric()`, on a metric the view reaches and
            its scope removes.
        SST-VAL318: a kept metric's expression, window or non-additive entry, or an attached
            filter's or verified query's `ref()`, names a column the scope removes.
    """
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    relationship_by_name = {relationship.name.casefold(): relationship for relationship in relationships}
    reader_by_key = {_reader_key(reader): reader for reader in readers}
    diagnostics: list[Diagnostic] = []
    for view in views:
        if view.poisoned:
            continue
        context = _ScopeContext(
            view, frozenset(view.declared_tables), models, metric_by_name, relationship_by_name, members, reader_by_key
        )
        diagnostics.extend(_shape_diagnostics(context))
        entries = scope_entries(view.source, view.origin, context.key)
        diagnostics.extend(found for entry in entries if (found := _entry_problem(entry, context)) is not None)
        diagnostics.extend(_kept_metric_diagnostics(context))
        diagnostics.extend(_reader_diagnostics(context))
    return tuple(diagnostics)


def _reader_key(reader: FilterDef | VerifiedQueryDef) -> str:
    kind = "filter" if isinstance(reader, FilterDef) else "verified_query"
    return artifact_key(kind, reader.name.casefold())


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
    if entry.refusal is not None:
        return entry.refusal
    if not entry.readable:
        return _unknown(entry, context, "is not a two-argument ref() call")
    if entry.kind == "column":
        return _column_problem(entry, context)
    if entry.kind == "metric":
        metric = context.metrics.get(entry.name)
        if metric is None:
            return _unknown(entry, context, "does not exist")
        member = artifact_key("metric", entry.name)
        # A metric the load leaves out is reported already; naming it here is no second fault.
        if member not in context.members.poisoned and not context.members.reaches(context.key, member):
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


def _kept_metric_diagnostics(context: _ScopeContext) -> Iterator[Diagnostic]:
    """Report what each metric the scope keeps needs and the scope removes.

    A metric is kept when it attaches to the view and the scope admits it. Its
    `using_relationships` must name relationships the view holds and the relationship scope
    keeps. Each metric it is built on that the view reaches must be kept too, or the view would
    hold the metric without its base. And every column its expression reads, or its window
    and non-additive dimensions name, must survive the column scope: a window or non-additive
    column would fail the view, and an expression's would publish data the scope hides.
    """
    scope = view_scope(context.view.source)
    for name in sorted(context.metrics):
        metric = context.metrics[name]
        if not context.members.keeps(context.key, artifact_key("metric", name)):
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
        yield from _removed_bases(metric, context)
        for column in dict.fromkeys((*_column_reads(metric.calls), *_dimension_columns(metric))):
            if not scope.admits_column_name(column):
                yield _hidden_read(metric.name, column, context)


def _removed_bases(metric: MetricDef, context: _ScopeContext) -> Iterator[Diagnostic]:
    """Report each metric `metric` is built on that the view reaches and its scope removes.

    A base the view keeps is checked as a kept metric in its turn, so its own bases are
    reported against it, each once.
    """
    for base in metric.referenced_metrics:
        key = artifact_key("metric", base)
        if context.members.reaches(context.key, key) and not context.members.keeps(context.key, key):
            yield D(
                "SST-VAL336",
                artifact=context.key,
                metric=metric.name,
                other=base,
                subject=context.key,
                origin=context.view.origin,
            )


def _reader_diagnostics(context: _ScopeContext) -> Iterator[Diagnostic]:
    """Report each column an attached filter or verified query reads with `ref()` and the scope removes.

    A scope never removes either member, so each attached one is checked; its bare column
    names, as a verified query's SQL may write them, are not read.
    """
    scope = view_scope(context.view.source)
    for key, reader in context.readers.items():
        if not context.members.keeps(context.key, key):
            continue
        for column in _column_reads(_reader_calls(reader)):
            if not scope.admits_column_name(column):
                yield _hidden_read(key, column, context)


def _reader_calls(reader: FilterDef | VerifiedQueryDef) -> tuple[TemplateCall, ...]:
    """The calls a filter's expression or a verified query's SQL makes; none when one is malformed."""
    if reader.template_calls:
        return reader.template_calls
    try:
        return scan_template_calls(reader.expr if isinstance(reader, FilterDef) else reader.sql)
    except TemplateSyntaxError:
        return ()


def _hidden_read(member: str, column: str, context: _ScopeContext) -> Diagnostic:
    return D(
        "SST-VAL318",
        artifact=context.key,
        member=member,
        column=column,
        subject=context.key,
        origin=context.view.origin,
    )


def _column_reads(calls: tuple[TemplateCall, ...]) -> tuple[str, ...]:
    """The columns two-argument `ref()` calls read, each `<model>.<column>` casefolded, once each in order."""
    return tuple(
        dict.fromkeys(
            f"{call.args[0].casefold()}.{call.args[1].casefold()}"
            for call in calls
            if call.function == "ref" and len(call.args) == 2
        )
    )


def _held(name: str, context: _ScopeContext) -> bool:
    """Report whether the view holds both tables of the relationship named `name`.

    A relationship that is not declared is held: SST-REF029 reports it.
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
