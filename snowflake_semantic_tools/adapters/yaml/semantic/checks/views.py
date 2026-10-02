"""Check each view against what it attaches, before it is built: one rule per concern, view by view.

Attachment here is what a view's tables imply -- a metric or filter whose tables the view holds,
a relationship joining two of them -- narrowed by the view's scope. The rules read that, the dbt
models, and the view's own keys, so each can poison the view before a build would fail on it.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field

from snowflake_semantic_tools.adapters.yaml.semantic.checks.scope import _metric_tables, view_scope
from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, ViewScope
from snowflake_semantic_tools.domain.sql import is_datatype
from snowflake_semantic_tools.domain.validate.shared import lacks_invocation

DIMENSION_TYPES = frozenset(("dimension", "time_dimension"))
# Words an expression may hold bare that are SQL rather than names. SST does not parse SQL, so
# the list is incomplete by construction; a word it misses is only a warning (SST-VAL221).
SQL_WORDS = frozenset(
    [
        "all",
        "and",
        "any",
        "as",
        "asc",
        "between",
        "both",
        "by",
        "case",
        "cast",
        "current",
        "day",
        "days",
        "desc",
        "distinct",
        "else",
        "end",
        "epoch",
        "escape",
        "exists",
        "excluding",
        "false",
        "filter",
        "first",
        "following",
        "from",
        "group",
        "hour",
        "hours",
        "ignore",
        "ilike",
        "in",
        "interval",
        "is",
        "last",
        "like",
        "microsecond",
        "millisecond",
        "minute",
        "minutes",
        "month",
        "months",
        "nanosecond",
        "not",
        "null",
        "nulls",
        "of",
        "on",
        "or",
        "order",
        "over",
        "partition",
        "preceding",
        "quarter",
        "range",
        "respect",
        "rlike",
        "row",
        "rows",
        "second",
        "seconds",
        "some",
        "then",
        "true",
        "try_cast",
        "unbounded",
        "week",
        "weeks",
        "when",
        "within",
        "year",
        "years",
    ]
)
_MASKS = (
    re.compile(r"\{\{.*?\}\}", re.DOTALL),
    re.compile(r"'(?:''|[^'])*'"),
    re.compile(r"\"(?:\"\"|[^\"])*\""),
    re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL),
)
# A word that is neither qualified, a qualifier, nor a function name.
_BARE = re.compile(r"(?<![\w$.])([A-Za-z_][A-Za-z0-9_$]*)(?![\w$])(?!\s*[.(])")


def bare_identifiers(expression: str) -> tuple[str, ...]:
    """The bare words of an expression that could be names, each once in order.

    Template calls, string literals, quoted identifiers and comments are masked first, and SQL
    keywords and type names are left out.
    """
    text = expression
    for mask in _MASKS:
        text = mask.sub(" ", text)
    return tuple(
        dict.fromkeys(
            word for word in _BARE.findall(text) if word.casefold() not in SQL_WORDS and not is_datatype(word.upper())
        )
    )


@dataclass(frozen=True, slots=True)
class ViewInputs:
    """What every view is checked against: the project's members, models and settings.

    Attributes:
        models: The dbt models, by casefolded name.
        instructions: Every custom instruction, by casefolded name.
        view_instructions: Each view's attached instruction names, casefolded, by view key.
        unavailable: The dbt models that produce no relation, casefolded, with why.
        description_floor: `validation.description_floor`; None checks no floor.
        instruction_budget: `validation.instruction_budget`; None checks no budget.
    """

    models: Mapping[str, DbtModel]
    metrics: tuple[MetricDef, ...]
    filters: tuple[FilterDef, ...]
    relationships: tuple[Relationship, ...]
    instructions: Mapping[str, InstructionDef]
    view_instructions: Mapping[str, frozenset[str]]
    unavailable: Mapping[str, str] = field(default_factory=dict)
    description_floor: int | None = None
    instruction_budget: int | None = None


@dataclass(frozen=True, slots=True)
class _Attached:
    """One view and what its tables attach that its scope keeps."""

    view: ParsedView
    tables: frozenset[str]
    scope: ViewScope
    metrics: tuple[MetricDef, ...]
    filters: tuple[FilterDef, ...]
    relationships: tuple[Relationship, ...]

    @property
    def key(self) -> str:
        return artifact_key("semantic_view", self.view.name)


def _attached(view: ParsedView, inputs: ViewInputs) -> _Attached:
    """What a view's tables attach and its scope keeps: metrics, filters, and relationships.

    A metric attaches when the view holds every table it and the metrics it reads need, a
    filter when the view holds the tables it declares or refs, and a relationship when the view
    holds both its tables.
    """
    tables = frozenset(view.declared_tables)
    scope = view_scope(view.source)
    by_name = {metric.name.casefold(): metric for metric in inputs.metrics}
    return _Attached(
        view,
        tables,
        scope,
        tuple(
            metric
            for metric in inputs.metrics
            if _metric_tables(metric, by_name) <= tables and scope.admits_metric(metric.name)
        ),
        tuple(item for item in inputs.filters if _filter_tables(item) <= tables),
        tuple(
            item
            for item in inputs.relationships
            if {item.from_table.casefold(), item.to_table.casefold()} <= tables and scope.admits_relationship(item.name)
        ),
    )


def _filter_tables(filter_def: FilterDef) -> frozenset[str]:
    referenced = re.findall(r"\bref\(\s*['\"]([^'\"]+)['\"]", filter_def.expr)
    return frozenset((*filter_def.tables, *(name.casefold() for name in referenced)))


def _view_rule_diagnostics(views: tuple[ParsedView, ...], inputs: ViewInputs) -> tuple[Diagnostic, ...]:
    """Run every view rule on every readable view, view by view, then the rules across views.

    Diagnostics:
        SST-VAL005: a view's description never says when to use the view.
        SST-VAL004: a view's description is shorter than `validation.description_floor`.
        SST-VAL018: a view's description and instructions exceed `validation.instruction_budget`.
        SST-VAL206: a range relationship attaches and its target declares no `distinct_range`.
        SST-VAL219: a `distinct_range` column does not exist on its model.
        SST-VAL220: a declared variable is used by no expression the view attaches.
        SST-VAL221: an attached expression holds a bare word that names nothing SST knows.
        SST-VAL326: an attached expression holds a bare name another table or view provides.
        SST-VAL301: the view resolves no dimension and no metric.
        SST-VAL304: `max_staleness` is under 120 seconds.
        SST-VAL303: a table names a dbt model that is disabled or ephemeral.
        SST-VAL323: a table's dbt model declares no columns.
        SST-VAL324: a table's dbt model has neither a contract nor a test.
        SST-VAL322: two views give one table different descriptions.
    """
    readable = tuple(_attached(view, inputs) for view in views if not view.poisoned)
    variables = {
        str(entry.get("name")).casefold(): artifact_key("semantic_view", item.view.name)
        for item in readable
        for entry in _list(item.view.source.get("variables"))
        if isinstance(entry, dict) and entry.get("name")
    }
    diagnostics: list[Diagnostic] = []
    for item in readable:
        diagnostics.extend(_prose_rules(item, inputs))
        diagnostics.extend(_table_rules(item, inputs))
        diagnostics.extend(_expression_rules(item, inputs, variables))
        diagnostics.extend(_member_rules(item, inputs))
    diagnostics.extend(_shared_table_descriptions(readable))
    return tuple(diagnostics)


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, dict) else {}


def _prose_rules(item: _Attached, inputs: ViewInputs) -> Iterator[Diagnostic]:
    """Report a description that never says when to use its view or is short, then an over-budget surface.

    A view is routed: the agent's Analyst tool chooses between views by their descriptions.
    """
    description = str(item.view.source.get("description") or "").strip()
    if lacks_invocation(description):
        yield D("SST-VAL005", origin=item.view.origin, subject=item.key, type="semantic_view", name=item.view.name)
    if inputs.description_floor is not None and description and len(description) < inputs.description_floor:
        yield D(
            "SST-VAL004",
            origin=item.view.origin,
            subject=item.key,
            type="semantic_view",
            name=item.view.name,
            size=len(description),
            expected=inputs.description_floor,
        )
    if inputs.instruction_budget is None:
        return
    names = inputs.view_instructions.get(item.key, frozenset())
    parts = [description] + [
        text
        for name in sorted(names)
        if (instruction := inputs.instructions.get(name)) is not None
        for text in (instruction.ai_sql_generation, instruction.ai_question_categorization)
        if text
    ]
    size = len("\n\n".join(part for part in parts if part))
    if size > inputs.instruction_budget:
        yield D(
            "SST-VAL018",
            origin=item.view.origin,
            subject=item.key,
            artifact=item.key,
            size=size,
            expected=inputs.instruction_budget,
        )


def _table_rules(item: _Attached, inputs: ViewInputs) -> Iterator[Diagnostic]:
    """Report what each table's model lacks, then each range join whose target declares no range.

    Table by table: a model that produces no relation, one with no columns, one with no contract
    and no tests, then a `distinct_range` bound naming a column the model does not have.
    """
    config = _mapping(item.view.source.get("table_config"))
    for table in item.view.declared_tables:
        model = inputs.models.get(table)
        if model is None:
            if table in inputs.unavailable:
                yield D(
                    "SST-VAL303",
                    origin=item.view.origin,
                    subject=item.key,
                    artifact=item.key,
                    name=table,
                    found=inputs.unavailable[table],
                )
            continue
        if not model.columns:
            yield D("SST-VAL323", origin=item.view.origin, subject=item.key, artifact=item.key, name=model.name)
        if not model.contract_enforced and not model.tested:
            yield D(
                "SST-VAL324",
                origin=item.view.origin,
                subject=item.key,
                artifact=item.key,
                name=model.name,
                detail="no contract and no tests",
            )
        per_table = _mapping(config.get(model.name)) or _mapping(config.get(table))
        bounds = _mapping(per_table.get("distinct_range"))
        for bound in ("start", "end"):
            column = bounds.get(bound)
            if isinstance(column, str) and column and model.column(column) is None:
                yield D(
                    "SST-VAL219",
                    origin=item.view.origin,
                    subject=item.key,
                    artifact=item.key,
                    model=model.name,
                    field=f"distinct_range.{bound}",
                    column=column,
                )
    ranged = {str(name).casefold() for name, value in config.items() if _mapping(value).get("distinct_range")}
    for relationship in item.relationships:
        if relationship.range_bounds is not None and relationship.to_table.casefold() not in ranged:
            yield D(
                "SST-VAL206",
                origin=item.view.origin,
                subject=item.key,
                relationship=relationship.name.casefold(),
                name=relationship.to_table.casefold(),
            )


def _expression_rules(item: _Attached, inputs: ViewInputs, variables: Mapping[str, str]) -> Iterator[Diagnostic]:
    """Report each bare name an attached expression cannot resolve, then each unused variable.

    A metric's bare column of its own table is left to SST-VAL110, and a filter that renders as
    prose is read only for the variables it uses.
    """
    declared = {
        str(entry.get("name")).casefold(): str(entry.get("name"))
        for entry in _list(item.view.source.get("variables"))
        if isinstance(entry, dict) and entry.get("name")
    }
    view_columns = {
        column.name.casefold()
        for table in item.tables
        if (model := inputs.models.get(table))
        for column in model.columns
    }
    other_columns = {
        column.name.casefold()
        for name, model in inputs.models.items()
        if name not in item.tables
        for column in model.columns
    }
    members: list[tuple[str, str, bool]] = [(metric.name, metric.expr, True) for metric in item.metrics]
    members.extend((item_filter.name, item_filter.expr, item_filter.entity_level) for item_filter in item.filters)
    used: set[str] = set()
    for member, expression, checked in members:
        for word in bare_identifiers(expression):
            folded = word.casefold()
            if folded in declared:
                used.add(folded)
            elif not checked or folded in view_columns:
                continue
            elif folded in variables or folded in other_columns:
                yield D(
                    "SST-VAL326",
                    origin=item.view.origin,
                    subject=item.key,
                    view=item.view.name,
                    member=member,
                    identifier=word,
                )
            else:
                yield D(
                    "SST-VAL221",
                    origin=item.view.origin,
                    subject=item.key,
                    artifact=item.key,
                    name=word,
                    field=f"the expr of '{member}'",
                )
    for folded, name in declared.items():
        if folded not in used:
            yield D("SST-VAL220", origin=item.view.origin, subject=item.key, artifact=item.key, name=name)


def _member_rules(item: _Attached, inputs: ViewInputs) -> Iterator[Diagnostic]:
    """Report a view with nothing to query, then a `max_staleness` under Snowflake's floor.

    A view naming a table that is not a dbt model is not checked for members: SST-REF001
    reports the table, and its members cannot be known.
    """
    dimensions = any(
        column.column_type in DIMENSION_TYPES
        and not column.excluded
        and item.scope.admits_column_name(f"{model.name}.{column.name}")
        for table in item.tables
        if (model := inputs.models.get(table)) is not None
        for column in model.columns
    )
    known = bool(item.tables) and all(table in inputs.models for table in item.tables)
    if known and not dimensions and not item.metrics and not any(f.entity_level for f in item.filters):
        yield D("SST-VAL301", origin=item.view.origin, subject=item.key, artifact=item.key)
    staleness = item.view.source.get("max_staleness")
    if isinstance(staleness, int) and not isinstance(staleness, bool) and staleness < 120:
        yield D("SST-VAL304", origin=item.view.origin, subject=item.key, artifact=item.key, found=staleness)


def _shared_table_descriptions(items: tuple[_Attached, ...]) -> Iterator[Diagnostic]:
    """Report each pair of views that describe one table differently, in view order.

    A view describes a table with `table_config.<model>.description`; a view that does not is
    not compared.
    """
    first: dict[str, tuple[str, str]] = {}
    for item in items:
        for model, value in _mapping(item.view.source.get("table_config")).items():
            description = " ".join(str(_mapping(value).get("description") or "").split())
            if not description:
                continue
            seen = first.setdefault(str(model).casefold(), (item.key, description))
            if seen[1] != description:
                yield D("SST-VAL322", origin=item.view.origin, subject=item.key, a=seen[0], b=item.key, name=model)
