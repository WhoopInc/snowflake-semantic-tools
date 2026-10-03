"""Check metrics against each other and the dbt models: structure, references, cycles and duplicates.

A window function metric's `window:` block is checked in `windows`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import MetricDef, WindowDef
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.calls import call_problem, variable_problem
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from snowflake_semantic_tools.domain.validate.column_metadata import is_temporal
from snowflake_semantic_tools.domain.validate.expression import is_aggregate_expression
from snowflake_semantic_tools.domain.validate.expression import root_function as _root_function
from snowflake_semantic_tools.domain.validate.semantic.expressions import bare_column_identifiers, scan_expression
from snowflake_semantic_tools.domain.validate.semantic.windows import (
    DIMENSION_TYPES,
    metric_owner,
    window_diagnostics,
)


def metric_cycles(metrics: tuple[MetricDef, ...]) -> tuple[tuple[str, ...], ...]:
    """Find cycles of `metric()` references, each as casefolded names whose last repeats the first.

    A depth-first walk starts from each metric in name order; a reference to an unknown metric
    ends there. Each cycle is rotated to start at its smallest name and listed once, in the order
    the walk finds them. The walk never re-enters a metric it has finished, so it can miss a
    cycle through one; each metric on a cycle that no listed cycle names then adds the shortest
    cycle through it, in name order, so every metric on a cycle is in at least one.
    """
    graph = {metric.name.casefold(): metric.referenced_metrics for metric in metrics}
    # Insertion-ordered, so each cycle is listed once in the order the walk finds it.
    cycles: dict[tuple[str, ...], None] = {}
    visited: set[str] = set()
    active: list[str] = []

    def visit(name: str) -> None:
        if name in active:
            start = active.index(name)
            _add_cycle(cycles, tuple(active[start:] + [name]))
            return
        if name in visited or name not in graph:
            return
        active.append(name)
        for dependency in graph[name]:
            visit(dependency)
        active.pop()
        visited.add(name)

    for metric_name in sorted(graph):
        visit(metric_name)
    for metric_name in sorted(graph):
        if any(metric_name in cycle for cycle in cycles):
            continue
        shortest = _shortest_cycle(graph, metric_name)
        if shortest is not None:
            _add_cycle(cycles, shortest)
    return tuple(cycles)


def _add_cycle(cycles: dict[tuple[str, ...], None], cycle: tuple[str, ...]) -> None:
    """Record `cycle` rotated to start at its smallest name, unless that rotation is listed."""
    variants = [tuple(cycle[index:-1] + cycle[:index] + (cycle[index],)) for index in range(len(cycle) - 1)]
    cycles.setdefault(min(variants))


def _shortest_cycle(graph: Mapping[str, tuple[str, ...]], start: str) -> tuple[str, ...] | None:
    """Return the shortest path of references from `start` back to it, or None when there is none.

    Breadth-first, following references in the order each metric names them, so the path is
    the same on every run.
    """
    parents: dict[str, str] = {}
    frontier = [start]
    while frontier:
        following: list[str] = []
        for name in frontier:
            for dependency in graph.get(name, ()):
                if dependency == start:
                    path = [name]
                    while path[-1] != start:
                        path.append(parents[path[-1]])
                    return (start, *reversed(path[:-1]), start)
                if dependency in graph and dependency not in parents:
                    parents[dependency] = name
                    following.append(dependency)
        frontier = following
    return None


def metric_diagnostics(
    metrics: tuple[MetricDef, ...],
    models: dict[str, DbtModel],
    variables: Mapping[str, object] | None = None,
) -> tuple[Diagnostic, ...]:
    """Check every metric against the others and the dbt models, rule by rule in a fixed order.

    Repeated names come first, each reported once; a metric whose name repeats is checked no
    further, but still takes part in the equivalence rule, which runs last over every metric.
    Every other metric runs the per-metric rules of `_one_metric_diagnostics` in turn, so its
    diagnostics come out rule by rule.

    Args:
        variables: The project's `vars:`; None checks only that each `var()` takes one name.
    """
    duplicate_names = _duplicate_names(metrics)
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    diagnostics = _duplicate_diagnostics(duplicate_names, metrics)
    for metric in metrics:
        if metric.name.casefold() not in duplicate_names:
            diagnostics.extend(_one_metric_diagnostics(metric, metric_by_name, models, variables))
    diagnostics.extend(_equivalence_diagnostics(metrics))
    return tuple(diagnostics)


def _duplicate_names(metrics: tuple[MetricDef, ...]) -> frozenset[str]:
    """Return the casefolded names that two or more metrics share."""
    counts: dict[str, int] = {}
    for metric in metrics:
        key = metric.name.casefold()
        counts[key] = counts.get(key, 0) + 1
    return frozenset(name for name, count in counts.items() if count > 1)


def _duplicate_diagnostics(duplicate_names: frozenset[str], metrics: tuple[MetricDef, ...]) -> list[Diagnostic]:
    """Report each repeated metric name once, in sorted order.

    Diagnostics:
        SST-PRS008: when a derived metric and a table-scoped metric share the name.
        SST-VAL001: when two or more metrics of one scope share a name, compared casefolded.
    """
    scopes: dict[str, set[bool]] = {}
    for metric in metrics:
        scopes.setdefault(metric.name.casefold(), set()).add(metric.derived)
    return [
        D("SST-PRS008", name=name, subject=artifact_key("metric", name))
        if len(scopes.get(name, ())) > 1
        else D("SST-VAL001", type="metric", name=name, subject=artifact_key("metric", name))
        for name in sorted(duplicate_names)
    ]


def _one_metric_diagnostics(
    metric: MetricDef,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
    variables: Mapping[str, object] | None,
) -> list[Diagnostic]:
    """Run the per-metric rules on one metric, in order.

    Structure, non-additive dimensions, their ordering, window or aggregate, additivity and
    division, the derived or the base-metric references, referenced metrics, template calls,
    then bare identifiers. A malformed template
    is reported (SST-LOD004) in place of the template-call rule and ends the metric's checks:
    the bare-identifier rule would only guess at an expression it cannot scan.
    """
    diagnostics = [
        *_structure_diagnostics(metric),
        *_non_additive_diagnostics(metric, models),
        *_ordering_diagnostics(metric),
        *_window_or_aggregate_diagnostics(metric, metric_by_name, models),
        *_additivity_diagnostics(metric, models),
    ]
    if metric.derived:
        diagnostics.extend(_derived_diagnostics(metric))
    else:
        diagnostics.extend(_base_reference_diagnostics(metric, metric_by_name))
    diagnostics.extend(_referenced_metric_diagnostics(metric, metric_by_name))
    calls = scan_expression(metric.expr, metric.origin, artifact_key("metric", metric.name))
    if isinstance(calls, Diagnostic):
        diagnostics.append(calls)
        return diagnostics
    diagnostics.extend(_template_call_diagnostics(metric, calls, models, variables))
    diagnostics.extend(_bare_identifier_diagnostics(metric, models, variables))
    return diagnostics


def _structure_diagnostics(metric: MetricDef) -> list[Diagnostic]:
    """Report the `tables:`, `using_relationships` and `access_modifier` a metric's kind rules out.

    Diagnostics:
        SST-VAL109: when a table-scoped metric has no `tables:` key.
        SST-PRS102: when a table-scoped metric's `tables:` is empty.
        SST-VAL108: when a derived metric declares `tables:`.
        SST-VAL113: when a derived metric declares `using_relationships`.
        SST-VAL115: when a metric declares a chain of more than one relationship.
        SST-VAL121: when `access_modifier` is neither `public_access` nor `private_access`.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    if not metric.derived and not metric.has_tables_key:
        diagnostics.append(D("SST-VAL109", metric=metric.name, subject=subject))
    elif not metric.derived and not metric.tables:
        diagnostics.append(D("SST-PRS102", artifact=subject, subject=subject))
    if metric.derived and metric.has_tables_key:
        diagnostics.append(D("SST-VAL108", metric=metric.name, subject=subject))
    if metric.derived and metric.using_relationships:
        diagnostics.append(D("SST-VAL113", metric=metric.name, subject=subject))
    if len(metric.using_relationships) > 1:
        diagnostics.append(D("SST-VAL115", metric=metric.name, count=len(metric.using_relationships), subject=subject))
    if metric.access_modifier not in ("public_access", "private_access"):
        diagnostics.append(D("SST-VAL121", metric=metric.name, found=metric.access_modifier, subject=subject))
    return diagnostics


def _non_additive_diagnostics(metric: MetricDef, models: Mapping[str, DbtModel]) -> list[Diagnostic]:
    """Report each non-additive dimension that is not a dimension of its table.

    An entry without `table:` names a dimension of the metric's own table, so on a metric of
    more than one table it never resolves.

    Diagnostics:
        SST-VAL118: when an entry names no column, an excluded column, or one that is not a dimension.
    """
    owner = metric_owner(metric)
    diagnostics: list[Diagnostic] = []
    for entry in metric.non_additive:
        model = models.get((entry.table or owner or "").casefold())
        column = model.column(entry.dimension) if model is not None else None
        if column is None or column.excluded or column.column_type not in DIMENSION_TYPES:
            diagnostics.append(
                D(
                    "SST-VAL118",
                    metric=metric.name,
                    value=f"{entry.table}.{entry.dimension}" if entry.table else entry.dimension,
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
    return diagnostics


def _ordering_diagnostics(metric: MetricDef) -> list[Diagnostic]:
    """Report the order of a metric's non-additive dimensions, then a sort with no null order.

    Diagnostics:
        SST-VAL119: the metric declares two or more non-additive dimensions, whose order counts.
        SST-VAL120: a non-additive or window order entry declares `sort_direction` and no
            `null_order`; reported once per metric.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    if len(metric.non_additive) > 1:
        order = ", ".join(_sort_label(*entry.key) for entry in metric.non_additive)
        diagnostics.append(D("SST-VAL119", metric=metric.name, value=order, subject=subject, origin=metric.origin))
    entries = [(entry.descending, entry.nulls_first) for entry in metric.non_additive]
    if metric.window is not None:
        entries.extend((entry.descending, entry.nulls_first) for entry in metric.window.order_by)
    if any(descending is not None and nulls_first is None for descending, nulls_first in entries):
        diagnostics.append(D("SST-VAL120", metric=metric.name, subject=subject, origin=metric.origin))
    return diagnostics


def _sort_label(expression: str, descending: bool | None, nulls_first: bool | None) -> str:
    direction = "" if descending is None else " DESC" if descending else " ASC"
    nulls = "" if nulls_first is None else " NULLS FIRST" if nulls_first else " NULLS LAST"
    return f"{expression}{direction}{nulls}"


def _additivity_diagnostics(metric: MetricDef, models: Mapping[str, DbtModel]) -> list[Diagnostic]:
    """Report a sum over a snapshot grain that declares no non-additive dimension, then a bare division.

    A table is a snapshot grain when one of its keys holds a date or timestamp column: it holds
    one row per entity per period, so summing a measure over periods counts it once per period.

    Diagnostics:
        SST-VAL117: a table-scoped SUM over a snapshot grain declares no non_additive_dimensions.
        SST-VAL111: the expression divides by a denominator neither NULLIF nor a non-zero number.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    owner = metric_owner(metric)
    model = models.get(owner) if owner is not None and not metric.derived else None
    if (
        model is not None
        and not metric.non_additive
        and metric.window is None
        and (_root_function(metric.expr) or "").upper() == "SUM"
        and _is_snapshot_grain(model)
    ):
        diagnostics.append(D("SST-VAL117", metric=metric.name, subject=subject, origin=metric.origin))
    if unguarded_division(metric.expr):
        diagnostics.append(D("SST-VAL111", metric=metric.name, subject=subject, origin=metric.origin))
    return diagnostics


def _is_snapshot_grain(model: DbtModel) -> bool:
    for key in (model.primary_key, *model.unique_keys):
        for name in key:
            column = model.column(name)
            if column is not None and (column.column_type == "time_dimension" or is_temporal(column.data_type)):
                return True
    return False


_MASKED = re.compile(r"\{\{.*?\}\}|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*.*?\*/", re.DOTALL)
_GUARDED = re.compile(r"\s*(?:NULLIF\s*\(|\(*\s*(?:0*[1-9][0-9]*(?:\.[0-9]*)?|0*\.[0-9]*[1-9][0-9]*)\b)", re.IGNORECASE)


def unguarded_division(expression: str) -> bool:
    """Report whether an expression divides by anything other than NULLIF(...) or a non-zero number.

    Template calls, strings and comments are masked first, so a `/` inside them is not division.
    """
    masked = _MASKED.sub(lambda match: "x" if match.group(0).startswith("{{") else " ", expression)
    return any(not _GUARDED.match(masked, match.end()) for match in re.finditer(r"/", masked))


def _window_or_aggregate_diagnostics(
    metric: MetricDef,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Check a table-scoped metric's window, or, when it has none, that its expression aggregates.

    A window is checked by `windows.window_diagnostics`; a derived metric's window is the
    derived rule's to report.

    Diagnostics:
        SST-VAL101: when a table-scoped metric without a window does not aggregate.
    """
    if not metric.derived and metric.window is not None:
        return window_diagnostics(metric, metric_by_name, models)
    if not metric.derived and metric.tables and not is_aggregate_expression(metric.expr):
        return [
            D(
                "SST-VAL101",
                metric=metric.name,
                subject=artifact_key("metric", metric.name),
                origin=metric.origin,
            )
        ]
    return []


def _derived_diagnostics(metric: MetricDef) -> list[Diagnostic]:
    """Report what a derived metric may not do: window, aggregate a metric, or read a member.

    Diagnostics:
        SST-VAL102: when the expression calls a window function or the metric has a `window:`.
        SST-VAL103: when the expression aggregates a referenced metric, once per metric.
        SST-VAL104: when the expression refs a column.
        SST-VAL105: when the expression calls `fact()` or `dimension()`.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    window = re.search(
        r"\b([A-Z_][A-Z0-9_]*)\s*\([\s\S]*?\)\s+OVER\s*\(",
        metric.expr,
        re.IGNORECASE,
    )
    if window or metric.window is not None:
        diagnostics.append(
            D(
                "SST-VAL102",
                function=window.group(1).upper() if window else _root_function(metric.expr) or "window",
                metric=metric.name,
                subject=subject,
            )
        )
    for referenced_metric in metric.referenced_metrics:
        pattern = (
            rf"\b(?:SUM|AVG|MIN|MAX|COUNT|MEDIAN)\s*\([^)]*"
            rf"metric\s*\(\s*['\"]{re.escape(referenced_metric)}['\"]\s*\)[^)]*\)"
        )
        if re.search(pattern, metric.expr, re.IGNORECASE):
            diagnostics.append(D("SST-VAL103", metric=metric.name, other=referenced_metric, subject=subject))
    for call in metric.calls:
        if call.function == "ref" and len(call.args) == 2:
            diagnostics.append(
                D("SST-VAL104", metric=metric.name, column=f"{call.args[0]}.{call.args[1]}", subject=subject)
            )
    for member in re.findall(
        r"\b(?:fact|dimension)\s*\(\s*['\"]([^'\"]+)['\"]\s*\)",
        metric.expr,
        re.IGNORECASE,
    ):
        diagnostics.append(D("SST-VAL105", metric=metric.name, member_type="member", other=member, subject=subject))
    return diagnostics


def _base_reference_diagnostics(metric: MetricDef, metric_by_name: Mapping[str, MetricDef]) -> list[Diagnostic]:
    """Report each metric a table-scoped metric references that it may not build on.

    Diagnostics:
        SST-VAL106: when the referenced metric is derived.
        SST-VAL107: when the referenced metric declares non-additive dimensions.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    for referenced_metric in metric.referenced_metrics:
        referenced = metric_by_name.get(referenced_metric)
        if referenced is None:
            continue
        if referenced.derived:
            diagnostics.append(D("SST-VAL106", metric=metric.name, other=referenced.name, subject=subject))
        if referenced.non_additive:
            diagnostics.append(D("SST-VAL107", metric=metric.name, other=referenced.name, subject=subject))
    return diagnostics


def _referenced_metric_diagnostics(metric: MetricDef, metric_by_name: Mapping[str, MetricDef]) -> list[Diagnostic]:
    """Report each metric a metric references that does not exist or is a window function metric.

    Diagnostics:
        SST-REF006: when the referenced metric does not exist.
        SST-VAL128: when the referenced metric has a window.
    """
    diagnostics: list[Diagnostic] = []
    for referenced_metric in metric.referenced_metrics:
        if referenced_metric not in metric_by_name:
            diagnostics.append(
                D(
                    "SST-REF006",
                    origin=metric.origin,
                    subject=artifact_key("metric", metric.name),
                    name=referenced_metric,
                )
            )
        elif metric_by_name[referenced_metric].window is not None:
            diagnostics.append(
                D(
                    "SST-VAL128",
                    metric=metric.name,
                    other=metric_by_name[referenced_metric].name,
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
    return diagnostics


def _template_call_diagnostics(
    metric: MetricDef,
    calls: tuple[TemplateCall, ...],
    models: Mapping[str, DbtModel],
    variables: Mapping[str, object] | None,
) -> list[Diagnostic]:
    """Check each template call of a metric's expression, in source order.

    A call must first be legal in a metric expression; a legacy call is left to the legacy
    check, which names it at its position, and a `metric()` call to the reference rules.

    Diagnostics:
        SST-REF004, SST-REF041, SST-REF015: when the call is not legal in a metric expression.
        SST-CFG029, SST-REF009: when a `var()` call names no project variable, or an empty one.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    for call in calls:
        if call.function in ("table", "column"):
            continue
        problem = call_problem(
            call, METRIC_EXPR.allowed, metric.origin, field="metric.expression", artifact=subject, subject=subject
        )
        if problem is not None:
            diagnostics.append(problem)
        elif call.function == "var" and variables is not None:
            found = variable_problem(call.args[0], variables, metric.origin, subject=subject)
            diagnostics.extend((found,) if found is not None else ())
        elif call.function == "ref":
            diagnostics.extend(_ref_call_diagnostics(metric, call, models))
    return diagnostics


def _ref_call_diagnostics(metric: MetricDef, call: TemplateCall, models: Mapping[str, DbtModel]) -> list[Diagnostic]:
    """Check one `ref()` call of a metric against the dbt models and the metric's `tables:`.

    The call is checked for what it names first, then for whether its model is one of the
    metric's tables; a call can report one of each.

    Diagnostics:
        SST-REF001: when the call names no dbt model.
        SST-REF002: when the call names a column its model does not have.
        SST-VAL318: when the call names a column its model excludes.
        SST-VAL112: when the call's model is outside the metric's `tables:`, all of them known models.
    """
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    model_name = call.args[0]
    model = models.get(model_name.casefold())
    if model is None:
        diagnostics.append(D("SST-REF001", model=model_name, subject=subject, origin=metric.origin))
    elif len(call.args) == 2:
        column_name = call.args[1]
        column = model.column(column_name)
        if column is None:
            diagnostics.append(
                D("SST-REF002", model=model_name, column=column_name, subject=subject, origin=metric.origin)
            )
        elif column.excluded:
            diagnostics.append(
                D(
                    "SST-VAL318",
                    artifact=subject,
                    member=metric.name,
                    column=column_name,
                    subject=subject,
                    origin=metric.origin,
                )
            )
    if (
        model is not None
        and metric.tables
        and model_name.casefold() not in metric.tables
        and all(table in models for table in metric.tables)
    ):
        diagnostics.append(
            D(
                "SST-VAL112",
                origin=metric.origin,
                subject=subject,
                metric=metric.name,
                artifact=subject,
                outside=model_name,
            )
        )
    return diagnostics


def _bare_identifier_diagnostics(
    metric: MetricDef,
    models: Mapping[str, DbtModel],
    variables: Mapping[str, object] | None,
) -> list[Diagnostic]:
    """Report the first column of the metric's tables that its expression names without a `ref()`.

    Diagnostics:
        SST-VAL110: when the expression names a column bare; only the first is reported.
    """
    identifiers = bare_column_identifiers(metric.expr, metric.tables, models, variables or {})
    if not identifiers:
        return []
    return [
        D(
            "SST-VAL110",
            metric=metric.name,
            column=identifiers[0],
            subject=artifact_key("metric", metric.name),
            origin=metric.origin,
        )
    ]


def _equivalence_diagnostics(metrics: tuple[MetricDef, ...]) -> list[Diagnostic]:
    """Report each table-scoped metric that computes exactly what an earlier metric computes.

    Two metrics compute the same when their expressions match ignoring whitespace and case, over
    the same tables, of the same kind, with the same non-additive ordering and window. A metric
    with no tables, or with the earlier metric's own name, is not reported and takes its place.

    Diagnostics:
        SST-VAL124: when a table-scoped metric computes what an earlier metric does.
    """
    diagnostics: list[Diagnostic] = []
    expressions: dict[
        tuple[str, tuple[str, ...], bool, tuple[tuple[str, bool | None, bool | None], ...], WindowDef | None], str
    ] = {}
    for metric in metrics:
        # The non-additive ordering and the window are part of what a metric
        # computes: the same SUM at the latest and at the earliest snapshot, or over
        # two windows, is two metrics, not one.
        canonical = (
            " ".join(metric.expr.split()).casefold(),
            tuple(sorted(metric.tables)),
            metric.derived,
            tuple(entry.key for entry in metric.non_additive),
            metric.window,
        )
        previous = expressions.get(canonical)
        if previous is not None and previous.casefold() != metric.name.casefold() and metric.tables:
            diagnostics.append(
                D(
                    "SST-VAL124",
                    metric=metric.name,
                    other=previous,
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
        else:
            expressions[canonical] = metric.name
    return diagnostics
