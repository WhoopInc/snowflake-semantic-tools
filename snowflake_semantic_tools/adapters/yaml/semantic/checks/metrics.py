"""Check metrics against each other and the dbt models: references, windows, cycles and duplicates."""

from __future__ import annotations

import re
from collections.abc import Mapping

from .....domain.model.artifact_key import artifact_key
from .....domain.model.dbt import DbtModel
from .....domain.model.diagnostic import D, Diagnostic
from .....domain.model.expression import call_arguments, is_aggregate_expression
from .....domain.model.expression import root_function as _root_function
from .....domain.model.reference import TemplateSyntaxError, scan_template_calls, single_template_call
from .....domain.model.semantic_view import ColumnKind, SortKey
from ..defs import MetricDef, WindowDef
from .expressions import _bare_column_identifiers, _var_diagnostic

# The column types a NON ADDITIVE BY entry or a window may sort or partition by.
DIMENSION_TYPES = frozenset((ColumnKind.DIMENSION.value, ColumnKind.TIME_DIMENSION.value))


def _metric_cycles(metrics: tuple[MetricDef, ...]) -> tuple[tuple[str, ...], ...]:
    graph = {metric.name.casefold(): metric.referenced_metrics for metric in metrics}
    cycles: list[tuple[str, ...]] = []
    visited: set[str] = set()
    active: list[str] = []

    def visit(name: str) -> None:
        if name in active:
            start = active.index(name)
            cycle = tuple(active[start:] + [name])
            variants = [tuple(cycle[index:-1] + cycle[:index] + (cycle[index],)) for index in range(len(cycle) - 1)]
            canonical = min(variants)
            if canonical not in cycles:
                cycles.append(canonical)
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
    return tuple(cycles)


def _metric_diagnostics(
    metrics: tuple[MetricDef, ...],
    models: dict[str, DbtModel],
    variables: Mapping[str, object] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    counts: dict[str, int] = {}
    for metric in metrics:
        key = metric.name.casefold()
        counts[key] = counts.get(key, 0) + 1
    duplicate_names = {name for name, count in counts.items() if count > 1}
    known_metrics = {metric.name.casefold() for metric in metrics}
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    diagnostics.extend(
        D("SST-VAL001", type="metric", name=name, subject=artifact_key("metric", name))
        for name in sorted(duplicate_names)
    )
    for metric in metrics:
        if metric.name.casefold() in duplicate_names:
            continue
        if not metric.derived and not metric.has_tables_key:
            diagnostics.append(D("SST-VAL109", metric=metric.name, subject=artifact_key("metric", metric.name)))
        elif not metric.derived and not metric.tables:
            diagnostics.append(
                D(
                    "SST-PRS102",
                    artifact=artifact_key("metric", metric.name),
                    subject=artifact_key("metric", metric.name),
                )
            )
        if metric.derived and metric.has_tables_key:
            diagnostics.append(D("SST-VAL108", metric=metric.name, subject=artifact_key("metric", metric.name)))
        if metric.derived and metric.using_relationships:
            diagnostics.append(D("SST-VAL113", metric=metric.name, subject=artifact_key("metric", metric.name)))
        if len(metric.using_relationships) > 1:
            diagnostics.append(
                D(
                    "SST-VAL115",
                    metric=metric.name,
                    count=len(metric.using_relationships),
                    subject=artifact_key("metric", metric.name),
                )
            )
        if metric.access_modifier not in ("public_access", "private_access"):
            diagnostics.append(
                D(
                    "SST-VAL121",
                    metric=metric.name,
                    found=metric.access_modifier,
                    subject=artifact_key("metric", metric.name),
                )
            )
        referenced_tables = metric.tables or metric.referenced_models
        owner = referenced_tables[0] if len(referenced_tables) == 1 else None
        for entry in metric.non_additive:
            # Without `table:` the dimension belongs to the metric's own table.
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
        if not metric.derived and metric.window is not None:
            diagnostics.extend(_window_diagnostics(metric, metric_by_name, models))
        elif not metric.derived and metric.tables and not is_aggregate_expression(metric.expr):
            diagnostics.append(
                D(
                    "SST-VAL101",
                    metric=metric.name,
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
        if metric.derived:
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
                        subject=artifact_key("metric", metric.name),
                    )
                )
            for referenced_metric in metric.referenced_metrics:
                pattern = (
                    rf"\b(?:SUM|AVG|MIN|MAX|COUNT|MEDIAN)\s*\([^)]*"
                    rf"metric\s*\(\s*['\"]{re.escape(referenced_metric)}['\"]\s*\)[^)]*\)"
                )
                if re.search(pattern, metric.expr, re.IGNORECASE):
                    diagnostics.append(
                        D(
                            "SST-VAL103",
                            metric=metric.name,
                            other=referenced_metric,
                            subject=artifact_key("metric", metric.name),
                        )
                    )
            for call in metric.calls:
                if call.function == "ref" and len(call.args) == 2:
                    diagnostics.append(
                        D(
                            "SST-VAL104",
                            metric=metric.name,
                            column=f"{call.args[0]}.{call.args[1]}",
                            subject=artifact_key("metric", metric.name),
                        )
                    )
            for member in re.findall(
                r"\b(?:fact|dimension)\s*\(\s*['\"]([^'\"]+)['\"]\s*\)",
                metric.expr,
                re.IGNORECASE,
            ):
                diagnostics.append(
                    D(
                        "SST-VAL105",
                        metric=metric.name,
                        member_type="member",
                        other=member,
                        subject=artifact_key("metric", metric.name),
                    )
                )
        else:
            for referenced_metric in metric.referenced_metrics:
                referenced = metric_by_name.get(referenced_metric)
                if referenced is None:
                    continue
                if referenced.derived:
                    diagnostics.append(
                        D(
                            "SST-VAL106",
                            metric=metric.name,
                            other=referenced.name,
                            subject=artifact_key("metric", metric.name),
                        )
                    )
                if referenced.non_additive:
                    diagnostics.append(
                        D(
                            "SST-VAL107",
                            metric=metric.name,
                            other=referenced.name,
                            subject=artifact_key("metric", metric.name),
                        )
                    )
        for referenced_metric in metric.referenced_metrics:
            if referenced_metric not in known_metrics:
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
        try:
            calls = scan_template_calls(metric.expr)
        except TemplateSyntaxError as exc:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=metric.origin,
                    file=metric.origin.file if metric.origin else "<expression>",
                    line=exc.line,
                    col=exc.col,
                    reason=exc.reason,
                    subject=artifact_key("metric", metric.name),
                )
            )
            continue
        for call in calls:
            if call.function == "var" and (
                len(call.args) != 1 or variables is not None and call.args[0] not in variables
            ):
                diagnostics.append(_var_diagnostic(call, metric.origin, artifact_key("metric", metric.name)))
                continue
            if call.function != "ref" or len(call.args) not in (1, 2):
                continue
            model_name = call.args[0]
            model = models.get(model_name.casefold())
            if model is None:
                diagnostics.append(
                    D(
                        "SST-REF001",
                        model=model_name,
                        subject=artifact_key("metric", metric.name),
                        origin=metric.origin,
                    )
                )
            elif len(call.args) == 2:
                column_name = call.args[1]
                column = model.column(column_name)
                if column is None:
                    diagnostics.append(
                        D(
                            "SST-REF002",
                            model=model_name,
                            column=column_name,
                            subject=artifact_key("metric", metric.name),
                            origin=metric.origin,
                        )
                    )
                elif column.excluded:
                    diagnostics.append(
                        D(
                            "SST-VAL318",
                            artifact=artifact_key("metric", metric.name),
                            member=metric.name,
                            column=column_name,
                            subject=artifact_key("metric", metric.name),
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
                        subject=artifact_key("metric", metric.name),
                        metric=metric.name,
                        artifact=artifact_key("metric", metric.name),
                        outside=model_name,
                    )
                )
        for identifier in _bare_column_identifiers(
            metric.expr,
            metric.tables,
            models,
            variables or {},
        ):
            diagnostics.append(
                D(
                    "SST-VAL110",
                    metric=metric.name,
                    column=identifier,
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
            break
    expressions: dict[tuple[str, tuple[str, ...], bool, tuple[SortKey, ...], WindowDef | None], str] = {}
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
    return tuple(diagnostics)


def _metric_owner(metric: MetricDef) -> str | None:
    """The one table a metric belongs to, casefolded; None for a cross-table metric."""
    tables = metric.tables or metric.referenced_models
    return tables[0] if len(tables) == 1 else None


def _window_diagnostics(
    metric: MetricDef,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Snowflake's rules for a window function metric that its own compile does not report plainly."""
    window = metric.window
    assert window is not None
    subject = artifact_key("metric", metric.name)
    diagnostics: list[Diagnostic] = []
    arguments = call_arguments(metric.expr)
    # The window must apply to a metric or an aggregate: over a raw column it is a
    # row-level window, which Snowflake allows only in a fact or dimension.
    if not arguments or not is_aggregate_expression(arguments[0]):
        diagnostics.append(
            D(
                "SST-VAL126",
                metric=metric.name,
                function=_root_function(metric.expr) or "the expression",
                subject=subject,
                origin=metric.origin,
            )
        )
    owner = _metric_owner(metric)
    if owner is None:
        # Without exactly one table the metric renders at view level, as a derived
        # metric does, and cannot carry a window either.
        diagnostics.append(
            D(
                "SST-VAL102",
                function=_root_function(metric.expr) or "window",
                metric=metric.name,
                subject=subject,
                origin=metric.origin,
            )
        )
    for field, text in window.references():
        dimensions_only = field.startswith("partition_by_excluding")
        try:
            column_call = single_template_call(text, "ref")
            metric_call = single_template_call(text, "metric")
        except TemplateSyntaxError:
            column_call = metric_call = None
        resolves = False
        if column_call is not None and len(column_call.args) == 2:
            model = models.get(column_call.args[0].casefold())
            column = model.column(column_call.args[1]) if model is not None else None
            resolves = column is not None and not column.excluded and column.column_type in DIMENSION_TYPES
        elif metric_call is not None and len(metric_call.args) == 1 and not dimensions_only:
            other = metric_by_name.get(metric_call.args[0].casefold())
            resolves = (
                other is not None
                and not other.derived
                and other.window is None
                and owner is not None
                and _metric_owner(other) == owner
            )
        if not resolves:
            diagnostics.append(
                D(
                    "SST-VAL125",
                    metric=metric.name,
                    field=field,
                    value=text,
                    expected="a dimension" if dimensions_only else "a dimension or a metric of the same table",
                    subject=subject,
                    origin=metric.origin,
                )
            )
    if window.frame is not None and not window.order_by:
        diagnostics.append(
            D("SST-VAL127", metric=metric.name, value=window.frame, subject=subject, origin=metric.origin)
        )
    return diagnostics
