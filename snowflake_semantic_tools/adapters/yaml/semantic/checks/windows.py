"""Check a window function metric's `window:` block against rules Snowflake's compile hides."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef, WindowDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, single_template_call
from snowflake_semantic_tools.domain.validate.expression import call_arguments, is_aggregate_expression
from snowflake_semantic_tools.domain.validate.expression import root_function as _root_function

# The column types a NON ADDITIVE BY entry or a window may sort or partition by.
DIMENSION_TYPES = frozenset((ColumnKind.DIMENSION.value, ColumnKind.TIME_DIMENSION.value))


def _metric_owner(metric: MetricDef) -> str | None:
    """Return the one table a metric belongs to, casefolded; None for a cross-table metric."""
    tables = metric.tables or metric.referenced_models
    return tables[0] if len(tables) == 1 else None


def _window_diagnostics(
    metric: MetricDef,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Check a window function metric against the rules Snowflake's own compile does not report plainly.

    The rules run in order: what the window applies to, the metric's table, each window
    entry in `WindowDef.references()` order, then the frame.
    """
    window = metric.window
    assert window is not None
    diagnostics = _window_argument_diagnostics(metric)
    owner = _metric_owner(metric)
    diagnostics.extend(_window_owner_diagnostics(metric, owner))
    diagnostics.extend(_window_entry_diagnostics(metric, window, owner, metric_by_name, models))
    diagnostics.extend(_window_frame_diagnostics(metric, window))
    return diagnostics


def _window_argument_diagnostics(metric: MetricDef) -> list[Diagnostic]:
    """Report a window that does not apply to a metric or an aggregate.

    Diagnostics:
        SST-VAL126: when the window function's first argument is not an aggregate.
    """
    arguments = call_arguments(metric.expr)
    # The window must apply to a metric or an aggregate: over a raw column it is a
    # row-level window, which Snowflake allows only in a fact or dimension.
    if arguments and is_aggregate_expression(arguments[0]):
        return []
    return [
        D(
            "SST-VAL126",
            metric=metric.name,
            function=_root_function(metric.expr) or "the expression",
            subject=artifact_key("metric", metric.name),
            origin=metric.origin,
        )
    ]


def _window_owner_diagnostics(metric: MetricDef, owner: str | None) -> list[Diagnostic]:
    """Report a window metric that does not belong to exactly one table.

    Diagnostics:
        SST-VAL102: when the metric has no single table to render under.
    """
    if owner is not None:
        return []
    # Without exactly one table the metric renders at view level, as a derived
    # metric does, and cannot carry a window either.
    return [
        D(
            "SST-VAL102",
            function=_root_function(metric.expr) or "window",
            metric=metric.name,
            subject=artifact_key("metric", metric.name),
            origin=metric.origin,
        )
    ]


def _window_entry_diagnostics(
    metric: MetricDef,
    window: WindowDef,
    owner: str | None,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Report each window entry that names neither a dimension nor, where allowed, a metric.

    Diagnostics:
        SST-VAL125: when a `partition_by_excluding` entry is not a dimension, or another entry is
            neither a dimension nor a metric of the same table.
    """
    diagnostics: list[Diagnostic] = []
    for field, text in window.references():
        dimensions_only = field.startswith("partition_by_excluding")
        if not _window_entry_resolves(text, dimensions_only, owner, metric_by_name, models):
            diagnostics.append(
                D(
                    "SST-VAL125",
                    metric=metric.name,
                    field=field,
                    value=text,
                    expected="a dimension" if dimensions_only else "a dimension or a metric of the same table",
                    subject=artifact_key("metric", metric.name),
                    origin=metric.origin,
                )
            )
    return diagnostics


def _window_entry_resolves(
    text: str,
    dimensions_only: bool,
    owner: str | None,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> bool:
    """Tell whether one window entry is a dimension column or, unless dimensions only, a sibling metric.

    A column must exist, not be excluded, and be a dimension. A metric must be a table-scoped,
    non-window metric of the same table as the window metric. A malformed template resolves to
    neither.
    """
    try:
        column_call = single_template_call(text, "ref")
        metric_call = single_template_call(text, "metric")
    except TemplateSyntaxError:
        column_call = metric_call = None
    if column_call is not None and len(column_call.args) == 2:
        model = models.get(column_call.args[0].casefold())
        column = model.column(column_call.args[1]) if model is not None else None
        return column is not None and not column.excluded and column.column_type in DIMENSION_TYPES
    if metric_call is not None and len(metric_call.args) == 1 and not dimensions_only:
        other = metric_by_name.get(metric_call.args[0].casefold())
        return (
            other is not None
            and not other.derived
            and other.window is None
            and owner is not None
            and _metric_owner(other) == owner
        )
    return False


def _window_frame_diagnostics(metric: MetricDef, window: WindowDef) -> list[Diagnostic]:
    """Report a window frame without an order to frame.

    Diagnostics:
        SST-VAL127: when the window has a frame and no `order_by`.
    """
    if window.frame is not None and not window.order_by:
        return [
            D(
                "SST-VAL127",
                metric=metric.name,
                value=window.frame,
                subject=artifact_key("metric", metric.name),
                origin=metric.origin,
            )
        ]
    return []
