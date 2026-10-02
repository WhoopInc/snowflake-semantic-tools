"""Check an eval's run config: agent version, metrics, dataset templates, and the run block.

The rules run in a fixed order, which fixes the order of their diagnostics. The run name
is claimed in a map shared by every eval of the catalog, so the evals must be checked in
catalog order: a name that collides is reported on the eval that claims it second.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.eval.model import (
    EVAL_COMPLETED,
    EVAL_CONCURRENCY_MIN,
    EVAL_PASS_STATUSES,
    EVAL_RETRY_MIN,
    SYSTEM_EVAL_METRIC_VERSION,
    SYSTEM_EVAL_METRICS,
    EvalDefaults,
    EvalRunConfig,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.resolve.eval_name import NAME_LIMIT, PROBE_SHA7, probe_name
from snowflake_semantic_tools.domain.validate.shared import Emitter

_PINNED_VERSION = re.compile(r"VERSION\$[1-9]\d*")
_BASELINE_RUNS_MIN = 1
# Agent tool types an eval run skips rather than fails (SST-VAL732).
_SKIPPED_TOOL_TYPES = ("mcp", "agent")


def validate_eval_config(
    resolved: ResolvedEval,
    defaults: EvalDefaults,
    run_names: dict[tuple[str, str], str],
) -> tuple[Diagnostic, ...]:
    """Check one eval's config, and claim its rendered run name in `run_names`.

    Args:
        run_names: Rendered run names claimed so far, keyed by the casefolded agent and run
            name, each mapped to the eval that claimed it; this eval's is added when free.

    Diagnostics:
        SST-VAL719: the effective agent_version is not committed, alias:<name> or VERSION$<n>.
        SST-VAL721: a custom metric the config names did not resolve, or a system metric
            name is unknown.
        SST-VAL762: dataset.name_template or dataset.source_table_template is not set.
        SST-PRS010: the rendered source table name is empty or longer than 128 characters.
        SST-PRS101: the config enables no system metric and names no custom metric.
        SST-VAL722: a system metric has no version, here or in `evals.+metric_version`.
        SST-PRS013: a system metric's effective version is not v3, or run.accept_statuses
            lists a status other than COMPLETED.
        SST-VAL723: a system metric pins a legacy version.
        SST-VAL724: a system metric sets judge_model.
        SST-VAL725: tool_selection_accuracy uses no LLM judge (info).
        SST-VAL726: logical_consistency is gated.
        SST-VAL733: a gated system metric has no usable threshold, or an ungated one has one.
        SST-VAL718: the rendered run name does not include the commit SHA.
        SST-VAL717: another eval of the same agent already claimed the rendered run name.
        SST-PRS016: the effective retry, concurrency or baseline_runs is below its minimum.
        SST-VAL731: run.concurrency exceeds `evals.+concurrency`.
        SST-VAL735: a system metric has a threshold and the effective baseline_runs is not positive.
        SST-VAL732: the agent has tools an eval run skips, one per tool type (info).
    """
    return (
        *_agent_version(resolved, defaults),
        *_unresolved_custom_metrics(resolved),
        *_dataset_templates(resolved),
        *_source_table_name(resolved),
        *_declares_metrics(resolved),
        *(
            diagnostic
            for metric in resolved.config.system_metrics
            for diagnostic in _system_metric(resolved, defaults, metric)
        ),
        *_run(resolved, defaults, run_names),
    )


def _emitter(resolved: ResolvedEval) -> Emitter:
    return Emitter(subject=resolved.key, origin=resolved.config.origin, artifact=resolved.name)


def _agent_version(resolved: ResolvedEval, defaults: EvalDefaults) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    version = resolved.config.agent_version or defaults.agent_version
    if version != "committed" and not (
        isinstance(version, str) and (version.startswith("alias:") or _PINNED_VERSION.fullmatch(version))
    ):
        emit("SST-VAL719", found=version)
    return emit.diagnostics


def _unresolved_custom_metrics(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    configured = {name.casefold() for name in resolved.config.custom_metric_names}
    found = {metric.name.casefold() for metric in resolved.custom_metrics}
    for name in sorted(configured - found):
        emit("SST-VAL721", name=name)
    return emit.diagnostics


def _dataset_templates(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    dataset = resolved.config.dataset
    for field_name in ("name_template", "source_table_template"):
        if dataset is None or getattr(dataset, field_name) is None:
            emit("SST-VAL762", field=field_name)
    return emit.diagnostics


def _source_table_name(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    # Only the source table's length is checked here: the dataset name's is SST-VAL702, so
    # one over-long name is never reported twice.
    dataset = resolved.config.dataset
    template = dataset.source_table_template if dataset is not None else None
    name = probe_name(template, resolved.agent.name) if template is not None else None
    if name is not None and (not name or len(name) > NAME_LIMIT):
        return (
            D(
                "SST-PRS010",
                name=name,
                size=len(name),
                expected=NAME_LIMIT,
                origin=resolved.config.origin,
                subject=resolved.key,
            ),
        )
    return ()


def _declares_metrics(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    config = resolved.config
    if not config.system_metrics and not config.custom_metric_names:
        emit("SST-PRS101", artifact=config.source_file, field="metrics")
    return emit.diagnostics


def _system_metric(resolved: ResolvedEval, defaults: EvalDefaults, metric: EvalSystemMetric) -> tuple[Diagnostic, ...]:
    """Check one system metric: that it exists, its version and judge, and its gate and threshold.

    An unknown name is reported alone; none of the metric's other checks apply to it.
    """
    emit = Emitter(subject=resolved.key, origin=metric.origin, artifact=resolved.name)
    name = metric.name or ""
    if name not in SYSTEM_EVAL_METRICS:
        emit("SST-VAL721", name=name)
        return emit.diagnostics
    version = metric.version or defaults.metric_version
    if version is None:
        emit("SST-VAL722", name=name)
    elif version != SYSTEM_EVAL_METRIC_VERSION:
        emit(
            "SST-PRS013",
            artifact=resolved.config.source_file,
            field=f"metrics.system.{name}.version",
            found=version,
            expected=SYSTEM_EVAL_METRIC_VERSION,
        )
        emit("SST-VAL723", name=name, found=version)
    if metric.judge_model is not None:
        emit("SST-VAL724", name=name)
    if name == "tool_selection_accuracy":
        emit("SST-VAL725", name=name, detail="uses no LLM judge")
    if name == "logical_consistency" and metric.gate:
        emit("SST-VAL726")
    if metric.gate:
        if not _usable_threshold(metric.threshold):
            emit("SST-VAL733", name=name, detail="no bound" if metric.threshold is None else "inverted bounds")
    elif metric.threshold is not None:
        emit("SST-VAL733", name=name, detail="a threshold on an ungated metric")
    return emit.diagnostics


def _run(
    resolved: ResolvedEval, defaults: EvalDefaults, run_names: dict[tuple[str, str], str]
) -> tuple[Diagnostic, ...]:
    # A config without a run block gets none of these checks, SST-VAL732 included.
    run = resolved.config.run
    if run is None:
        return ()
    return (
        *_run_name(resolved, run, run_names),
        *_run_limits(resolved, run, defaults),
        *_threshold_baseline(resolved, run, defaults),
        *_accept_statuses(resolved, run),
        *_skipped_tool_types(resolved),
    )


def _run_name(
    resolved: ResolvedEval, run: EvalRunConfig, run_names: dict[tuple[str, str], str]
) -> tuple[Diagnostic, ...]:
    if run.name_template is None:
        return ()
    run_name = probe_name(run.name_template, resolved.agent.name, run.variant)
    if run_name is None:
        return ()
    emit = _emitter(resolved)
    if PROBE_SHA7 not in run_name:
        emit("SST-VAL718", value=run_name)
    run_key = (resolved.agent.name.casefold(), run_name.casefold())
    if run_key in run_names:
        emit("SST-VAL717", value=run_name)
    else:
        run_names[run_key] = resolved.name
    return emit.diagnostics


def _run_limits(resolved: ResolvedEval, run: EvalRunConfig, defaults: EvalDefaults) -> tuple[Diagnostic, ...]:
    # The ceiling is checked between the minimums, so SST-VAL731 keeps its place among the SST-PRS016s.
    return (
        *_below_minimum(resolved, "run.retry", _effective(run.retry, defaults.retry), EVAL_RETRY_MIN),
        *_below_minimum(
            resolved, "run.concurrency", _effective(run.concurrency, defaults.concurrency), EVAL_CONCURRENCY_MIN
        ),
        *_concurrency_ceiling(resolved, run, defaults),
        *_below_minimum(
            resolved, "run.baseline_runs", _effective(run.baseline_runs, defaults.baseline_runs), _BASELINE_RUNS_MIN
        ),
    )


def _below_minimum(resolved: ResolvedEval, field: str, value: int | None, minimum: int) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    if value is not None and value < minimum:
        emit("SST-PRS016", artifact=resolved.config.source_file, field=field, found=value, expected=f">= {minimum}")
    return emit.diagnostics


def _concurrency_ceiling(resolved: ResolvedEval, run: EvalRunConfig, defaults: EvalDefaults) -> tuple[Diagnostic, ...]:
    # Compares the authored value: an inherited one is the ceiling itself.
    emit = _emitter(resolved)
    if defaults.concurrency is not None and run.concurrency is not None and run.concurrency > defaults.concurrency:
        emit("SST-VAL731", found=run.concurrency, expected=defaults.concurrency)
    return emit.diagnostics


def _threshold_baseline(resolved: ResolvedEval, run: EvalRunConfig, defaults: EvalDefaults) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    has_threshold = any(
        (metric.name or "") in SYSTEM_EVAL_METRICS and metric.threshold is not None
        for metric in resolved.config.system_metrics
    )
    baseline_runs = _effective(run.baseline_runs, defaults.baseline_runs)
    if has_threshold and not (baseline_runs or 0) > 0:
        emit("SST-VAL735", name="gated metric")
    return emit.diagnostics


def _accept_statuses(resolved: ResolvedEval, run: EvalRunConfig) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    for status in run.accept_statuses:
        if status not in EVAL_PASS_STATUSES:
            emit(
                "SST-PRS013",
                artifact=resolved.config.source_file,
                field="run.accept_statuses",
                found=status,
                expected=EVAL_COMPLETED,
            )
    return emit.diagnostics


def _skipped_tool_types(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    emit = _emitter(resolved)
    for tool_type in sorted({tool.type for tool in resolved.agent.tools if tool.type in _SKIPPED_TOOL_TYPES}):
        emit("SST-VAL732", value=tool_type)
    return emit.diagnostics


def _effective(value: int | None, default: int | None) -> int | None:
    return value if value is not None else default


def _usable_threshold(value: ThresholdRange | None) -> bool:
    if value is None or (value.min is None and value.max is None):
        return False
    return value.min is None or value.max is None or value.min <= value.max
