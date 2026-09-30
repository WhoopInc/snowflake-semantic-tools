"""Metadata-only baseline capture and retrospective eval regression gating."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from ..domain.model.diagnostic import D, DiagnosticBag
from ..domain.model.eval import (
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalGateState,
    EvalGateVerdict,
    EvalRegression,
    EvalRunAttempt,
)
from ..domain.ports.eval_state import EvalStateStore
from .eval_compile import CompiledEval
from .eval_run import EvalRunResult

BASELINE_TTL_DAYS = 30
# SST-VAL760 fires in the last week of a baseline's life: long enough to capture a
# replacement, short enough that a fresh baseline does not warn from the day it lands.
BASELINE_WARNING_DAYS = 7


def capture_baseline(
    compiled: CompiledEval,
    result: EvalRunResult,
    *,
    reason: str,
    captured_at: str,
    default_tier: str | None = None,
    required_attempts: int | None = None,
) -> EvalBaselineRecord:
    if not reason.strip():
        raise ValueError("baseline capture requires a non-empty reason")
    if any(attempt.terminal_status != "COMPLETED" or attempt.retrieval_error for attempt in result.attempts):
        raise ValueError("baseline capture refuses partial, cancelled, failed, or unretrievable attempts")
    required = required_attempts or _baseline_runs(compiled)
    completed = tuple(
        attempt
        for attempt in result.attempts
        if attempt.terminal_status == "COMPLETED" and attempt.retrieval_error is None
    )
    if len(completed) < required:
        raise ValueError(f"baseline capture requires {required} completed attempts, found {len(completed)}")
    selected = completed[:required]
    vectors = _attempt_vectors(selected)
    captured = _parse_timestamp(captured_at)
    return EvalBaselineRecord(
        eval_key=compiled.artifact_key,
        dataset_fingerprint=compiled.rendered.dataset_fingerprint,
        config_fingerprint=compiled.rendered.config_fingerprint,
        agent_version=_attempt_agent_version(selected),
        metric_versions=_metric_versions(compiled),
        metrics=tuple(
            EvalBaselineMetric(
                question_key,
                metric_name,
                flags,
                _score_range(compiled, metric_name),
            )
            for (question_key, metric_name), flags in sorted(vectors.items())
        ),
        run_names=tuple(attempt.run_name for attempt in selected),
        captured_at=_format_timestamp(captured),
        expires_at=_format_timestamp(captured + timedelta(days=BASELINE_TTL_DAYS)),
        reason=reason.strip(),
        tier=_resolved_tier(compiled, default_tier),
        gate_policy=_gate_policy(compiled),
    )


def evaluate_gate(
    compiled: CompiledEval,
    result: EvalRunResult,
    baseline: EvalBaselineRecord | None,
    *,
    now: str,
    default_tier: str | None = None,
) -> tuple[EvalGateVerdict, DiagnosticBag]:
    tier = _resolved_tier(compiled, default_tier)
    if baseline is None:
        diagnostic = D("SST-VAL758", artifact=compiled.artifact_key)
        return EvalGateVerdict(tier, (), False, "baseline_absent"), DiagnosticBag((diagnostic,))
    try:
        current_agent_version = _result_agent_version(result)
    except ValueError as exc:
        diagnostic = D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' {exc}")
        return EvalGateVerdict(tier, (), False, "current_no_signal"), DiagnosticBag((diagnostic,))
    incompatibility = _incompatibility(compiled, baseline, default_tier, current_agent_version)
    if incompatibility is not None:
        diagnostic = D("SST-VAL759", artifact=compiled.artifact_key, detail=incompatibility)
        return EvalGateVerdict(tier, (), False, "baseline_incompatible"), DiagnosticBag((diagnostic,))
    now_value = _parse_timestamp(now)
    expiry = _parse_timestamp(baseline.expires_at)
    if now_value >= expiry:
        diagnostic = D("SST-VAL761", artifact=compiled.artifact_key, date=baseline.expires_at)
        return EvalGateVerdict(tier, (), False, "baseline_expired"), DiagnosticBag((diagnostic,))
    diagnostics = []
    if expiry - now_value <= timedelta(days=BASELINE_WARNING_DAYS):
        diagnostics.append(D("SST-VAL760", artifact=compiled.artifact_key, date=baseline.expires_at))
    completed = tuple(
        attempt
        for attempt in result.attempts
        if attempt.terminal_status == "COMPLETED" and attempt.retrieval_error is None
    )
    invalid_attempts = tuple(
        attempt
        for attempt in result.attempts
        if attempt.terminal_status != "COMPLETED" or attempt.retrieval_error is not None
    )
    if invalid_attempts:
        diagnostic = D(
            "SST-SNO001",
            detail=f"eval '{compiled.artifact_key}' has partial or unretrievable current attempts",
        )
        return EvalGateVerdict(tier, (), False, "current_no_signal"), DiagnosticBag((*diagnostics, diagnostic))
    if not completed:
        diagnostic = D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' has no retrievable completed attempt")
        return EvalGateVerdict(tier, (), False, "retrieval_no_signal"), DiagnosticBag((*diagnostics, diagnostic))
    baseline_vectors = {(item.question_key, item.metric_name): item.passed_attempts for item in baseline.metrics}
    current_vectors = _attempt_vectors(completed)
    if set(baseline_vectors) != set(current_vectors):
        diagnostic = D("SST-VAL759", artifact=compiled.artifact_key, detail="question/metric vector differs")
        return EvalGateVerdict(tier, (), False, "baseline_incompatible"), DiagnosticBag((*diagnostics, diagnostic))
    regressions = tuple(
        EvalRegression(question_key, metric_name)
        for (question_key, metric_name), baseline_flags in sorted(baseline_vectors.items())
        if _is_gated_metric(compiled, metric_name)
        and all(baseline_flags)
        and not all(current_vectors[(question_key, metric_name)])
    )
    passed = tier != "blocking" or not regressions
    return EvalGateVerdict(tier, regressions, passed, None), DiagnosticBag(tuple(diagnostics))


def persist_gate(
    store: EvalStateStore,
    target_name: str,
    compiled: CompiledEval,
    result: EvalRunResult,
    verdict: EvalGateVerdict,
    *,
    evaluated_at: str,
) -> EvalGateState:
    state = EvalGateState(
        eval_key=compiled.artifact_key,
        tier=verdict.tier,
        regression_count=verdict.regression_count,
        regressions=verdict.regressions,
        unresolved=verdict.tier == "blocking" and (not verdict.passed or verdict.reason is not None),
        evaluated_at=evaluated_at,
        run_names=tuple(attempt.run_name for attempt in result.attempts),
    )
    store.write_gate(target_name, state)
    return state


def _incompatibility(
    compiled: CompiledEval,
    baseline: EvalBaselineRecord,
    default_tier: str | None,
    current_agent_version: str,
) -> str | None:
    current = (
        compiled.artifact_key,
        compiled.rendered.dataset_fingerprint,
        compiled.rendered.config_fingerprint,
        current_agent_version,
        _metric_versions(compiled),
        _metric_policy(compiled),
        _resolved_tier(compiled, default_tier),
        _gate_policy(compiled),
    )
    recorded = (
        baseline.eval_key,
        baseline.dataset_fingerprint,
        baseline.config_fingerprint,
        baseline.agent_version,
        baseline.metric_versions,
        tuple(
            sorted(
                {
                    (item.metric_name, item.score_range, _is_gated_metric(compiled, item.metric_name))
                    for item in baseline.metrics
                }
            )
        ),
        baseline.tier,
        baseline.gate_policy,
    )
    return None if current == recorded else "eval payload, agent version, dataset or metric identity changed"


def _baseline_runs(compiled: CompiledEval) -> int:
    run = compiled.resolved.config.run
    return run.baseline_runs if run is not None and run.baseline_runs is not None else 1


def _attempt_agent_version(attempts: tuple[EvalRunAttempt, ...]) -> str:
    versions = {attempt.agent_version for attempt in attempts if attempt.agent_version}
    if len(versions) != 1:
        raise ValueError("baseline attempts do not identify one immutable agent version")
    version = next(iter(versions))
    if re.fullmatch(r"VERSION\$[1-9]\d*", version) is None:
        raise ValueError(f"baseline attempt agent version {version!r} is not immutable")
    return version


def _result_agent_version(result: EvalRunResult) -> str:
    completed = tuple(
        attempt
        for attempt in result.attempts
        if attempt.terminal_status == "COMPLETED" and attempt.retrieval_error is None
    )
    if not completed:
        raise ValueError("does not identify one immutable current agent version")
    return _attempt_agent_version(completed)


def _metric_versions(compiled: CompiledEval) -> tuple[tuple[str, str], ...]:
    values = [
        (metric.name or "", metric.version or "")
        for metric in compiled.resolved.config.system_metrics
        if metric.name is not None
    ]
    values.extend(
        (metric.name, f"custom:{metric.model or ''}") for metric in compiled.resolved.custom_metrics if metric.enabled
    )
    return tuple(sorted(values))


def _metric_policy(
    compiled: CompiledEval,
) -> tuple[tuple[str, tuple[float | None, float | None] | None, bool], ...]:
    names = [metric.name or "" for metric in compiled.resolved.config.system_metrics]
    names.extend(metric.name for metric in compiled.resolved.custom_metrics if metric.enabled)
    return tuple(sorted((name, _score_range(compiled, name), _is_gated_metric(compiled, name)) for name in names))


def _gate_policy(compiled: CompiledEval) -> tuple[tuple[str, bool], ...]:
    return tuple((name, gated) for name, _, gated in _metric_policy(compiled))


def _resolved_tier(compiled: CompiledEval, default_tier: str | None) -> str:
    run_tier = compiled.resolved.config.run.tier if compiled.resolved.config.run is not None else None
    tier = (run_tier or default_tier or "report").casefold()
    if tier not in {"blocking", "report"}:
        raise ValueError(f"invalid eval tier {tier!r}")
    return tier


def _score_range(compiled: CompiledEval, metric_name: str) -> tuple[float | None, float | None] | None:
    system = next(
        (
            metric
            for metric in compiled.resolved.config.system_metrics
            if (metric.name or "").casefold() == metric_name.casefold()
        ),
        None,
    )
    threshold = system.threshold if system is not None else None
    if system is None:
        custom = next(
            (metric for metric in compiled.resolved.custom_metrics if metric.name.casefold() == metric_name.casefold()),
            None,
        )
        threshold = custom.threshold_default if custom is not None else None
    return (threshold.min, threshold.max) if threshold is not None else None


def _is_gated_metric(compiled: CompiledEval, metric_name: str) -> bool:
    system = next(
        (
            metric
            for metric in compiled.resolved.config.system_metrics
            if (metric.name or "").casefold() == metric_name.casefold()
        ),
        None,
    )
    if system is not None:
        return bool(system.gate)
    custom = next(
        (metric for metric in compiled.resolved.custom_metrics if metric.name.casefold() == metric_name.casefold()),
        None,
    )
    return bool(custom and custom.gate_default)


def _attempt_vectors(attempts: tuple[EvalRunAttempt, ...]) -> dict[tuple[str, str], tuple[bool, ...]]:
    values: dict[tuple[str, str], list[bool]] = {}
    for attempt in attempts:
        seen = set()
        for row in attempt.rows:
            for metric in row.metrics:
                key = (metric.question_key, metric.metric_name)
                if key in seen:
                    raise ValueError(f"attempt {attempt.run_name!r} duplicates {key!r}")
                seen.add(key)
                values.setdefault(key, []).append(metric.passed)
        if set(values) and seen != set(values):
            raise ValueError(f"attempt {attempt.run_name!r} has an incomplete question/metric vector")
    if not values:
        raise ValueError("baseline attempts contain no metric vectors")
    return {key: tuple(flags) for key, flags in values.items()}


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
