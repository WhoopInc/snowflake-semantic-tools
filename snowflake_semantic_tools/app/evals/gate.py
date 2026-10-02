"""Metadata-only baseline capture and retrospective eval regression gating."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunResult
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_COMPLETED,
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalGateState,
    EvalGateVerdict,
    EvalRegression,
    EvalRunAttempt,
)
from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore

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
    """Capture a baseline from a run: which metrics passed on which questions, and nothing more.

    Only metadata is kept -- pass flags, score ranges, and the identities of the dataset, the
    config, the agent version and the metrics -- never an input, an output or a score. The first
    `required_attempts` completed attempts are captured, else the eval's `baseline_runs`, else
    one; the baseline expires `BASELINE_TTL_DAYS` after its capture.

    Raises:
        ValueError: the reason is blank; an attempt did not complete or its results were not
            read; too few attempts completed; the attempts do not share one immutable agent
            version or one complete question/metric vector; `captured_at` does not parse; or
            the tier is invalid.
    """
    if not reason.strip():
        raise ValueError("baseline capture requires a non-empty reason")
    if any(attempt.terminal_status != EVAL_COMPLETED or attempt.retrieval_error for attempt in result.attempts):
        raise ValueError("baseline capture refuses partial, cancelled, failed, or unretrievable attempts")
    required = required_attempts or _baseline_runs(compiled)
    completed = _completed_attempts(result)
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
    """Decide whether a run regressed against its baseline, or say why the gate cannot tell.

    A metric regresses on a question when it is gated, passed in every baseline attempt and
    failed in some current one. Only a blocking tier fails on a regression, and reports it as
    SST-VAL763; a report tier passes and lists it. Without a usable baseline or a clean current
    run the verdict has no signal: it does not pass, and its reason says why -- `baseline_absent`,
    `current_no_signal`, `baseline_incompatible`, `baseline_expired` or `retrieval_no_signal`.

    Raises:
        ValueError: the tier is invalid, a timestamp does not parse, or a current attempt's
            question/metric vector is malformed.

    Diagnostics:
        SST-VAL758: the eval has no baseline.
        SST-VAL759: the baseline was captured for another payload, agent version, dataset,
            metric set or question/metric vector.
        SST-VAL760: the baseline expires within `BASELINE_WARNING_DAYS`.
        SST-VAL761: the baseline has expired.
        SST-VAL763: a blocking eval regressed; the error fails the run.
        SST-SNO001: the current run has no immutable agent version, or has a partial,
            unretrievable or no completed attempt.
    """
    tier = _resolved_tier(compiled, default_tier)
    if baseline is None:
        diagnostic = D("SST-VAL758", artifact=compiled.artifact_key)
        return EvalGateVerdict(tier, (), False, "baseline_absent"), DiagnosticBag((diagnostic,))
    unusable = _unusable_baseline(compiled, result, baseline, now, default_tier)
    if unusable is not None:
        reason, diagnostic = unusable
        return EvalGateVerdict(tier, (), False, reason), DiagnosticBag((diagnostic,))
    warnings = _expiry_warning(compiled, baseline, now)
    no_signal = _current_no_signal(compiled, result)
    if no_signal is not None:
        reason, diagnostic = no_signal
        return EvalGateVerdict(tier, (), False, reason), DiagnosticBag((*warnings, diagnostic))
    baseline_vectors = {(item.question_key, item.metric_name): item.passed_attempts for item in baseline.metrics}
    current_vectors = _attempt_vectors(_completed_attempts(result))
    if set(baseline_vectors) != set(current_vectors):
        diagnostic = D("SST-VAL759", artifact=compiled.artifact_key, detail="question/metric vector differs")
        return EvalGateVerdict(tier, (), False, "baseline_incompatible"), DiagnosticBag((*warnings, diagnostic))
    regressions = _regressions(compiled, baseline_vectors, current_vectors)
    if tier != "blocking" or not regressions:
        return EvalGateVerdict(tier, regressions, True, None), DiagnosticBag(warnings)
    metrics = ", ".join(sorted({item.metric_name for item in regressions}))
    regressed = D("SST-VAL763", artifact=compiled.artifact_key, count=len(regressions), detail=metrics)
    return EvalGateVerdict(tier, regressions, False, None), DiagnosticBag((*warnings, regressed))


def persist_gate(
    store: EvalStateStore,
    target_name: str,
    compiled: CompiledEval,
    result: EvalRunResult,
    verdict: EvalGateVerdict,
    *,
    evaluated_at: str,
) -> EvalGateState:
    """Record an eval's gate verdict in the eval state store, and return what was recorded.

    A blocking gate stays unresolved while it fails or has no signal; a report gate never does.
    """
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


def _unusable_baseline(
    compiled: CompiledEval,
    result: EvalRunResult,
    baseline: EvalBaselineRecord,
    now: str,
    default_tier: str | None,
) -> tuple[str, Diagnostic] | None:
    """Say why a baseline cannot judge the run, as a verdict reason and its diagnostic; None when it can.

    Checked in order: the run's agent version, the baseline's compatibility, then its expiry.
    """
    try:
        current_agent_version = _result_agent_version(result)
    except ValueError as exc:
        return "current_no_signal", D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' {exc}")
    incompatibility = _incompatibility(compiled, baseline, default_tier, current_agent_version)
    if incompatibility is not None:
        return "baseline_incompatible", D("SST-VAL759", artifact=compiled.artifact_key, detail=incompatibility)
    if _parse_timestamp(now) >= _parse_timestamp(baseline.expires_at):
        return "baseline_expired", D("SST-VAL761", artifact=compiled.artifact_key, date=baseline.expires_at)
    return None


def _expiry_warning(compiled: CompiledEval, baseline: EvalBaselineRecord, now: str) -> tuple[Diagnostic, ...]:
    """Warn with SST-VAL760 in a baseline's last `BASELINE_WARNING_DAYS`, else report nothing."""
    if _parse_timestamp(baseline.expires_at) - _parse_timestamp(now) <= timedelta(days=BASELINE_WARNING_DAYS):
        return (D("SST-VAL760", artifact=compiled.artifact_key, date=baseline.expires_at),)
    return ()


def _current_no_signal(compiled: CompiledEval, result: EvalRunResult) -> tuple[str, Diagnostic] | None:
    """Say why the run gives the gate no signal, as a verdict reason and its diagnostic; None when it does."""
    if any(
        attempt.terminal_status != EVAL_COMPLETED or attempt.retrieval_error is not None for attempt in result.attempts
    ):
        detail = f"eval '{compiled.artifact_key}' has partial or unretrievable current attempts"
        return "current_no_signal", D("SST-SNO001", detail=detail)
    if not _completed_attempts(result):
        detail = f"eval '{compiled.artifact_key}' has no retrievable completed attempt"
        return "retrieval_no_signal", D("SST-SNO001", detail=detail)
    return None


def _regressions(
    compiled: CompiledEval,
    baseline_vectors: dict[tuple[str, str], tuple[bool, ...]],
    current_vectors: dict[tuple[str, str], tuple[bool, ...]],
) -> tuple[EvalRegression, ...]:
    """List, in question/metric order, each gated metric that always passed before and now failed."""
    return tuple(
        EvalRegression(question_key, metric_name)
        for (question_key, metric_name), baseline_flags in sorted(baseline_vectors.items())
        if _is_gated_metric(compiled, metric_name)
        and all(baseline_flags)
        and not all(current_vectors[(question_key, metric_name)])
    )


def _completed_attempts(result: EvalRunResult) -> tuple[EvalRunAttempt, ...]:
    """Return the attempts that completed and whose results were read, in order."""
    return tuple(
        attempt
        for attempt in result.attempts
        if attempt.terminal_status == EVAL_COMPLETED and attempt.retrieval_error is None
    )


def _incompatibility(
    compiled: CompiledEval,
    baseline: EvalBaselineRecord,
    default_tier: str | None,
    current_agent_version: str,
) -> str | None:
    """Say why a baseline cannot judge the current eval; None when it can.

    The baseline must record what the eval compiles to now: its key, dataset and config
    fingerprints, metric versions, each metric's score range and gating, tier and gate policy,
    and the agent version the current run resolved.
    """
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
    completed = _completed_attempts(result)
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
    """Return a metric's threshold as `(min, max)`, by casefolded name; None when it has none.

    A system metric's threshold wins over a custom metric's default of the same name.
    """
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
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
