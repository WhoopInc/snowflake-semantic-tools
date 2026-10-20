"""Metadata-only baseline capture and retrospective eval regression gating.

The gate's checks (SST-VAL758 to SST-VAL763) stay in `app` rather than `domain.validate`:
they judge a run against a baseline read through the `EvalStateStore` port, and date it
with `datetime`, which the pure ring may not import.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunResult, ResolvedAgent
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_COMPLETED,
    CustomEvalMetric,
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
    `required_attempts` completed attempts whose results were read are captured, in attempt
    order, else the eval's `baseline_runs`, else one; the baseline expires `BASELINE_TTL_DAYS`
    after its capture. An attempt that failed, was partial or was not read is never captured:
    the run reported it, and the retry that completed in its place counts instead. The agent
    version's specification fingerprint is recorded when every captured attempt read the same.

    Raises:
        ValueError: the reason is blank; too few attempts completed and were read; the
            captured attempts do not share one immutable agent version or one complete
            question/metric vector; `captured_at` does not parse; or the tier is invalid.
    """
    if not reason.strip():
        raise ValueError("baseline capture requires a non-empty reason")
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
        agent_version=_attempt_agent_version(selected, "baseline"),
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
        agent_spec_fingerprint=_attempt_spec_fingerprint(selected),
    )


def recorded_judges(compiled: CompiledEval) -> DiagnosticBag:
    """Report, for a captured baseline, the version each system metric's judge was recorded under.

    A system metric's judge comes with its version and moves on Snowflake's cadence, so the
    baseline's `metric_versions` is what explains a later score shift.

    Diagnostics:
        SST-VAL746: one per system metric, in config order (info).
    """
    return DiagnosticBag(
        tuple(
            D("SST-VAL746", artifact=metric.name, value=metric.version or "", subject=compiled.artifact_key)
            for metric in compiled.resolved.config.system_metrics
            if metric.name is not None
        )
    )


def evaluate_gate(
    compiled: CompiledEval,
    result: EvalRunResult,
    baseline: EvalBaselineRecord | None,
    *,
    now: str,
    default_tier: str | None = None,
    default_baseline_runs: int | None = None,
) -> tuple[EvalGateVerdict, DiagnosticBag]:
    """Decide whether a run regressed against its baseline, or say why the gate cannot tell.

    A metric regresses on a question when it is gated, passed in every baseline attempt and
    failed in some current one. Only the current attempts that completed and were read are
    judged: one that failed or was partial is never a pass and scores nothing, and the run
    reports it (SST-APL029 when a retry completed in its place). Only a blocking tier fails on a
    regression, and reports it as SST-VAL763; a report tier passes and lists it. Without a
    usable baseline or a completed current attempt the verdict has no signal: it does not pass,
    and its reason says why -- `baseline_absent`, `current_no_signal`, `baseline_incompatible`,
    `baseline_incomplete` or `baseline_expired`.

    Raises:
        ValueError: the tier is invalid, a timestamp does not parse, or a current attempt's
            question/metric vector is malformed.

    Diagnostics:
        SST-VAL758: the eval has no baseline.
        SST-VAL745: a custom metric the baseline's runs scored was edited in place since.
        SST-VAL759: the baseline was captured for another payload, agent version, dataset,
            metric set or question/metric vector; the message names each part that differs.
        SST-VAL734: the baseline holds fewer runs than `baseline_runs` now requires.
        SST-VAL760: the baseline expires within `BASELINE_WARNING_DAYS`.
        SST-VAL761: the baseline has expired.
        SST-VAL763: a blocking eval regressed; the error fails the run.
        SST-SNO001: no current attempt completed and was read, or the ones that did have no
            one immutable agent version.
    """
    tier = _resolved_tier(compiled, default_tier)
    if baseline is None:
        return _no_signal(tier, _absent(compiled))
    try:
        agent = _result_agent(result)
    except ValueError as exc:
        return _no_signal(tier, ("current_no_signal", D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' {exc}")))
    unusable = _unusable_baseline(compiled, baseline, agent, now, default_tier, default_baseline_runs)
    if unusable is not None:
        return _no_signal(tier, unusable)
    warnings = _expiry_warning(compiled, baseline, now)
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


def gate_refusal(
    compiled: CompiledEval,
    baseline: EvalBaselineRecord | None,
    agent: ResolvedAgent,
    *,
    now: str,
    default_tier: str | None = None,
    default_baseline_runs: int | None = None,
) -> tuple[EvalGateVerdict, DiagnosticBag] | None:
    """Say why the gate could not judge a run against `agent`, before it starts; None when it could.

    Every attempt of a run uses the version the eval's selector resolved to, so each check
    `evaluate_gate` makes before it reads the attempts is decided here, in the same order and
    with the same verdict. One that passes here is checked again once the run's attempts are in.

    Raises:
        ValueError: the tier is invalid, or a timestamp does not parse.

    Diagnostics:
        SST-VAL758: the eval has no baseline.
        SST-VAL745: a custom metric the baseline's runs scored was edited in place since.
        SST-VAL759: the baseline was captured for another payload, agent version, dataset or
            metric set; the message names each part that differs.
        SST-VAL734: the baseline holds fewer runs than `baseline_runs` now requires.
        SST-VAL761: the baseline has expired.
    """
    tier = _resolved_tier(compiled, default_tier)
    found = (
        _absent(compiled)
        if baseline is None
        else _unusable_baseline(compiled, baseline, agent, now, default_tier, default_baseline_runs)
    )
    return None if found is None else _no_signal(tier, found)


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


def _no_signal(tier: str, found: tuple[str, Diagnostic]) -> tuple[EvalGateVerdict, DiagnosticBag]:
    """Return the verdict of a gate that compared nothing, for `found`'s reason, and its diagnostic."""
    reason, diagnostic = found
    return EvalGateVerdict(tier, (), False, reason), DiagnosticBag((diagnostic,))


def _absent(compiled: CompiledEval) -> tuple[str, Diagnostic]:
    return "baseline_absent", D("SST-VAL758", artifact=compiled.artifact_key)


def _unusable_baseline(
    compiled: CompiledEval,
    baseline: EvalBaselineRecord,
    agent: ResolvedAgent,
    now: str,
    default_tier: str | None,
    default_baseline_runs: int | None,
) -> tuple[str, Diagnostic] | None:
    """Say why a baseline cannot judge a run against `agent`, as a verdict reason and its diagnostic.

    None when it can. Checked in order: an in-place metric edit, the baseline's compatibility,
    its run count, then its expiry.
    """
    edited = _edited_metric(compiled, baseline)
    if edited is not None:
        diagnostic = D("SST-VAL745", artifact=edited, value=baseline.run_names[-1], subject=f"eval_metric:{edited}")
        return "baseline_incompatible", diagnostic
    incompatibility = _incompatibility(compiled, baseline, default_tier, agent)
    if incompatibility is not None:
        return "baseline_incompatible", D("SST-VAL759", artifact=compiled.artifact_key, detail=incompatibility)
    required = _baseline_runs(compiled, default_baseline_runs)
    if len(baseline.run_names) < required:
        diagnostic = D(
            "SST-VAL734",
            artifact=compiled.name,
            count=len(baseline.run_names),
            expected=required,
            subject=compiled.artifact_key,
        )
        return "baseline_incomplete", diagnostic
    if _parse_timestamp(now) >= _parse_timestamp(baseline.expires_at):
        return "baseline_expired", D("SST-VAL761", artifact=compiled.artifact_key, date=baseline.expires_at)
    return None


def _expiry_warning(compiled: CompiledEval, baseline: EvalBaselineRecord, now: str) -> tuple[Diagnostic, ...]:
    """Warn with SST-VAL760 in a baseline's last `BASELINE_WARNING_DAYS`, else report nothing."""
    if _parse_timestamp(baseline.expires_at) - _parse_timestamp(now) <= timedelta(days=BASELINE_WARNING_DAYS):
        return (D("SST-VAL760", artifact=compiled.artifact_key, date=baseline.expires_at),)
    return ()


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
    agent: ResolvedAgent,
) -> str | None:
    """Name, joined by `; `, each part of the eval that differs from what its baseline recorded.

    None when nothing does. The baseline must record the agent version the run resolves, and
    what the eval compiles to now: its key, dataset and config fingerprints, metric versions,
    each metric's score range and gating, tier and gate policy. Whether those match is decided
    on the whole identity; the parts only say which differ.
    """
    changes = list(_agent_change(baseline, agent))
    if _identity(compiled, default_tier) != _recorded_identity(compiled, baseline):
        changes.extend(_identity_changes(compiled, baseline, default_tier) or ("the metric identity changed",))
    return "; ".join(changes) or None


def _agent_change(baseline: EvalBaselineRecord, agent: ResolvedAgent) -> tuple[str, ...]:
    """Name the agent version the run uses when it is not the one the baseline was captured on.

    The version matches when it is the one recorded, or when it and the baseline both carry an
    `sst-spec:` fingerprint and the two are equal: a revert commits a new version holding the
    same specification. A version without a fingerprint, or a baseline recorded without one,
    matches only by name.
    """
    if baseline.agent_version == agent.version:
        return ()
    recorded, current = baseline.agent_spec_fingerprint, agent.spec_fingerprint
    if recorded is not None and recorded == current:
        return ()
    if recorded is None:
        held = ""
    elif current is None:
        held = ", which carries no sst-spec fingerprint"
    else:
        held = ", whose specification differs"
    captured_on = baseline.agent_version or "no recorded version"
    return (f"it was captured on {captured_on}, and this run uses {agent.version}{held}",)


def _identity(compiled: CompiledEval, default_tier: str | None) -> tuple[object, ...]:
    """Return what a baseline of the eval as it compiles now records, besides the agent version."""
    return (
        compiled.artifact_key,
        compiled.rendered.dataset_fingerprint,
        compiled.rendered.config_fingerprint,
        _metric_versions(compiled),
        _metric_policy(compiled),
        _resolved_tier(compiled, default_tier),
        _gate_policy(compiled),
    )


def _recorded_identity(compiled: CompiledEval, baseline: EvalBaselineRecord) -> tuple[object, ...]:
    """Return what `baseline` recorded, in `_identity`'s order; a metric's gating is the eval's now."""
    policy = {
        (item.metric_name, item.score_range, _is_gated_metric(compiled, item.metric_name)) for item in baseline.metrics
    }
    return (
        baseline.eval_key,
        baseline.dataset_fingerprint,
        baseline.config_fingerprint,
        baseline.metric_versions,
        tuple(sorted(policy)),
        baseline.tier,
        baseline.gate_policy,
    )


def _identity_changes(
    compiled: CompiledEval, baseline: EvalBaselineRecord, default_tier: str | None
) -> tuple[str, ...]:
    """Name each part of the eval's identity but the agent version that its baseline records otherwise."""
    tier = _resolved_tier(compiled, default_tier)
    parts = (
        (baseline.eval_key != compiled.artifact_key, f"it was captured for '{baseline.eval_key}'"),
        (baseline.dataset_fingerprint != compiled.rendered.dataset_fingerprint, "the dataset changed"),
        (baseline.config_fingerprint != compiled.rendered.config_fingerprint, "the eval config changed"),
    )
    return (
        *(change for differs, change in parts if differs),
        *_metric_changes(compiled, baseline),
        *((f"the tier changed from {baseline.tier} to {tier}",) if baseline.tier != tier else ()),
    )


def _metric_changes(compiled: CompiledEval, baseline: EvalBaselineRecord) -> tuple[str, ...]:
    """Name, by metric name, each metric added, removed, or whose version, threshold or gating moved."""
    versions = (dict(baseline.metric_versions), dict(_metric_versions(compiled)))
    gates = (dict(baseline.gate_policy), dict(_gate_policy(compiled)))
    recorded_ranges: dict[str, set[tuple[float | None, float | None] | None]] = {}
    for item in baseline.metrics:
        recorded_ranges.setdefault(item.metric_name, set()).add(item.score_range)
    ranges = (recorded_ranges, {name: {score_range} for name, score_range, _ in _metric_policy(compiled)})
    recorded = {*versions[0], *gates[0], *ranges[0]}
    current = {*versions[1], *gates[1], *ranges[1]}
    changes: list[str] = []
    for name in sorted(recorded | current):
        if name not in recorded:
            changes.append(f"metric '{name}' was added")
        elif name not in current:
            changes.append(f"metric '{name}' was removed")
        else:
            old_version, new_version = (side.get(name) for side in versions)
            if old_version != new_version:
                changes.append(_version_change(name, old_version, new_version))
            old_ranges = ranges[0].get(name)
            if old_ranges is None:
                changes.append(f"metric '{name}' scored no question in the baseline's runs")
            elif old_ranges != ranges[1].get(name):
                changes.append(f"metric '{name}' changed its threshold")
            if gates[0].get(name) != gates[1].get(name):
                changes.append(f"metric '{name}' changed its gating")
    return tuple(changes)


def _version_change(name: str, recorded: str | None, current: str | None) -> str:
    """Name a metric's version change; a custom metric's version is its judge and definition."""
    if any((version or "").startswith("custom:") for version in (recorded, current)):
        return f"metric '{name}' changed its judge or definition"
    return f"metric '{name}' moved from version {recorded or 'none'} to {current or 'none'}"


def _baseline_runs(compiled: CompiledEval, default: int | None = None) -> int:
    resolved = compiled.resolved.options.baseline_runs
    return resolved if resolved is not None else default or 1


def _attempt_agent_version(attempts: tuple[EvalRunAttempt, ...], run: str) -> str:
    versions = {attempt.agent_version for attempt in attempts if attempt.agent_version}
    if len(versions) != 1:
        raise ValueError(f"{run} attempts do not identify one immutable agent version")
    version = next(iter(versions))
    if re.fullmatch(r"VERSION\$[1-9]\d*", version) is None:
        raise ValueError(f"{run} attempt agent version {version!r} is not immutable")
    return version


def _result_agent(result: EvalRunResult) -> ResolvedAgent:
    """Return the one agent version the run's completed attempts ran against, and its fingerprint.

    Raises:
        ValueError: no attempt completed and was read, or those that did name no one version.
    """
    completed = _completed_attempts(result)
    if not completed:
        raise ValueError("has no retrievable completed attempt")
    return ResolvedAgent(_attempt_agent_version(completed, "current"), _attempt_spec_fingerprint(completed))


def _attempt_spec_fingerprint(attempts: tuple[EvalRunAttempt, ...]) -> str | None:
    """Return the specification fingerprint every attempt read; None when they read none or differ."""
    fingerprints = {attempt.agent_spec_fingerprint for attempt in attempts}
    return next(iter(fingerprints)) if len(fingerprints) == 1 else None


def _metric_versions(compiled: CompiledEval) -> tuple[tuple[str, str], ...]:
    values = [
        (metric.name or "", metric.version or "")
        for metric in compiled.resolved.config.system_metrics
        if metric.name is not None
    ]
    values.extend(
        (metric.name, _custom_version(metric)) for metric in compiled.resolved.custom_metrics if metric.enabled
    )
    return tuple(sorted(values))


def _custom_version(metric: CustomEvalMetric) -> str:
    """A custom metric's identity: its judge model and a digest of its prompt and bands.

    A custom metric has no Snowflake version, so an edited prompt or scale is told apart only
    by this digest.
    """
    return f"custom:{metric.model or ''}:{metric.definition_digest}"


def _edited_metric(compiled: CompiledEval, baseline: EvalBaselineRecord) -> str | None:
    """Name the first custom metric the baseline recorded under another definition; None when none was."""
    current = dict(_metric_versions(compiled))
    for name, recorded in baseline.metric_versions:
        version = current.get(name)
        # A baseline captured before metrics were digested records `custom:<model>`, with no
        # definition to compare; SST-VAL759 reports that it differs.
        if version is not None and recorded.count(":") == 2 and recorded != version:
            return name
    return None


def _metric_policy(
    compiled: CompiledEval,
) -> tuple[tuple[str, tuple[float | None, float | None] | None, bool], ...]:
    names = [metric.name or "" for metric in compiled.resolved.config.system_metrics]
    names.extend(metric.name for metric in compiled.resolved.custom_metrics if metric.enabled)
    return tuple(sorted((name, _score_range(compiled, name), _is_gated_metric(compiled, name)) for name in names))


def _gate_policy(compiled: CompiledEval) -> tuple[tuple[str, bool], ...]:
    return tuple((name, gated) for name, _, gated in _metric_policy(compiled))


def _resolved_tier(compiled: CompiledEval, default_tier: str | None) -> str:
    tier = (compiled.resolved.options.eval_tier or default_tier or "report").casefold()
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
