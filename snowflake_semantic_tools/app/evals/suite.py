"""Run the eval suite against published agents and gate it on stored baselines.

`RunEvalGate` is the evals test suite's use case: it holds the target's run lease -- the
local lock and the remote one `sst apply` takes -- for the run, refuses to start unless
every eval is published exactly as the compiled manifest renders it, refuses before it starts
each eval whose stored baseline cannot gate it, runs the others as `RunEvalSuite` does, and
then either captures each eval's baseline or gates each eval on the one stored. It reaches
Snowflake, the locks, and the baseline store only through ports.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import (
    capture_baseline,
    evaluate_gate,
    gate_refusal,
    persist_gate,
    recorded_judges,
)
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.privileges import EvalRolePort, eval_role_diagnostics
from snowflake_semantic_tools.app.evals.progress import EvalProgress, no_progress
from snowflake_semantic_tools.app.evals.projection import empty_eval_suite_json, eval_suite_json
from snowflake_semantic_tools.app.evals.run import (
    EvalRunResult,
    EvalSuiteResult,
    RunEvalSuite,
    resolve_agent,
    validate_eval_publication,
)
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.model.eval import (
    DEFAULT_EVAL_CONFIG_STAGE,
    EVAL_COMPLETED,
    EvalBaselineRecord,
    EvalDefaults,
    EvalGateVerdict,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import Manifest, State
from snowflake_semantic_tools.domain.state.lock import LockClaim

# The prefix of an eval run's lock id, so SST-APL011 names an eval run holding the lock as one.
EVAL_RUN_PREFIX = "eval-"
# The gate reasons only a new baseline clears: running the suite again can never compare.
RECAPTURE_REASONS = frozenset(("baseline_absent", "baseline_incompatible", "baseline_incomplete", "baseline_expired"))
_DEFAULT_LOCK_POLICY = LockPolicy()


class EvalGatePort(CatalogPublicationPort, StatePort, EvalRolePort, Protocol):
    """The Snowflake roles the eval gate uses: running the suite, reading state, and the run role's grants."""


@dataclass(frozen=True, slots=True)
class EvalGateRequest:
    """How an eval suite run goes: whether it stops early, and whether it captures baselines.

    Attributes:
        reason: Why the baselines change; required to capture, and recorded with each one.
        threads: The evals run at once when neither the project nor any eval says, as
            `--threads` resolved.
        break_stale_lock: Take over the target's lock when the run holding it has expired, as
            `sst apply --break-stale-lock` does; a live holder is refused all the same.
    """

    fail_fast: bool = False
    capture_baseline: bool = False
    reason: str | None = None
    threads: int = 1
    break_stale_lock: bool = False


@dataclass(frozen=True, slots=True)
class EvalGateOutcome:
    """What an eval suite run found, for the CLI to report.

    Attributes:
        diagnostics: What reading state and the publication check reported, then the suite's,
            then each gate's, in eval order.
        suite: The suite result, every eval in eval order, one its gate refused before any run
            started with no attempts; None when the publication check refused to start any eval.
        passed: Whether every eval was accepted and nothing reported an error.
        data: The run as JSON: the suite's projection, and the gate's verdict and regressions.
        recapture: The keys of the evals, in eval order, whose gate compared nothing for a
            reason only a new baseline clears (`RECAPTURE_REASONS`).
    """

    diagnostics: DiagnosticBag
    suite: EvalSuiteResult | None
    passed: bool
    data: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    recapture: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Precheck:
    """What the gate decided about each eval before any run started.

    Attributes:
        baselines: The baseline stored for each eval it checked, None for none, by eval key; an
            eval whose agent version did not resolve was not checked.
        refused: The no-signal verdict, and its diagnostics, of each eval the gate refused.
    """

    baselines: Mapping[str, EvalBaselineRecord | None] = field(default_factory=dict)
    refused: Mapping[str, tuple[EvalGateVerdict, DiagnosticBag]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Gated:
    """What gating, or capturing, every eval of a run gave, in eval order."""

    verdicts: list[EvalGateVerdict] = field(default_factory=list)
    captured: list[EvalBaselineRecord] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)
    recapture: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class EvalGateRefused:
    """The run did not start: another operation holds the target's lock.

    Attributes:
        diagnostics: What taking the lease reported.
    """

    reason: str
    diagnostics: DiagnosticBag = DiagnosticBag()


def compiled_evals(result: CompileResult) -> tuple[CompiledEval, ...]:
    """Return the evals among the compiled artifacts, in compile order."""
    return tuple(item for item in result.compiled if isinstance(item, CompiledEval))


class RunEvalGate:
    """Run the compiled evals against their published agents, then capture or gate each one.

    The target's run lease -- the local lock and the remote one every `sst apply` takes -- is
    held from before state is read until the run ends, so no apply on any machine regenerates
    what the evals run against. The lease is released however the run ends. Evals that run at
    once each lease a session from `sessions`, opened from `port`; without it they run one at
    a time. Each attempt's progress goes to `progress` while the suite runs.
    """

    def __init__(
        self,
        port: EvalGatePort,
        inputs: ProjectInputs,
        state_store: StateStore,
        eval_store: EvalStateStore,
        clock: ClockPort,
        *,
        actor: str = "",
        host: str = "",
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
        sessions: SessionPool[CatalogPublicationPort] | None = None,
        progress: EvalProgress = no_progress,
    ) -> None:
        self._port = port
        self._inputs = inputs
        self._state_store = state_store
        self._eval_store = eval_store
        self._clock = clock
        self._actor = actor
        self._host = host
        self._lock_policy = lock_policy
        self._sessions = sessions
        self._progress = progress

    def run(
        self,
        evals: tuple[CompiledEval, ...],
        manifest: Manifest,
        request: EvalGateRequest,
        *,
        target: TargetIdentity,
        state_table: QualifiedName,
    ) -> EvalGateOutcome | EvalGateRefused:
        """Run the evals and gate them, or capture their baselines, under the target's state lock.

        Steps, in order:

        1. Read the `evals:` defaults and the eval config stage from the project.
        2. Take the target's run lease; refuse when another operation holds either lock.
        3. Read authoritative state, check every eval is published as `manifest` renders it, and
           that the primary role alone holds what a run needs in each agent's schema.
        4. Unless that check reported an error and unless capturing, refuse each eval whose
           stored baseline cannot gate a run against the agent version it resolves to.
        5. Run the other evals, then capture each one's baseline, or evaluate and record each
           gate, a refused one's verdict included.
        6. Release the lease.

        Args:
            manifest: The manifest the evals were compiled into, which `sst compile` wrote.
            target: The live target; baselines and gates are stored under its name.

        Raises:
            SnowflakePortError: the remote lock could not be read or claimed.

        Diagnostics:
            SST-APL011: another run holds the target's lock, so no eval starts.
            SST-APL010: the request breaks stale locks, and an expired one was taken over.
            SST-VAL727, SST-VAL728: the primary role lacks a privilege a run needs in an eval's
                agent schema; as `eval_role_diagnostics` reports, and no eval starts.
            Those of `read_state`, `validate_eval_publication`, `gate_refusal` for each eval
                refused before it starts, the suite, and each gate.
        """
        tree = self._inputs.config().tree
        defaults = self._inputs.eval_catalog().defaults
        stage = config_block(config_block(tree.get("apply")).get("eval_config_stage"))
        lifecycle_config = EvalLifecycleConfig(str(stage.get("stage") or DEFAULT_EVAL_CONFIG_STAGE))
        handler = EvalLifecycleHandler(self._port, lifecycle_config)
        lock_id = f"{EVAL_RUN_PREFIX}{self._clock.new_run_id()}"
        lease = RunLease(
            self._state_store,
            self._port,
            state_table,
            target.name,
            LockClaim(lock_id, self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, lock_diagnostics = lease.acquire(break_stale=request.break_stale_lock)
        if not locked:
            # No eval started, so nothing overlapped what the holder changes; the lock row
            # cannot say what that is.
            holder = lease.holder or "another operation"
            return EvalGateRefused(
                f"cannot run evals while {holder} holds the target lock", DiagnosticBag(lock_diagnostics)
            )
        try:
            state, state_diagnostics = read_state(self._state_store, self._port, state_table=state_table, target=target)
            publication = validate_eval_publication(evals, manifest, state, handler)
            preflight = DiagnosticBag(
                (*lock_diagnostics, *state_diagnostics, *publication, *eval_role_diagnostics(self._port, evals))
            )
            if preflight.has_errors:
                return EvalGateOutcome(preflight, None, False, {"suite": "evals", **empty_eval_suite_json()})
            return self._run_suite(evals, state, preflight, defaults, lifecycle_config, request, target.name)
        finally:
            lease.release()

    def _run_suite(
        self,
        evals: tuple[CompiledEval, ...],
        state: State,
        preflight: DiagnosticBag,
        defaults: EvalDefaults,
        lifecycle_config: EvalLifecycleConfig,
        request: EvalGateRequest,
        target_name: str,
    ) -> EvalGateOutcome:
        """Refuse each eval its baseline cannot gate, run the others, then gate every one.

        Each staged config whose digest state recorded is trusted. A refused eval starts no run,
        and the others still run; with `fail_fast`, a refusal starts none at all, since the run
        already fails.
        """
        checked = _Precheck() if request.capture_baseline else self._precheck(evals, defaults, target_name)
        runnable = tuple(item for item in evals if item.artifact_key not in checked.refused)
        if request.fail_fast and checked.refused:
            runnable = ()
        config_digests = {
            item.artifact_key: digest
            for item in runnable
            if (entry := state.applied.get(item.artifact_key)) is not None
            if (digest := dict(entry.component_fingerprints).get("config_stage_md5")) is not None
        }
        ran = (
            RunEvalSuite(self._port, self._clock, lifecycle_config, self._sessions, self._progress).run(
                runnable,
                defaults=defaults,
                options=EvalRunOptions(self._inputs.git_sha()),
                fail_fast=request.fail_fast,
                config_digests=config_digests,
                baseline_capture=request.capture_baseline,
                threads=request.threads,
            )
            if runnable
            else EvalSuiteResult((), DiagnosticBag())
        )
        suite = _with_refused(evals, ran, checked)
        gated = self._gate(evals, suite, defaults, request, target_name, checked)
        diagnostics = DiagnosticBag((*preflight, *suite.diagnostics, *gated.diagnostics))
        # A report-tier regression adds no diagnostic: it reaches the caller through the
        # verdict in `data` and the gate state recorded for it. A blocking one is SST-VAL763,
        # an error, so the run fails.
        passed = suite.success and not diagnostics.has_errors
        data = _gate_data(suite, gated.verdicts, gated.captured, request)
        return EvalGateOutcome(diagnostics, suite, passed, data, tuple(gated.recapture))

    def _precheck(self, evals: tuple[CompiledEval, ...], defaults: EvalDefaults, target_name: str) -> _Precheck:
        """Read each eval's baseline and refuse, before any run starts, each eval it cannot gate.

        Every attempt runs on the version the eval's selector resolves to now, and the run lease
        keeps an apply from committing another until the run ends, so the refusal is exact. An
        eval whose version does not resolve is not checked: its setup reports why, and its gate
        is evaluated after the run as before.

        Raises:
            SnowflakePortError: a stored baseline could not be read.

        Diagnostics:
            Those of `gate_refusal`, for each eval refused.
        """
        baselines: dict[str, EvalBaselineRecord | None] = {}
        refused: dict[str, tuple[EvalGateVerdict, DiagnosticBag]] = {}
        for item in evals:
            try:
                agent = resolve_agent(self._port, item, defaults)
            except SnowflakePortError:
                continue
            stored = baselines[item.artifact_key] = self._eval_store.read_baseline(target_name, item.artifact_key)
            refusal = gate_refusal(
                item,
                stored,
                agent,
                now=self._clock.now_iso(),
                default_tier=defaults.eval_tier,
                default_baseline_runs=defaults.baseline_runs,
            )
            if refusal is not None:
                refused[item.artifact_key] = refusal
        return _Precheck(baselines, refused)

    def _gate(
        self,
        evals: tuple[CompiledEval, ...],
        suite: EvalSuiteResult,
        defaults: EvalDefaults,
        request: EvalGateRequest,
        target_name: str,
        checked: _Precheck,
    ) -> _Gated:
        """Capture each eval's baseline, or evaluate its gate and record the verdict, in eval order.

        Captured baselines are written together once every eval has one, so a capture that
        fails part way writes none. A capture some eval of which completed fewer attempts
        than its baseline needs captures nothing: each attempt has reported its own outcome.
        An eval refused before it ran keeps the verdict it was refused with, and that gate is
        recorded with no runs, so a blocking one stays unresolved; an eval the run did not
        reach, as `--fail-fast` leaves one, is not gated.

        Diagnostics:
            SST-VAL734: under a capture, an eval completed and read fewer attempts than its
                `baseline_runs` setting, else the root's, requires; once per such eval.
        """
        gated = _Gated()
        if request.capture_baseline:
            short = _short_captures(evals, suite, defaults)
            if short:
                gated.diagnostics.extend(short)
                return gated
        results = {result.eval_key: result for result in suite.evals}
        for item in evals:
            item_result = results.get(item.artifact_key)
            if item_result is None:
                continue
            if request.capture_baseline:
                gated.captured.append(
                    capture_baseline(
                        item,
                        item_result,
                        reason=request.reason or "",
                        captured_at=self._clock.now_iso(),
                        default_tier=defaults.eval_tier,
                        required_attempts=_required_attempts(item, defaults),
                    )
                )
                gated.diagnostics.extend(recorded_judges(item))
                continue
            refused = checked.refused.get(item.artifact_key)
            verdict, item_diagnostics = refused or evaluate_gate(
                item,
                item_result,
                self._baseline(checked, target_name, item.artifact_key),
                now=self._clock.now_iso(),
                default_tier=defaults.eval_tier,
                default_baseline_runs=defaults.baseline_runs,
            )
            gated.diagnostics.extend(item_diagnostics)
            persist_gate(self._eval_store, target_name, item, item_result, verdict, evaluated_at=self._clock.now_iso())
            gated.verdicts.append(verdict)
            if verdict.reason in RECAPTURE_REASONS:
                gated.recapture.append(item.artifact_key)
        if request.capture_baseline:
            self._eval_store.write_baselines(target_name, tuple(gated.captured))
        return gated

    def _baseline(self, checked: _Precheck, target_name: str, eval_key: str) -> EvalBaselineRecord | None:
        """Return the eval's stored baseline: the one the pre-check read, else read now."""
        if eval_key in checked.baselines:
            return checked.baselines[eval_key]
        return self._eval_store.read_baseline(target_name, eval_key)


def _with_refused(evals: tuple[CompiledEval, ...], ran: EvalSuiteResult, checked: _Precheck) -> EvalSuiteResult:
    """Return the suite's results with each refused eval in its place, as one that made no attempt.

    The results stay in eval order; an eval neither refused nor run, as `--fail-fast` leaves
    one, is left out.
    """
    results = {result.eval_key: result for result in ran.evals}
    merged = (
        EvalRunResult(item.artifact_key, (), DiagnosticBag())
        if item.artifact_key in checked.refused
        else results.get(item.artifact_key)
        for item in evals
    )
    return EvalSuiteResult(tuple(result for result in merged if result is not None), ran.diagnostics)


def _required_attempts(item: CompiledEval, defaults: EvalDefaults) -> int | None:
    """Return the completed attempts a capture of `item` needs: its `baseline_runs`, else the default."""
    resolved = item.resolved.options.baseline_runs
    return resolved if resolved is not None else defaults.baseline_runs


def _short_captures(
    evals: tuple[CompiledEval, ...], suite: EvalSuiteResult, defaults: EvalDefaults
) -> tuple[Diagnostic, ...]:
    """Report each eval a capture cannot take a baseline of, for too few completed attempts.

    An attempt counts once it completed and its results were read, as `capture_baseline` counts
    it; an eval the run did not reach, as `--fail-fast` leaves one, completed none.

    Diagnostics:
        SST-VAL734: the eval completed fewer attempts than it requires; once per such eval.
    """
    found: list[Diagnostic] = []
    for index, item in enumerate(evals):
        attempts = suite.evals[index].attempts if index < len(suite.evals) else ()
        completed = sum(
            attempt.terminal_status == EVAL_COMPLETED and attempt.retrieval_error is None for attempt in attempts
        )
        required = _required_attempts(item, defaults) or 1
        if completed < required:
            found.append(
                D("SST-VAL734", artifact=item.name, count=completed, expected=required, subject=item.artifact_key)
            )
    return tuple(found)


def _gate_data(
    suite: EvalSuiteResult,
    verdicts: list[EvalGateVerdict],
    captured: list[EvalBaselineRecord],
    request: EvalGateRequest,
) -> dict[str, object]:
    """Project a run for the CLI: the suite's projection, with the gate's verdict filled in.

    The verdict is `captured` for a capture, `not_evaluated` for one that captured nothing;
    otherwise `no_signal` when some gate could not tell, else `regressed` when some eval
    regressed, else `passed`.
    """
    if request.capture_baseline:
        verdict = "captured" if captured else "not_evaluated"
    elif any(item.reason is not None for item in verdicts):
        verdict = "no_signal"
    elif any(item.regression_count for item in verdicts):
        verdict = "regressed"
    else:
        verdict = "passed"
    return {
        "suite": "evals",
        **eval_suite_json(suite),
        "captured_baselines": [record.eval_key for record in captured],
        "regression_count": sum(item.regression_count for item in verdicts),
        "gate_verdict": verdict,
        "gate_reasons": [item.reason for item in verdicts if item.reason is not None],
        "regressions": [
            {"question_key": regression.question_key, "metric_name": regression.metric_name}
            for item in verdicts
            for regression in item.regressions
        ],
    }
