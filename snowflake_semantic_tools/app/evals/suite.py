"""Run the eval suite against published agents and gate it on stored baselines.

`RunEvalGate` is the evals test suite's use case: it holds the target's run lease -- the
local lock and the remote one `sst apply` takes -- for the run, refuses to start unless
every eval is published exactly as the compiled manifest renders it, runs the evals as
`RunEvalSuite` does, and then either captures each eval's baseline or gates each eval on the
one stored. It reaches Snowflake, the locks, and the baseline store only through ports.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate, persist_gate, recorded_judges
from snowflake_semantic_tools.app.evals.run import (
    EvalRunOptions,
    EvalSuiteResult,
    RunEvalSuite,
    empty_eval_suite_json,
    eval_suite_json,
    validate_eval_publication,
)
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.model.eval import (
    DEFAULT_EVAL_CONFIG_STAGE,
    EvalBaselineRecord,
    EvalDefaults,
    EvalGateVerdict,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import Manifest, State
from snowflake_semantic_tools.domain.state.lock import LockClaim

# The prefix of an eval run's lock id, so a refused run can tell an eval holder from an apply.
EVAL_RUN_PREFIX = "eval-"
_DEFAULT_LOCK_POLICY = LockPolicy()


class EvalGatePort(CatalogPublicationPort, StatePort, Protocol):
    """The Snowflake roles the eval gate uses: running the suite, and reading recorded state."""


@dataclass(frozen=True, slots=True)
class EvalGateRequest:
    """How an eval suite run goes: whether it stops early, and whether it captures baselines.

    Attributes:
        reason: Why the baselines change; required to capture, and recorded with each one.
    """

    fail_fast: bool = False
    capture_baseline: bool = False
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class EvalGateOutcome:
    """What an eval suite run found, for the CLI to report.

    Attributes:
        diagnostics: What reading state and the publication check reported, then the suite's,
            then each gate's, in eval order.
        suite: The suite result; None when the publication check refused to start any eval.
        passed: Whether every eval was accepted and nothing reported an error.
        data: The run as JSON: the suite's projection, and the gate's verdict and regressions.
    """

    diagnostics: DiagnosticBag
    suite: EvalSuiteResult | None
    passed: bool
    data: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class EvalGateRefused:
    """The run did not start: another operation holds the target's lock.

    Attributes:
        diagnostics: What taking the lease reported, then each view an apply holding it may
            be regenerating under an eval's agent.
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
    what the evals run against. The lease is released however the run ends.
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
    ) -> None:
        self._port = port
        self._inputs = inputs
        self._state_store = state_store
        self._eval_store = eval_store
        self._clock = clock
        self._actor = actor
        self._host = host
        self._lock_policy = lock_policy

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
        3. Read authoritative state, and check every eval is published as `manifest` renders it.
        4. Unless that check reported an error, run the suite, then capture each eval's
           baseline or evaluate and record its gate.
        5. Release the lease.

        Args:
            manifest: The manifest the evals were compiled into, which `sst compile` wrote.
            target: The live target; baselines and gates are stored under its name.

        Raises:
            SnowflakePortError: the remote lock could not be read or claimed.

        Diagnostics:
            SST-APL011: another run holds the target's lock, so no eval starts.
            SST-VAL755: the run holding the lock is not an eval run, so it may be regenerating
                a semantic view an eval's agent uses; one per eval and view.
            Those of `read_state`, `validate_eval_publication`, the suite, and each gate.
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
        locked, lock_diagnostics = lease.acquire(break_stale=False)
        if not locked:
            holder = lease.holder
            overlaps = () if holder is not None and holder.startswith(EVAL_RUN_PREFIX) else view_overlaps(evals)
            return EvalGateRefused(
                f"cannot run evals while {holder or 'another operation'} holds the target lock",
                DiagnosticBag((*lock_diagnostics, *overlaps)),
            )
        try:
            state, state_diagnostics = read_state(self._state_store, self._port, state_table=state_table, target=target)
            publication = validate_eval_publication(evals, manifest, state, handler)
            preflight = DiagnosticBag((*state_diagnostics, *publication))
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
        """Run the suite, trusting each staged config whose digest state recorded, then gate it."""
        config_digests = {
            item.artifact_key: digest
            for item in evals
            if (entry := state.applied.get(item.artifact_key)) is not None
            if (digest := dict(entry.component_fingerprints).get("config_stage_md5")) is not None
        }
        suite = RunEvalSuite(self._port, self._clock, lifecycle_config).run(
            evals,
            defaults=defaults,
            options=EvalRunOptions(self._inputs.git_sha()),
            fail_fast=request.fail_fast,
            config_digests=config_digests,
            baseline_capture=request.capture_baseline,
        )
        verdicts, captured, gate_diagnostics = self._gate(evals, suite, defaults, request, target_name)
        diagnostics = DiagnosticBag((*preflight, *suite.diagnostics, *gate_diagnostics))
        # A report-tier regression adds no diagnostic: it reaches the caller through the
        # verdict in `data` and the gate state recorded for it. A blocking one is SST-VAL763,
        # an error, so the run fails.
        passed = suite.success and not diagnostics.has_errors
        return EvalGateOutcome(diagnostics, suite, passed, _gate_data(suite, verdicts, captured, request))

    def _gate(
        self,
        evals: tuple[CompiledEval, ...],
        suite: EvalSuiteResult,
        defaults: EvalDefaults,
        request: EvalGateRequest,
        target_name: str,
    ) -> tuple[list[EvalGateVerdict], list[EvalBaselineRecord], list[Diagnostic]]:
        """Capture each eval's baseline, or evaluate its gate and record the verdict, in eval order.

        Captured baselines are written together once every eval has one, so a capture that
        fails part way writes none.
        """
        verdicts: list[EvalGateVerdict] = []
        captured: list[EvalBaselineRecord] = []
        diagnostics: list[Diagnostic] = []
        for item, item_result in zip(evals, suite.evals, strict=False):
            if request.capture_baseline:
                run = item.resolved.config.run
                captured.append(
                    capture_baseline(
                        item,
                        item_result,
                        reason=request.reason or "",
                        captured_at=self._clock.now_iso(),
                        default_tier=defaults.eval_tier,
                        required_attempts=(
                            run.baseline_runs
                            if run is not None and run.baseline_runs is not None
                            else defaults.baseline_runs
                        ),
                    )
                )
                diagnostics.extend(recorded_judges(item))
                continue
            stored = self._eval_store.read_baseline(target_name, item.artifact_key)
            verdict, item_diagnostics = evaluate_gate(
                item,
                item_result,
                stored,
                now=self._clock.now_iso(),
                default_tier=defaults.eval_tier,
                default_baseline_runs=defaults.baseline_runs,
            )
            diagnostics.extend(item_diagnostics)
            persist_gate(self._eval_store, target_name, item, item_result, verdict, evaluated_at=self._clock.now_iso())
            verdicts.append(verdict)
        if request.capture_baseline:
            self._eval_store.write_baselines(target_name, tuple(captured))
        return verdicts, captured, diagnostics


def view_overlaps(evals: tuple[CompiledEval, ...]) -> tuple[Diagnostic, ...]:
    """Warn, per eval, of each semantic view its agent's tools query, which a running apply may regenerate.

    Snowflake does not coordinate an eval run with a regenerate of a view its agent reads, so
    the scores would grade a definition that changed under them.

    Diagnostics:
        SST-VAL755: the eval's agent uses the view; once per view, in tool order.
    """
    return tuple(
        D("SST-VAL755", subject=item.artifact_key, artifact=item.name, value=view)
        for item in evals
        for view in dict.fromkeys(
            tool.semantic_view for tool in item.resolved.agent.tools if tool.semantic_view is not None
        )
    )


def _gate_data(
    suite: EvalSuiteResult,
    verdicts: list[EvalGateVerdict],
    captured: list[EvalBaselineRecord],
    request: EvalGateRequest,
) -> dict[str, object]:
    """Project a run for the CLI: the suite's projection, with the gate's verdict filled in.

    The verdict is `captured` for a capture; otherwise `no_signal` when some gate could not
    tell, else `regressed` when some eval regressed, else `passed`.
    """
    if request.capture_baseline:
        verdict = "captured"
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
