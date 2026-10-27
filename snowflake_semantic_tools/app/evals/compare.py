"""Compare orchestration models on the evals of published agents and CoWork profiles.

`RunEvalComparison` is `sst eval --compare-models`'s use case. Under the target's
run lease -- the local lock and the remote one `sst apply` and the eval gate take -- it:

1. reads authoritative state and refuses to start unless every eval is published as the manifest
   renders it, the run role holds what a run needs, and each eval's subject is published exactly
   as compiled, served from its release (`published_release`);
2. commits each subject's candidate versions and restores it (`CompareLifecycle`), subject by
   subject, before any run starts, so a subject is without a live version only while its own
   versions commit;
3. runs the pinned model on the release and each committed candidate on its `EVAL_` alias, every
   run of every eval at once (`run_comparison`).

Then, with the lease released, it reads each model's cost (`read_costs`) and builds the report.
A comparison never moves a default or a released alias, gates nothing, writes no baseline or eval
state, and never fails on scores: only a refusal, or a version it could not commit or restore,
fails it.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare_cost import CostQuery, read_costs
from snowflake_semantic_tools.app.evals.compare_lifecycle import (
    CompareLifecycle,
    CompareLifecyclePort,
    CompareSubject,
    LifecycleResult,
)
from snowflake_semantic_tools.app.evals.compare_precheck import published_release
from snowflake_semantic_tools.app.evals.compare_report import ComparedEval, comparison_report
from snowflake_semantic_tools.app.evals.compare_runs import (
    ComparedModel,
    ComparisonRuns,
    EvalRequest,
    PlannedEval,
    labeled,
    run_comparison,
)
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.privileges import EvalRolePort, eval_role_diagnostics
from snowflake_semantic_tools.app.evals.progress import EvalProgress, no_progress
from snowflake_semantic_tools.app.evals.run import RunEvalSuite, validate_eval_publication
from snowflake_semantic_tools.app.evals.suite import EVAL_RUN_PREFIX, EvalGateRefused
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, PINNED
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport
from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import BaseModelsPort, CortexBaseModel
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.snowflake.usage import UsagePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import Manifest, State
from snowflake_semantic_tools.domain.state.lock import LockClaim
from snowflake_semantic_tools.domain.validate.eval_config import candidate_model_problems

# The runs of each model a comparison makes when neither the command line nor a setting says.
DEFAULT_COMPARE_RUNS = 2
_DEFAULT_LOCK_POLICY = LockPolicy()
_NOT_RUN_ID = re.compile(r"[^0-9a-z]")


class ComparePort(
    CompareLifecyclePort, CatalogPublicationPort, StatePort, EvalRolePort, UsagePort, BaseModelsPort, Protocol
):
    """The Snowflake roles a comparison uses: versions, runs, state, the run role, usage, and models."""


@dataclass(frozen=True, slots=True)
class CompareOutcome:
    """What a comparison found, for the CLI to report.

    Attributes:
        diagnostics: What reading state and the prechecks reported, then each subject's
            lifecycle, the runs, and the report's notices, in eval order.
        report: The report; None when a precheck refused before anything was committed.
        passed: Whether nothing reported an error: a refusal, or a version not committed or restored.
    """

    diagnostics: DiagnosticBag
    report: CompareReport | None
    passed: bool


def requested_models(
    evals: Sequence[CompiledEval],
    *,
    given: tuple[str, ...] | None,
    runs: int | None,
    allowed: frozenset[str],
) -> Mapping[str, EvalRequest] | Diagnostic:
    """Resolve what each eval compares: `--compare-models`' list, else its `candidate_models` setting.

    The runs are `--compare-runs`, else the eval's `compare_runs` setting, else two. A list given
    on the command line is checked as a setting is; a configured one was checked when the eval
    compiled.

    Args:
        allowed: `snowflake.orchestration_models`, casefolded; empty when it is not set.

    Returns:
        Each eval's request by eval key; else the first reason the command line is refused.

    Diagnostics:
        SST-VAL770, SST-VAL771: a model the command line gives is `auto`, or not allowed.
        SST-VAL772: an eval has no candidate to compare.
    """
    resolved: dict[str, EvalRequest] = {}
    for item in evals:
        options = item.resolved.options
        candidates = given if given is not None else options.candidate_models
        if given is not None:
            problems = candidate_model_problems(
                given, allowed, artifact=item.name, field="--compare-models", subject=item.artifact_key
            )
            if problems:
                return problems[0]
        if not candidates:
            return D("SST-VAL772", artifact=item.name, subject=item.artifact_key)
        resolved[item.artifact_key] = EvalRequest(candidates, runs or options.compare_runs or DEFAULT_COMPARE_RUNS)
    return resolved


def comparison_run_id(value: str) -> str:
    """Shorten a clock's run id to the token a comparison's names carry: eight letters or digits."""
    return _NOT_RUN_ID.sub("", value.lower())[:8] or "run"


@dataclass(frozen=True, slots=True)
class _Run:
    """What names one comparison: its run id, when it started, its target, and where its report goes."""

    run_id: str
    started_at: str
    target: str
    report_path: str


class RunEvalComparison:
    """Compare models on each eval's published subject, as the module docstring says.

    Runs that go on at once each lease a session from `sessions`, opened from `port`; without it
    they run one at a time. Each run's progress goes to `progress`, labeled by eval and model.
    """

    def __init__(
        self,
        port: ComparePort,
        inputs: ProjectInputs,
        state_store: StateStore,
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
        self._clock = clock
        self._actor = actor
        self._host = host
        self._lock_policy = lock_policy
        self._sessions = sessions
        self._progress = progress

    def run(
        self,
        evals: tuple[CompiledEval, ...],
        subjects: Mapping[str, CompareSubject],
        requests: Mapping[str, EvalRequest],
        manifest: Manifest,
        *,
        target: TargetIdentity,
        state_table: QualifiedName,
        report_path: str,
        break_stale_lock: bool = False,
    ) -> CompareOutcome | EvalGateRefused:
        """Run the comparison under the target's run lease, then read its cost and build its report.

        Args:
            subjects: Each eval's subject as plan publishes it, by eval key.
            requests: Each eval's candidates and runs, by eval key, as `requested_models` resolves them.
            report_path: Where the report will be written, which a pending cost's notice names.

        Raises:
            SnowflakePortError: the remote lock could not be read or claimed.
            EvalRunsInterrupted: the comparison was interrupted after it started a run; every
                subject was already restored.

        Diagnostics:
            SST-APL011, SST-APL010: as taking the run lease reports them.
            SST-VAL727, SST-VAL728: as `eval_role_diagnostics` reports them.
            SST-APL108: a subject is not published exactly as compiled; nothing is committed.
            Those of `read_state`, `validate_eval_publication`, `CompareLifecycle.commit`, the
                runs, and `comparison_report`.
        """
        run_id = comparison_run_id(self._clock.new_run_id())
        started_at = self._clock.now_iso()
        lease = RunLease(
            self._state_store,
            self._port,
            state_table,
            target.name,
            LockClaim(f"{EVAL_RUN_PREFIX}{run_id}", self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, lock_diagnostics = lease.acquire(break_stale=break_stale_lock)
        if not locked:
            holder = lease.holder or "another operation"
            reason = f"cannot compare models while {holder} holds the target lock"
            return EvalGateRefused(reason, DiagnosticBag(lock_diagnostics))
        try:
            state, state_diagnostics = read_state(self._state_store, self._port, state_table=state_table, target=target)
            releases, prechecked = self._precheck(evals, subjects, manifest, state)
            preflight = DiagnosticBag((*lock_diagnostics, *state_diagnostics, *prechecked))
            if preflight.has_errors:
                return CompareOutcome(preflight, None, False)
            compared = tuple(
                ComparedEval(
                    item, subjects[item.artifact_key], releases[item.artifact_key], requests[item.artifact_key], _
                )
                for item, _ in self._commit(evals, subjects, requests, run_id)
            )
            ran = self._runs(compared, run_id)
        finally:
            lease.release()
        return self._reported(compared, ran, preflight, _Run(run_id, started_at, target.name, report_path))

    def _precheck(
        self, evals: Sequence[CompiledEval], subjects: Mapping[str, CompareSubject], manifest: Manifest, state: State
    ) -> tuple[dict[str, str], tuple[Diagnostic, ...]]:
        """Check every eval and subject may be compared, before anything is committed; return each release.

        Diagnostics:
            Those of `validate_eval_publication`, `eval_role_diagnostics` and `published_release`.
        """
        evals = tuple(evals)
        handler = EvalLifecycleHandler(self._port, self._lifecycle_config())
        found = [*validate_eval_publication(evals, manifest, state, handler), *eval_role_diagnostics(self._port, evals)]
        releases: dict[str, str] = {}
        for item in evals:
            subject = subjects[item.artifact_key]
            release = published_release(
                self._port, subject, state.applied.get(subject.key), fetched_at=self._clock.now_iso()
            )
            if isinstance(release, Diagnostic):
                found.append(release)
            else:
                releases[item.artifact_key] = release
        return releases, tuple(found)

    def _commit(
        self,
        evals: Sequence[CompiledEval],
        subjects: Mapping[str, CompareSubject],
        requests: Mapping[str, EvalRequest],
        run_id: str,
    ) -> list[tuple[CompiledEval, LifecycleResult]]:
        """Commit each subject's candidates and restore it, subject by subject, before any run starts."""
        lifecycle = CompareLifecycle(
            self._port, run_id=run_id, git_sha=self._inputs.git_sha(), dirty=bool(self._inputs.git_dirty())
        )
        return [
            (
                item,
                lifecycle.commit(
                    subjects[item.artifact_key], _candidates(subjects[item.artifact_key], requests[item.artifact_key])
                ),
            )
            for item in evals
        ]

    def _runs(self, compared: Sequence[ComparedEval], run_id: str) -> ComparisonRuns:
        """Run the pinned model on each release and each committed candidate on its alias, all at once."""
        plans = [
            PlannedEval(
                item.compiled,
                (
                    ComparedModel(item.subject.pinned_model, PINNED, item.release),
                    *(
                        ComparedModel(each.model, CANDIDATE, each.version, each.alias)
                        for each in item.lifecycle.candidates
                    ),
                ),
                item.request.runs,
            )
            for item in compared
        ]
        progress = labeled(self._progress, plans, run_id=run_id)
        suite = RunEvalSuite(self._port, self._clock, self._lifecycle_config(), self._sessions, progress)
        defaults = self._inputs.eval_catalog().defaults
        return run_comparison(
            suite, plans, run_id=run_id, defaults=defaults, options=EvalRunOptions(self._inputs.git_sha())
        )

    def _reported(
        self, compared: Sequence[ComparedEval], ran: ComparisonRuns, preflight: DiagnosticBag, run: _Run
    ) -> CompareOutcome:
        """Read every model's cost and the models' lifecycles, then build the report and its notices."""
        queries = tuple(
            CostQuery(item.compiled.artifact_key, model, entry.request_ids, entry.run_names)
            for item in compared
            for model, entry in item.entries(ran)
        )
        now = self._clock.now_iso()
        costs = read_costs(self._port, queries, started_at=run.started_at, now=now)
        report, notices = comparison_report(
            compared,
            ran,
            costs,
            self._listed_models(),
            run_id=run.run_id,
            generated_at=now,
            target=run.target,
            git_sha=self._inputs.git_sha(),
            report_path=run.report_path,
        )
        lifecycle = tuple(diagnostic for item in compared for diagnostic in item.lifecycle.diagnostics)
        diagnostics = DiagnosticBag((*preflight, *lifecycle, *ran.suite.diagnostics, *notices))
        return CompareOutcome(diagnostics, report, not any(item.blocks for item in (*preflight, *lifecycle)))

    def _listed_models(self) -> Mapping[str, CortexBaseModel] | None:
        """Read the account's Cortex base models by casefolded name; None when SHOW is refused."""
        try:
            return {row.name.casefold(): row for row in self._port.base_models()}
        except SnowflakePortError:
            return None

    def _lifecycle_config(self) -> EvalLifecycleConfig:
        stage = config_block(config_block(self._inputs.config().tree.get("apply")).get("eval_config_stage"))
        return EvalLifecycleConfig(str(stage.get("stage") or DEFAULT_EVAL_CONFIG_STAGE))


def _candidates(subject: CompareSubject, request: EvalRequest) -> tuple[str, ...]:
    """The candidates to commit: every one asked but the pinned model, which runs once, as pinned."""
    return tuple(model for model in request.candidates if model.casefold() != subject.pinned_model.casefold())
