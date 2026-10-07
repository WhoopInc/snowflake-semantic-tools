"""Start, poll, retrieve, and normalize Cortex Agent evaluation runs.

`RunEvalSuite` runs each compiled eval as a series of attempts: stage the eval's config, start
a run, poll it to a terminal status (`poll` rides out reads that fail in transit), and read its
results (`retrieve` reads and checks what Snowflake reports). While runs are in flight, the
suite reports their progress as `progress` events. The suite result carries every attempt;
`eval_suite_json` projects it for the CLI, without judging it -- the gate compares it with a
baseline.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from hashlib import md5
from threading import Lock

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.poll import StatusPoll
from snowflake_semantic_tools.app.evals.progress import EvalProgress, RunWatch, no_progress
from snowflake_semantic_tools.app.evals.retrieve import _read_results, _sum_costs
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.lifecycle.evals import (
    EvalLifecycleConfig,
    EvalLifecycleHandler,
    eval_stage_format_matches,
)
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_PASS_STATUSES,
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    EvalRunConfig,
)
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.project import UNKNOWN_GIT_SHA
from snowflake_semantic_tools.domain.ports.snowflake.errors import AgentVersionNotFound, SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.resolve.eval_name import render_eval_name_template
from snowflake_semantic_tools.domain.sql import Sql, literal, sql
from snowflake_semantic_tools.domain.state import APPLIED, Manifest, State

_PARTIAL_STATUSES = frozenset(("INVOCATION_PARTIALLY_COMPLETED", "PARTIALLY_COMPLETED"))
_DEFAULT_RUN_NAME_TEMPLATE = "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}"


@dataclass(frozen=True, slots=True)
class EvalRunResult:
    """Every attempt of one eval, in order, with what they reported.

    Attributes:
        accepted: Whether some attempt completed and its results were read.
        retention: The run's retention class, `audit` or `decision`, from its variant and
            `run.retention`, else `evals.+retention`; None when neither classifies it. SST never
            deletes a run, so this is reported for whoever reaps decision runs.
        decision_window_days: How long a decision run is kept; None for any other class.
    """

    eval_key: str
    attempts: tuple[EvalRunAttempt, ...]
    diagnostics: DiagnosticBag
    accepted: bool = False
    retention: str | None = None
    decision_window_days: int | None = None

    @property
    def success(self) -> bool:
        """Report whether the eval was accepted."""
        return self.accepted


@dataclass(frozen=True, slots=True)
class EvalSuiteResult:
    """Every eval a suite ran, in suite order, with all of their diagnostics."""

    evals: tuple[EvalRunResult, ...]
    diagnostics: DiagnosticBag

    @property
    def success(self) -> bool:
        """Report whether the suite ran some eval, every eval was accepted, and nothing erred."""
        return bool(self.evals) and not self.diagnostics.has_errors and all(result.success for result in self.evals)


class EvalRunsInterrupted(KeyboardInterrupt):
    """The suite was interrupted after it asked Snowflake to start evaluation runs.

    A started run goes on in Snowflake whatever becomes of the suite, so whoever reports the
    interrupt names each one, for the user to check or cancel.

    Attributes:
        run_names: Every run the suite asked Snowflake to start, in name order.
    """

    def __init__(self, run_names: tuple[str, ...]) -> None:
        super().__init__(", ".join(run_names))
        self.run_names = run_names


class _StartedRuns:
    """The runs a suite has asked Snowflake to start, as every worker records them."""

    def __init__(self) -> None:
        self._names: set[str] = set()
        self._lock = Lock()

    def add(self, run_name: str) -> None:
        with self._lock:
            self._names.add(run_name)

    def discard(self, run_name: str) -> None:
        with self._lock:
            self._names.discard(run_name)

    @property
    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._names))


@dataclass(frozen=True, slots=True)
class _RunSetup:
    """What every attempt of one eval shares, resolved before the first one starts.

    Attributes:
        timestamp: The run names' `ts` token.
        agent_version: The concrete version the agent's selector resolved to.
        required_completed: The completed attempts the eval needs: a baseline capture's
            `baseline_runs`, else 1. No attempt starts once that many completed and were read.
        attempt_limit: The attempts allowed in all: `required_completed` plus the retries.
        config_path: Where the eval's config is staged.
    """

    run: EvalRunConfig
    timestamp: str
    agent_version: str
    required_completed: int
    attempt_limit: int
    config_path: str


@dataclass(frozen=True, slots=True)
class _SuiteRun:
    """What one suite run shares across its evals and their attempts.

    Attributes:
        fanout: Where every round's setups and attempts run, as many at once as the suite's
            concurrency allows.
        started: The runs started so far, which an interrupt reports.
        watch: The runs in flight, which every attempt reports its progress to.
    """

    defaults: EvalDefaults
    options: EvalRunOptions
    config_digests: Mapping[str, str]
    baseline_capture: bool
    fanout: Fanout[CatalogPublicationPort]
    started: _StartedRuns
    watch: RunWatch


@dataclass(slots=True)
class _Progress:
    """One eval's attempts so far, in attempt order, each with the diagnostics its outcome reported."""

    compiled: CompiledEval
    setup: _RunSetup | Diagnostic
    outcomes: list[tuple[EvalRunAttempt, tuple[Diagnostic, ...]]] = field(default_factory=list)
    completed: int = 0
    cancelled: bool = False

    def next_steps(self) -> tuple[tuple[_Progress, _RunSetup, int], ...]:
        """Return the attempts the next round starts, by number: one per completed attempt still needed.

        Never past the attempt limit, and none once an attempt was cancelled or the eval could
        not be set up. An eval needing one completed attempt so starts another only after one
        that did not pass: it failed, was partial, or its status or results could not be read.
        """
        setup = self.setup
        if isinstance(setup, Diagnostic) or self.cancelled:
            return ()
        started = len(self.outcomes)
        wanted = min(setup.required_completed - self.completed, setup.attempt_limit - started)
        return tuple((self, setup, number) for number in range(started + 1, started + 1 + max(0, wanted)))

    def record(self, attempt: EvalRunAttempt, diagnostics: tuple[Diagnostic, ...]) -> None:
        self.outcomes.append((attempt, diagnostics))
        if attempt.terminal_status == "CANCELLED":
            self.cancelled = True
        elif _retrieved(attempt):
            self.completed += 1

    def result(self) -> EvalRunResult:
        """Report every attempt that ran; any one completed and read accepts the eval.

        Once the eval has every completed attempt it needs, each attempt that did not pass was
        retried in its place, so it reports SST-APL029, a warning naming its run, status and
        details, rather than the error its outcome is on its own. Short of that, every attempt
        reports its outcome as it ended. A partial attempt the config accepts reports SST-VAL730
        either way: the config, not the run, is what is wrong.
        """
        setup = self.setup
        if isinstance(setup, Diagnostic):
            return EvalRunResult(self.compiled.artifact_key, (), DiagnosticBag((setup,)))
        retried = self.completed >= setup.required_completed
        diagnostics = tuple(
            diagnostic
            for attempt, outcome in self.outcomes
            for diagnostic in (
                *_accepted_partial(self.compiled, setup.run, attempt.terminal_status),
                *((_retried_attempt(self.compiled, attempt),) if retried and not _retrieved(attempt) else outcome),
            )
        )
        return EvalRunResult(
            self.compiled.artifact_key,
            tuple(attempt for attempt, _ in self.outcomes),
            DiagnosticBag(diagnostics),
            self.completed > 0,
        )


class RunEvalSuite:
    """Run a suite of compiled evals against their published agents.

    Only reads and runs evaluations; it publishes nothing but each eval's config file, which it
    stages when the staged copy is missing or cannot be trusted.

    Args:
        sessions: Where setups and attempts running at once lease a session each, opened from
            `port`, so each attempt starts and polls its run on a session of its own; None runs
            everything on `port`, one at a time, so no two share a session.
        progress: Where each attempt's start, status changes, failed status reads and end go,
            and the heartbeat naming the runs still in flight; by default nowhere.
    """

    def __init__(
        self,
        port: CatalogPublicationPort,
        clock: ClockPort,
        lifecycle_config: EvalLifecycleConfig = EvalLifecycleConfig(),
        sessions: SessionPool[CatalogPublicationPort] | None = None,
        progress: EvalProgress = no_progress,
    ) -> None:
        self._port = port
        self._clock = clock
        self._lifecycle_config = lifecycle_config
        self._sessions = sessions
        self._progress = progress

    def run(
        self,
        compiled: Sequence[CompiledEval],
        *,
        defaults: EvalDefaults = EvalDefaults(),
        options: EvalRunOptions,
        fail_fast: bool = False,
        config_digests: Mapping[str, str] | None = None,
        baseline_capture: bool = False,
        threads: int = 1,
    ) -> EvalSuiteResult:
        """Run every eval and collect their results, each eval's attempts in attempt order, in suite order.

        Every eval is set up, then its attempts run in rounds: each round starts, for every eval,
        one attempt per completed attempt it still needs, within its attempt limit (see
        `_RunSetup`), so a gate run retries only an attempt that did not pass, and a baseline
        capture starts all of its `baseline_runs` in its first round. A round's attempts, across
        every eval, run on sessions of their own, as many at once as `suite_concurrency` allows,
        so no more START calls are in flight than it says; without a session pool, one at a
        time. With `fail_fast` the evals run one after another, and the suite stops at the first
        one not accepted.

        Args:
            config_digests: The MD5 digest of each eval's staged config that state trusts, by
                eval key; a staged config without one is staged again.
            baseline_capture: Run each eval until it has the completed attempts its baseline
                needs, rather than until one attempt passes.
            threads: The attempts run at once when neither the project nor any eval says.

        Raises:
            EvalRunsInterrupted: the suite was interrupted after it started a run. Every session
                the pool lent is halted first, so no worker starts or polls another; interrupted
                before any run started, the `KeyboardInterrupt` itself propagates.
        """
        context = _SuiteRun(
            defaults,
            options,
            config_digests or {},
            baseline_capture,
            Fanout(self._port, self._sessions, suite_concurrency(compiled, defaults, threads)),
            _StartedRuns(),
            RunWatch(self._progress, self._clock, options.heartbeat_ms),
        )
        try:
            if fail_fast:
                results = _in_order(compiled, lambda item: self._run_rounds((item,), context)[0])
            else:
                results = self._run_rounds(compiled, context)
        except KeyboardInterrupt as exc:
            if self._sessions is not None:
                self._sessions.halt("the eval run was interrupted")
            if context.started.names:
                raise EvalRunsInterrupted(context.started.names) from exc
            raise
        diagnostics = [diagnostic for result in results for diagnostic in result.diagnostics]
        return EvalSuiteResult(tuple(results), DiagnosticBag(tuple(diagnostics)))

    def _on(self, port: CatalogPublicationPort) -> RunEvalSuite:
        """Return this suite, or one like it running on `port`, a session leased for one item."""
        return self if port is self._port else RunEvalSuite(port, self._clock, self._lifecycle_config)

    def _run_rounds(self, compiled: Sequence[CompiledEval], context: _SuiteRun) -> list[EvalRunResult]:
        """Set up each eval, then run rounds of attempts until no eval needs another; report each eval.

        An eval that cannot be set up runs no attempt and reports why.
        """

        def set_up(port: CatalogPublicationPort, item: CompiledEval) -> _RunSetup | Diagnostic:
            return self._on(port)._setup(item, context)

        def attempt(
            port: CatalogPublicationPort, step: tuple[_Progress, _RunSetup, int]
        ) -> tuple[EvalRunAttempt, tuple[Diagnostic, ...]]:
            progress, setup, number = step
            return self._on(port)._attempt(progress.compiled, context, setup, number)

        setups = context.fanout.map(set_up, compiled)
        evals = [_Progress(item, setup) for item, setup in zip(compiled, setups, strict=True)]
        while batch := [step for progress in evals for step in progress.next_steps()]:
            for (progress, _, _), outcome in zip(batch, context.fanout.map(attempt, batch), strict=True):
                progress.record(*outcome)
        return [_with_retention(progress.result(), progress.compiled, context.defaults) for progress in evals]

    def _setup(self, compiled: CompiledEval, context: _SuiteRun) -> _RunSetup | Diagnostic:
        """Resolve what every attempt shares and stage the config; a diagnostic when the eval cannot run.

        A baseline capture needs its `baseline_runs` completed attempts, else the project's, else
        one, and allows that many plus the retries; any other run needs one, and allows it plus
        the retries.

        Diagnostics:
            SST-APL023: the eval has no run configuration, its agent version cannot be read, or
                its config cannot be staged.
            SST-VAL720: the agent version the config pins does not exist, or was dropped.
            SST-VAL729: the config stage declares another file format than evals read.
        """
        run = compiled.resolved.config.run
        if run is None:
            return D("SST-APL023", artifact=compiled.artifact_key, detail="run configuration is absent")
        timestamp = context.options.timestamp or _compact_timestamp()
        retry_count = run.retry if run.retry is not None else context.defaults.retry or 0
        selector = compiled.resolved.config.agent_version or ""
        try:
            agent_version = self._port.resolve_agent_version(compiled.agent_target, selector)
        except AgentVersionNotFound:
            return D("SST-VAL720", artifact=compiled.name, value=selector, subject=compiled.artifact_key)
        except SnowflakePortError as exc:
            return D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
        stage_problem = self._stage_format_problem(compiled)
        if stage_problem is not None:
            return stage_problem
        baseline_runs = run.baseline_runs or context.defaults.baseline_runs or 1
        required_completed = baseline_runs if context.baseline_capture else 1
        config_path = EvalLifecycleHandler(self._port, self._lifecycle_config).config_path(compiled.rendered_artifact)
        try:
            self._ensure_config(
                config_path,
                compiled.rendered.config_yaml.encode("utf-8"),
                context.config_digests.get(compiled.artifact_key),
            )
        except SnowflakePortError as exc:
            return D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
        return _RunSetup(
            run, timestamp, agent_version, required_completed, required_completed + retry_count, config_path
        )

    def _attempt(
        self,
        compiled: CompiledEval,
        context: _SuiteRun,
        setup: _RunSetup,
        attempt_number: int,
    ) -> tuple[EvalRunAttempt, tuple[Diagnostic, ...]]:
        """Start one attempt, poll it to a terminal status, and read its results once it completed.

        A failure at any step ends the attempt with it: the attempt records the terminal status
        it reached and why it failed. Its diagnostics describe the attempt on its own; the
        eval's result reports them only when no retry completed in its place. The watch hears
        the attempt start and end, with the status it ended in.

        Diagnostics:
            SST-APL023: the run could not be started, its status could not be read (a read
                failed for good, or `max_read_failures` in a row failed in transit), it did not
                end by the deadline, or it ended in a status that is neither a pass nor partial,
                such as `FAILED` or one Snowflake does not document; the message carries its
                status details.
            SST-APL024: the run ended partially completed, in a status the config does not
                accept.
            SST-SNO001: the run completed but its results could not be read or did not match.
        """
        run_name = _run_name(compiled, setup, context.options, attempt_number)
        context.watch.started(run_name, attempt_number, setup.attempt_limit)
        refused = self._start(compiled, run_name, setup.config_path, context.started)
        if refused is not None:
            context.watch.ended(run_name, "START_FAILED")
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=refused)
            return EvalRunAttempt(run_name, attempt_number, "START_FAILED", retrieval_error=refused), (diagnostic,)
        poll = StatusPoll(self._port, self._clock, context.options, context.watch)
        try:
            terminal_status, status_details = poll.until_terminal(compiled, run_name, setup.config_path)
        except (SnowflakePortError, ValueError) as exc:
            context.watch.ended(run_name, "STATUS_FAILED")
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
            return EvalRunAttempt(run_name, attempt_number, "STATUS_FAILED", retrieval_error=str(exc)), (diagnostic,)
        context.watch.ended(run_name, terminal_status)
        diagnostics = _partial_status(compiled, setup.run, terminal_status) or _failed_status(
            compiled, run_name, terminal_status, status_details
        )
        if terminal_status not in EVAL_PASS_STATUSES:
            return EvalRunAttempt(run_name, attempt_number, terminal_status, status_details=status_details), diagnostics
        completed = EvalRunAttempt(
            run_name,
            attempt_number,
            terminal_status,
            status_details=status_details,
            agent_version=setup.agent_version,
        )
        try:
            rows, cost = _read_results(self._port, compiled, run_name)
        except (SnowflakePortError, ValueError) as exc:
            failure = D("SST-SNO001", detail=f"eval '{compiled.artifact_key}' retrieval failed: {exc}")
            return replace(completed, retrieval_error=str(exc)), (*diagnostics, failure)
        return replace(completed, rows=rows, cost=cost), diagnostics

    def _stage_format_problem(self, compiled: CompiledEval) -> Diagnostic | None:
        """SST-VAL729 when the existing config stage declares a file format EXECUTE_AI_EVALUATION cannot read."""
        stage = EvalLifecycleHandler(self._port, self._lifecycle_config).config_stage(compiled.rendered_artifact)
        if not self._port.object_exists("STAGE", stage):
            return None
        found = self._port.describe_stage_file_format(stage)
        if eval_stage_format_matches(found):
            return None
        return D("SST-VAL729", artifact=compiled.name, found=found or "absent", subject=compiled.artifact_key)

    def _ensure_config(self, config_path: str, content: bytes, trusted_digest: str | None) -> None:
        """Stage the rendered config unless a trusted, identical copy is staged, then verify the copy.

        The staged bytes are read back and compared, because a stage's listed size and digest
        need not describe its bytes. A copy is trusted only when state recorded its digest.

        Raises:
            SnowflakePortError: the staged copy is absent or unreadable, or its length, digest
                or bytes differ from the rendered config.
        """
        observed, staged_content, staged_digest = self._staged_config(config_path)
        if observed is None or staged_content != content or trusted_digest is None or staged_digest != trusted_digest:
            self._port.upload(config_path, content)
            observed, staged_content, staged_digest = self._staged_config(config_path)
        if observed is None:
            raise SnowflakePortError(f"staged evaluation config {config_path} is absent")
        if staged_content is None:
            raise SnowflakePortError(f"staged evaluation config {config_path} is unreadable")
        if len(staged_content) != len(content):
            raise SnowflakePortError(
                f"staged evaluation config {config_path} has {len(staged_content)} bytes, expected {len(content)}"
            )
        if trusted_digest is not None and staged_digest != trusted_digest:
            raise SnowflakePortError(
                f"staged evaluation config {config_path} has digest {staged_digest}, expected {trusted_digest}"
            )
        if staged_content != content:
            raise SnowflakePortError(f"staged evaluation config {config_path} bytes do not match rendered config")

    def _staged_config(self, config_path: str) -> tuple[StagedFileMetadata | None, bytes | None, str | None]:
        """Observe the staged config and read back its bytes and their MD5; None for what is absent."""
        observed = self._port.observe_staged_file(config_path)
        staged_content = self._port.read_staged_file(config_path) if observed is not None else None
        staged_digest = md5(staged_content, usedforsecurity=False).hexdigest() if staged_content is not None else None
        return observed, staged_content, staged_digest

    def _start(self, compiled: CompiledEval, run_name: str, config_path: str, started: _StartedRuns) -> str | None:
        """Start a run in the agent's schema; the error Snowflake gave when it refuses.

        The call resolves in the agent's schema on the port's scoped session, so it never
        changes the schema any other statement runs in. The run counts as started from before
        the call until Snowflake refuses it: an interrupt during the call may leave it running.
        """
        started.add(run_name)
        try:
            self._port.query_in_context(
                SchemaScope.from_qualified_name(compiled.agent_target),
                _evaluation_call("START", run_name, config_path),
            )
        except SnowflakePortError as exc:
            started.discard(run_name)
            return str(exc)
        return None


def validate_eval_publication(
    compiled: Sequence[CompiledEval],
    manifest: Manifest,
    state: State,
    handler: EvalLifecycleHandler,
) -> DiagnosticBag:
    """Check that every eval is published exactly as this manifest renders it, before running any.

    State must record the eval as applied from this manifest, with the rendered fingerprint and
    target, and its handler must plan no change against what is live now.

    Diagnostics:
        SST-APL012: an eval is not applied as this manifest renders it, or the live eval differs.
        Whatever the handler's plan reports is passed on, ahead of that eval's SST-APL012.
    """
    diagnostics: list[Diagnostic] = []
    for item in compiled:
        artifact = item.rendered_for_publish(manifest.manifest_id)
        entry = state.applied.get(item.artifact_key)
        if (
            entry is None
            or entry.fingerprint != artifact.fingerprint
            or entry.manifest_id != manifest.manifest_id
            or entry.qualified_name.casefold() != artifact.target.sql.casefold()
            or entry.outcome != APPLIED
        ):
            diagnostics.append(D("SST-APL012", artifact=item.artifact_key, value=artifact.target.sql))
            continue
        plan = handler.plan(artifact, entry, manifest)
        diagnostics.extend(plan.diagnostics)
        if plan.action is not Action.NOOP and not plan.diagnostics.has_errors:
            diagnostics.append(D("SST-APL012", artifact=item.artifact_key, value=artifact.target.sql))
    return DiagnosticBag(tuple(diagnostics))


def eval_suite_json(result: EvalSuiteResult) -> dict[str, object]:
    """Project a suite result for the CLI: each attempt's summary and cost, and the suite's totals.

    The regression count and gate verdict are placeholders the gate fills in when it runs.
    """
    attempts = tuple(attempt for eval_result in result.evals for attempt in eval_result.attempts)
    total_cost = _sum_costs(tuple(attempt.cost for attempt in attempts))
    return {
        "evals": [
            {
                "eval_key": eval_result.eval_key,
                "retention": {
                    "class": eval_result.retention,
                    "decision_window_days": eval_result.decision_window_days,
                },
                "attempts": [
                    {
                        "run_name": attempt.run_name,
                        "attempt": attempt.attempt,
                        "terminal_status": attempt.terminal_status,
                        "status_details": list(attempt.status_details),
                        "retrieval_error": attempt.retrieval_error,
                        "metric_summaries": _metric_summaries(attempt.rows),
                        "cost": _cost_json(attempt.cost),
                    }
                    for attempt in eval_result.attempts
                ],
            }
            for eval_result in result.evals
        ],
        "attempt_count": len(attempts),
        "cost_totals": _cost_json(total_cost),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def empty_eval_suite_json() -> dict[str, object]:
    """Project a suite that ran nothing, in the shape `eval_suite_json` gives a suite that ran."""
    return {
        "evals": [],
        "attempt_count": 0,
        "cost_totals": _cost_json(EvalCostSummary()),
        "regression_count": 0,
        "gate_verdict": "not_evaluated",
    }


def _in_order(
    compiled: Sequence[CompiledEval], run_one: Callable[[CompiledEval], EvalRunResult]
) -> list[EvalRunResult]:
    """Run the evals one at a time, and stop after the first one not accepted."""
    results = []
    for item in compiled:
        result = run_one(item)
        results.append(result)
        if not result.success:
            break
    return results


def _with_retention(result: EvalRunResult, compiled: CompiledEval, defaults: EvalDefaults) -> EvalRunResult:
    """Classify the eval's runs for retention, from its run configuration else the project's."""
    run = compiled.resolved.config.run
    retention = retention_class(run, defaults.retention) if run is not None else defaults.retention
    window = run.retention.decision_window_days if run is not None and retention == "decision" else None
    return replace(result, retention=retention, decision_window_days=window)


def suite_concurrency(compiled: Sequence[CompiledEval], defaults: EvalDefaults, threads: int = 1) -> int:
    """Return how many runs a suite starts and polls at once, across all of its evals.

    The project's `evals.+concurrency`, else the most any eval's `run.concurrency` asks; with
    neither, `threads`: `--threads` paces the runs no setting paces. The session pool the
    suite leases from holds this many sessions.
    """
    requested: list[int] = []
    for item in compiled:
        run = item.resolved.config.run
        if run is not None and run.concurrency is not None:
            requested.append(run.concurrency)
    if defaults.concurrency:
        return max(1, defaults.concurrency)
    return max(1, max(requested, default=threads))


def _run_name(compiled: CompiledEval, setup: _RunSetup, options: EvalRunOptions, attempt_number: int) -> str:
    """Name one attempt's run from the eval's template; a retry appends `_R<attempt>`.

    `sha7` is the commit's first seven characters. Outside a git work tree there is no commit,
    so it is the first seven of the eval's fingerprint: still derived from what the project
    declares, and never from where the project happens to sit on disk.
    """
    known = options.git_sha and options.git_sha != UNKNOWN_GIT_SHA
    base_name = render_eval_name_template(
        setup.run.name_template or _DEFAULT_RUN_NAME_TEMPLATE,
        agent=compiled.name,
        sha7=(options.git_sha if known else compiled.rendered_artifact.fingerprint)[:7],
        variant=setup.run.variant or "ci",
        ts=setup.timestamp,
    )
    return base_name if attempt_number == 1 else f"{base_name}_R{attempt_number}"


def retention_class(run: EvalRunConfig, default: str | None) -> str | None:
    """Classify a run by its variant: `audit` or `decision` as `run.retention` lists it, else `default`.

    The variant defaults to `ci`, as it does in the run name. A variant both lists name is
    rejected by validation; here the audit list, which keeps the run, wins.
    """
    variant = run.variant or "ci"
    if variant in run.retention.audit:
        return "audit"
    if variant in run.retention.decision:
        return "decision"
    return default


def _partial_status(compiled: CompiledEval, run: EvalRunConfig, terminal_status: str) -> tuple[Diagnostic, ...]:
    """Warn of a partial terminal status the config does not accept."""
    if terminal_status not in _PARTIAL_STATUSES or terminal_status in run.accept_statuses:
        return ()
    return (D("SST-APL024", artifact=compiled.artifact_key, found=terminal_status),)


def _accepted_partial(compiled: CompiledEval, run: EvalRunConfig, terminal_status: str) -> tuple[Diagnostic, ...]:
    """Report SST-VAL730, an error, for a partial terminal status the config accepts.

    A partial status is still not a pass, whatever the config accepts, and whether or not a
    retry completed in its place.
    """
    if terminal_status in _PARTIAL_STATUSES and terminal_status in run.accept_statuses:
        return (D("SST-VAL730", artifact=compiled.name, found=terminal_status, subject=compiled.artifact_key),)
    return ()


def _retried_attempt(compiled: CompiledEval, attempt: EvalRunAttempt) -> Diagnostic:
    """Warn with SST-APL029 of an attempt that did not pass and that a completed retry replaced.

    The message carries the run's name, the status it reached, and its status details, else
    why its status or results could not be read.
    """
    reasons = (*attempt.status_details, *((attempt.retrieval_error,) if attempt.retrieval_error else ()))
    return D(
        "SST-APL029",
        artifact=compiled.artifact_key,
        value=attempt.run_name,
        found=attempt.terminal_status,
        detail="; ".join(reasons) or "no status details",
    )


def _failed_status(
    compiled: CompiledEval, run_name: str, terminal_status: str, status_details: tuple[str, ...]
) -> tuple[Diagnostic, ...]:
    """Report a terminal status that is neither a pass nor partial, with the run's status details."""
    if terminal_status in EVAL_PASS_STATUSES or terminal_status in _PARTIAL_STATUSES:
        return ()
    reason = "; ".join(status_details) or "no status details"
    detail = f"run {run_name!r} ended {terminal_status}: {reason}"
    return (D("SST-APL023", artifact=compiled.artifact_key, detail=detail),)


def _retrieved(attempt: EvalRunAttempt) -> bool:
    """Report whether an attempt completed and its results were read, which accepts its eval."""
    return attempt.terminal_status in EVAL_PASS_STATUSES and attempt.retrieval_error is None


def _evaluation_call(job: str, run_name: str, config_path: str) -> Sql:
    return sql(
        "CALL EXECUTE_AI_EVALUATION({job}, OBJECT_CONSTRUCT('run_name', {run_name}), {config})",
        job=literal(job),
        run_name=literal(run_name),
        config=literal(config_path),
    )


def _compact_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _metric_summaries(rows: tuple[EvalResultRow, ...]) -> list[dict[str, object]]:
    """Summarize each metric across an attempt's questions, by name: records, passes, mean score.

    The mean counts only the questions with a score; None when no question has one.
    """
    values: dict[str, list[EvalMetricResult]] = {}
    for row in rows:
        for metric in row.metrics:
            values.setdefault(metric.metric_name, []).append(metric)
    return [
        {
            "metric_name": name,
            "record_count": len(metrics),
            "passed_count": sum(metric.passed for metric in metrics),
            "average_score": (
                sum(score for metric in metrics if (score := metric.score) is not None)
                / sum(metric.score is not None for metric in metrics)
                if any(metric.score is not None for metric in metrics)
                else None
            ),
        }
        for name, metrics in sorted(values.items())
    ]


def _cost_json(value: EvalCostSummary) -> dict[str, object]:
    return {
        "duration_ms": value.duration_ms,
        "prompt_tokens": value.prompt_tokens,
        "completion_tokens": value.completion_tokens,
        "total_tokens": value.total_tokens,
        "total_input_tokens": value.total_input_tokens,
        "total_output_tokens": value.total_output_tokens,
        "llm_call_count": value.llm_call_count,
        "credits": None,
        "credit_attribution": "not_requested",
    }
