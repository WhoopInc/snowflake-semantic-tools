"""Start, poll, retrieve, and normalize Cortex Agent evaluation runs.

`RunEvalSuite` runs each compiled eval as a series of attempts: stage the eval's config, start
a run, poll it to a terminal status, and read its results (`retrieve` reads and checks what
Snowflake reports). The suite result carries every attempt; `eval_suite_json` projects it for
the CLI, without judging it -- the gate compares it with a baseline.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import md5

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.retrieve import _read_results, _read_status, _sum_costs
from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleConfig, EvalLifecycleHandler
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EVAL_PASS_STATUSES,
    EVAL_TERMINAL_STATUSES,
    EvalCostSummary,
    EvalDefaults,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    EvalRunConfig,
    render_eval_name_template,
)
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.ports.snowflake import (
    ClockPort,
    SnowflakePort,
    SnowflakePortError,
    StagedFileMetadata,
)
from snowflake_semantic_tools.domain.sql import Sql, ident, literal, scope, sql
from snowflake_semantic_tools.domain.state import APPLIED, Manifest, State

_PARTIAL_STATUSES = frozenset(("INVOCATION_PARTIALLY_COMPLETED", "PARTIALLY_COMPLETED"))
_DEFAULT_RUN_NAME_TEMPLATE = "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}"


@dataclass(frozen=True, slots=True)
class EvalRunOptions:
    """How a suite names and polls its runs.

    Attributes:
        git_sha: The commit the run names carry, as its first seven characters.
        timestamp: The run names' `ts` token; None uses the current UTC time.
        poll_interval_ms: The wait between two status reads of a run, in milliseconds.
        max_polls: The status reads of a run before it counts as not terminating.
    """

    git_sha: str
    timestamp: str | None = None
    poll_interval_ms: int = 5_000
    max_polls: int = 240


@dataclass(frozen=True, slots=True)
class EvalRunResult:
    """Every attempt of one eval, in order, with what they reported.

    Attributes:
        accepted: Whether some attempt completed and its results were read.
    """

    eval_key: str
    attempts: tuple[EvalRunAttempt, ...]
    diagnostics: DiagnosticBag
    accepted: bool = False

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


@dataclass(frozen=True, slots=True)
class _RunSetup:
    """What every attempt of one eval shares, resolved before the first one starts.

    Attributes:
        timestamp: The run names' `ts` token.
        agent_version: The concrete version the agent's selector resolved to.
        required_completed: The completed attempts a baseline capture needs; 1 otherwise.
        attempt_limit: The attempts allowed in all.
        config_path: Where the eval's config is staged.
    """

    run: EvalRunConfig
    timestamp: str
    agent_version: str
    required_completed: int
    attempt_limit: int
    config_path: str


class RunEvalSuite:
    """Run a suite of compiled evals against their published agents.

    Only reads and runs evaluations; it publishes nothing but each eval's config file, which it
    stages when the staged copy is missing or cannot be trusted.
    """

    def __init__(
        self,
        port: SnowflakePort,
        clock: ClockPort,
        lifecycle_config: EvalLifecycleConfig = EvalLifecycleConfig(),
    ) -> None:
        self._port = port
        self._clock = clock
        self._lifecycle_config = lifecycle_config

    def run(
        self,
        compiled: Sequence[CompiledEval],
        *,
        defaults: EvalDefaults = EvalDefaults(),
        options: EvalRunOptions,
        fail_fast: bool = False,
        config_digests: Mapping[str, str] | None = None,
        baseline_capture: bool = False,
    ) -> EvalSuiteResult:
        """Run every eval and collect their results in suite order.

        With `fail_fast`, or fewer than two evals, they run one at a time, and `fail_fast` stops
        at the first eval not accepted. Otherwise they run in parallel, as many at once as
        `defaults.concurrency` says, else as the most any eval's run configuration requests.

        Args:
            config_digests: The MD5 digest of each eval's staged config that state trusts, by
                eval key; a staged config without one is staged again.
            baseline_capture: Run each eval until it has the completed attempts its baseline
                needs, rather than its configured attempts.
        """
        config_digests = config_digests or {}

        def run_one(item: CompiledEval) -> EvalRunResult:
            return self._run_eval(
                item,
                defaults,
                options,
                config_digests.get(item.artifact_key),
                baseline_capture,
                defaults.baseline_runs,
            )

        if fail_fast or len(compiled) < 2:
            results = _in_order(compiled, run_one, fail_fast)
        else:
            with ThreadPoolExecutor(max_workers=_suite_concurrency(compiled, defaults)) as pool:
                results = list(pool.map(run_one, compiled))
        diagnostics = [diagnostic for result in results for diagnostic in result.diagnostics]
        return EvalSuiteResult(tuple(results), DiagnosticBag(tuple(diagnostics)))

    def _run_eval(
        self,
        compiled: CompiledEval,
        defaults: EvalDefaults,
        options: EvalRunOptions,
        config_digest: str | None,
        baseline_capture: bool,
        default_baseline_runs: int | None,
    ) -> EvalRunResult:
        """Run one eval: resolve what its attempts share and stage its config, then run the attempts.

        An eval that cannot be set up runs no attempt and reports why.
        """
        setup = self._setup(compiled, defaults, options, config_digest, baseline_capture, default_baseline_runs)
        if isinstance(setup, Diagnostic):
            return EvalRunResult(compiled.artifact_key, (), DiagnosticBag((setup,)))
        return self._attempts(compiled, options, setup, baseline_capture)

    def _setup(
        self,
        compiled: CompiledEval,
        defaults: EvalDefaults,
        options: EvalRunOptions,
        config_digest: str | None,
        baseline_capture: bool,
        default_baseline_runs: int | None,
    ) -> _RunSetup | Diagnostic:
        """Resolve what every attempt shares and stage the config; a diagnostic when the eval cannot run.

        A baseline capture allows the completed attempts it needs plus the retries; any other
        run allows its first attempt plus the retries.

        Diagnostics:
            SST-APL023: the eval has no run configuration, its agent version does not resolve,
                or its config cannot be staged.
        """
        run = compiled.resolved.config.run
        if run is None:
            return D("SST-APL023", artifact=compiled.artifact_key, detail="run configuration is absent")
        timestamp = options.timestamp or _compact_timestamp()
        retry_count = run.retry if run.retry is not None else defaults.retry or 0
        try:
            agent_version = self._port.resolve_agent_version(
                compiled.agent_target,
                compiled.resolved.config.agent_version or "",
            )
        except SnowflakePortError as exc:
            return D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
        required_completed = (run.baseline_runs or default_baseline_runs or 1) if baseline_capture else 1
        attempt_limit = required_completed + retry_count if baseline_capture else retry_count + 1
        config_path = EvalLifecycleHandler(self._port, self._lifecycle_config).config_path(compiled.rendered_artifact)
        try:
            self._ensure_config(config_path, compiled.rendered.config_yaml.encode("utf-8"), config_digest)
        except SnowflakePortError as exc:
            return D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
        return _RunSetup(run, timestamp, agent_version, required_completed, attempt_limit, config_path)

    def _attempts(
        self,
        compiled: CompiledEval,
        options: EvalRunOptions,
        setup: _RunSetup,
        baseline_capture: bool,
    ) -> EvalRunResult:
        """Run the eval's attempts in order and record each; any one completed and read accepts it.

        Every allowed attempt runs, a completed one included, except that a cancelled run ends
        the eval and a baseline capture stops once it has the completed attempts it needs.
        """
        attempts: list[EvalRunAttempt] = []
        diagnostics: list[Diagnostic] = []
        completed_count = 0
        for attempt_number in range(1, setup.attempt_limit + 1):
            attempt, attempt_diagnostics = self._attempt(compiled, options, setup, attempt_number)
            attempts.append(attempt)
            diagnostics.extend(attempt_diagnostics)
            if attempt.terminal_status == "CANCELLED":
                break
            if _retrieved(attempt):
                completed_count += 1
                if baseline_capture and completed_count >= setup.required_completed:
                    break
        return EvalRunResult(
            compiled.artifact_key,
            tuple(attempts),
            DiagnosticBag(tuple(diagnostics)),
            completed_count > 0,
        )

    def _attempt(
        self,
        compiled: CompiledEval,
        options: EvalRunOptions,
        setup: _RunSetup,
        attempt_number: int,
    ) -> tuple[EvalRunAttempt, tuple[Diagnostic, ...]]:
        """Start one attempt, poll it to a terminal status, and read its results once it completed.

        A failure at any step ends the attempt with it: the attempt records the terminal status
        it reached and why it failed.

        Diagnostics:
            SST-APL023: the run could not be started, or its status could not be read.
            SST-APL024: the run ended partially completed.
            SST-SNO001: the run completed but its results could not be read or did not match.
        """
        run_name = _run_name(compiled, setup, options, attempt_number)
        started = self._start(compiled, run_name, setup.config_path)
        if started is not None:
            return EvalRunAttempt(run_name, attempt_number, "START_FAILED", retrieval_error=started.message), (started,)
        try:
            terminal_status, status_details = self._poll(compiled, run_name, setup.config_path, options)
        except (SnowflakePortError, ValueError) as exc:
            diagnostic = D("SST-APL023", artifact=compiled.artifact_key, detail=str(exc))
            return EvalRunAttempt(run_name, attempt_number, "STATUS_FAILED", retrieval_error=str(exc)), (diagnostic,)
        diagnostics: tuple[Diagnostic, ...] = ()
        if terminal_status in _PARTIAL_STATUSES:
            diagnostics = (D("SST-APL024", artifact=compiled.artifact_key, found=terminal_status),)
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

    def _start(self, compiled: CompiledEval, run_name: str, config_path: str) -> Diagnostic | None:
        """Start a run in the agent's schema; its SST-APL023 diagnostic when Snowflake refuses."""
        result = self._port.execute_script(
            (
                sql("USE DATABASE {database}", database=ident(compiled.agent_target.database)),
                sql("USE SCHEMA {schema}", schema=scope(SchemaScope.from_qualified_name(compiled.agent_target))),
                _evaluation_call("START", run_name, config_path),
            )
        )
        if not result.ok:
            detail = result.error.message if result.error is not None else "EXECUTE_AI_EVALUATION returned no result"
            return D("SST-APL023", artifact=compiled.artifact_key, detail=detail)
        return None

    def _poll(
        self,
        compiled: CompiledEval,
        run_name: str,
        config_path: str,
        options: EvalRunOptions,
    ) -> tuple[str, tuple[str, ...]]:
        """Read a run's status until it is terminal, waiting the poll interval between two reads.

        Raises:
            SnowflakePortError: a read failed, or the run was not terminal after `max_polls` reads.
            ValueError: a status row was malformed.
        """
        for poll in range(options.max_polls):
            status, details = _read_status(self._port, compiled, run_name, config_path)
            if status in EVAL_TERMINAL_STATUSES:
                return status, details
            if poll + 1 < options.max_polls:
                self._clock.sleep(options.poll_interval_ms)
        raise SnowflakePortError(f"evaluation run {run_name!r} did not reach a terminal status")


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
    compiled: Sequence[CompiledEval],
    run_one: Callable[[CompiledEval], EvalRunResult],
    fail_fast: bool,
) -> list[EvalRunResult]:
    """Run the evals one at a time; with `fail_fast`, stop after the first one not accepted."""
    results = []
    for item in compiled:
        result = run_one(item)
        results.append(result)
        if fail_fast and not result.success:
            break
    return results


def _suite_concurrency(compiled: Sequence[CompiledEval], defaults: EvalDefaults) -> int:
    """Return how many evals run at once: the project's setting, else the most any eval requests."""
    requested: list[int] = []
    for item in compiled:
        run = item.resolved.config.run
        if run is not None and run.concurrency is not None:
            requested.append(run.concurrency)
    if defaults.concurrency:
        return max(1, defaults.concurrency)
    return max(1, max(requested, default=1))


def _run_name(compiled: CompiledEval, setup: _RunSetup, options: EvalRunOptions, attempt_number: int) -> str:
    """Name one attempt's run from the eval's template; a retry appends `_R<attempt>`."""
    base_name = render_eval_name_template(
        setup.run.name_template or _DEFAULT_RUN_NAME_TEMPLATE,
        agent=compiled.name,
        sha7=options.git_sha[:7],
        variant=setup.run.variant or "ci",
        ts=setup.timestamp,
    )
    return base_name if attempt_number == 1 else f"{base_name}_R{attempt_number}"


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
