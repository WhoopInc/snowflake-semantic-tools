"""The eval gate over in-memory ports: the lock, the publication check, capture, and gating."""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.evals.run import EvalRunsInterrupted, empty_eval_suite_json
from snowflake_semantic_tools.app.evals.suite import (
    EvalGateOutcome,
    EvalGateRefused,
    EvalGateRequest,
    RunEvalGate,
    compiled_evals,
)
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.cli.runner import terminated_as_interrupt
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalCatalog,
    EvalDefaults,
    EvalRunConfig,
    EvalSystemMetric,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compile_eval, resolved_eval, result_rows, status_result
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.project_inputs import InMemoryProjectInputs

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")


@pytest.fixture(autouse=True)
def published(monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat every eval as published, unless a test restores the real check."""
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.validate_eval_publication", lambda *args: DiagnosticBag()
    )
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run._compact_timestamp", lambda: "20260928T010203Z")


def completed_attempt() -> list[QueryResult | Exception]:
    return [status_result("COMPLETED"), result_rows()]


def gate(
    port: EvalSnowflake,
    *,
    request: EvalGateRequest = EvalGateRequest(),
    result: CompileResult | None = None,
    inputs: InMemoryProjectInputs | None = None,
    store: InMemoryEvalStateStore | None = None,
    state_store: InMemoryStateStore | None = None,
) -> tuple[EvalGateOutcome | EvalGateRefused, InMemoryEvalStateStore, InMemoryStateStore, InMemoryProjectInputs]:
    compiled = result or compile_eval()
    project = inputs or InMemoryProjectInputs(revision="abcdef0", evals=EvalCatalog((), (), EvalDefaults()))
    eval_store = store if store is not None else InMemoryEvalStateStore()
    lock = state_store or InMemoryStateStore()
    use_case = RunEvalGate(port, project, lock, eval_store, FixedClock())
    outcome = use_case.run(
        compiled_evals(compiled), build_manifest(compiled), request, target=target(), state_table=STATE_TABLE
    )
    return outcome, eval_store, lock, project


def test_the_run_is_refused_while_another_operation_holds_the_target_lock() -> None:
    held = InMemoryStateStore()
    held.acquire_lock("apply-run", break_stale=False)
    port = EvalSnowflake([])

    outcome, _, lock, inputs = gate(port, state_store=held)

    assert isinstance(outcome, EvalGateRefused)
    assert outcome.reason == "cannot run evals while apply-run holds the target lock"
    assert [item.code for item in outcome.diagnostics] == ["SST-APL011"]
    assert lock.holder == "apply-run" and port.queries == []
    assert inputs.reads == ["config", "eval_catalog"]
    unnamed = InMemoryStateStore()
    unnamed.locked = True
    refused, _, _, _ = gate(EvalSnowflake([]), state_store=unnamed)
    assert isinstance(refused, EvalGateRefused)
    assert refused.reason == "cannot run evals while another operation holds the target lock"


def test_the_run_takes_the_remote_lock_an_apply_takes_and_releases_it() -> None:
    port = EvalSnowflake(completed_attempt())
    elsewhere = EvalSnowflake([])
    elsewhere.run_locks = port.run_locks
    other = elsewhere.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("eval-other"), break_stale=False)
    assert other.fence is not None

    refused, _, lock, _ = gate(port)

    assert isinstance(refused, EvalGateRefused)
    assert refused.reason == "cannot run evals while eval-other holds the target lock"
    # Another eval run regenerates nothing, so only the refusal is reported.
    assert [item.code for item in refused.diagnostics] == ["SST-APL011"]
    assert not lock.locked
    port.run_locks.release_run_lock(STATE_TABLE, "verify", other.fence)
    outcome, _, _, _ = gate(port)
    assert isinstance(outcome, EvalGateOutcome)
    assert port.run_locks.claims[-1].startswith("eval-") and port.run_locks.rows == {}


def test_an_unpublished_eval_stops_before_any_run_and_releases_the_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.app.evals.run import validate_eval_publication

    monkeypatch.setattr("snowflake_semantic_tools.app.evals.suite.validate_eval_publication", validate_eval_publication)
    port = EvalSnowflake([])

    outcome, _, lock, _ = gate(port)

    assert isinstance(outcome, EvalGateOutcome)
    assert (outcome.passed, outcome.suite) == (False, None)
    assert [item.code for item in outcome.diagnostics] == ["SST-APL012"]
    assert dict(outcome.data) == {"suite": "evals", **empty_eval_suite_json()}
    assert not lock.locked and port.scripts == []


def test_a_gate_without_a_baseline_has_no_signal_and_records_the_gate() -> None:
    outcome, store, lock, inputs = gate(EvalSnowflake(completed_attempt()))

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL758"]
    assert (outcome.data["gate_verdict"], outcome.data["gate_reasons"]) == ("no_signal", ["baseline_absent"])
    assert ("verify", "eval:sales_agent") in store.gates and not lock.locked
    assert inputs.reads == ["config", "eval_catalog", "git_sha"]


def gated_eval(tier: str | None = None) -> CompileResult:
    """The sales eval with `answer_correctness` gating at a score of 0.5, in `tier` when one is given."""
    resolved = resolved_eval()
    metric = EvalSystemMetric(
        resolved.config.system_metrics[0].origin, "answer_correctness", "v3", True, ThresholdRange(0.5)
    )
    config = replace(resolved.config, system_metrics=(metric,))
    if tier is not None and config.run is not None:
        config = replace(config, run=replace(config.run, tier=tier))
    return compile_eval(replace(resolved, config=config))


def test_a_captured_baseline_lets_the_next_gate_pass_then_report_a_regression() -> None:
    store = InMemoryEvalStateStore()
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    result = gated_eval()

    captured, _, _, _ = gate(EvalSnowflake(completed_attempt()), request=capture, store=store, result=result)
    passing, _, _, _ = gate(EvalSnowflake(completed_attempt()), store=store, result=result)
    failing_rows = result_rows()
    worse = replace(failing_rows, rows=tuple((*row[:10], 0.0, *row[11:]) for row in failing_rows.rows))
    regressed, _, _, _ = gate(EvalSnowflake([status_result("COMPLETED"), worse]), store=store, result=result)

    assert isinstance(captured, EvalGateOutcome) and captured.passed
    assert (captured.data["gate_verdict"], captured.data["captured_baselines"]) == ("captured", ["eval:sales_agent"])
    assert store.baselines[("verify", "eval:sales_agent")].reason == "initial"
    assert isinstance(passing, EvalGateOutcome) and passing.passed and passing.data["gate_verdict"] == "passed"
    assert isinstance(regressed, EvalGateOutcome)
    assert regressed.data["gate_verdict"] == "regressed" and regressed.data["regression_count"] == 1
    regressions = regressed.data["regressions"]
    assert isinstance(regressions, list)
    [regression] = regressions
    assert regression["metric_name"] == "answer_correctness"
    # A report-tier regression adds no diagnostic, so the run itself still passes.
    assert regressed.passed


def test_a_blocking_regression_fails_the_run_and_leaves_the_gate_unresolved() -> None:
    store = InMemoryEvalStateStore()
    result = gated_eval("blocking")
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    gate(EvalSnowflake(completed_attempt()), request=capture, store=store, result=result)
    failing_rows = result_rows()
    worse = replace(failing_rows, rows=tuple((*row[:10], 0.0, *row[11:]) for row in failing_rows.rows))

    regressed, _, _, _ = gate(EvalSnowflake([status_result("COMPLETED"), worse]), store=store, result=result)

    assert isinstance(regressed, EvalGateOutcome) and not regressed.passed
    assert [item.code for item in regressed.diagnostics] == ["SST-VAL763"]
    assert (regressed.data["gate_verdict"], regressed.data["regression_count"]) == ("regressed", 1)
    assert store.gates[("verify", "eval:sales_agent")].unresolved


def test_a_capture_takes_the_baseline_runs_of_the_eval_before_the_default() -> None:
    resolved = resolved_eval()
    eval_with_runs = replace(resolved, config=replace(resolved.config, run=EvalRunConfig(label="ci", baseline_runs=2)))
    result = compile_eval(eval_with_runs)
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    second = status_result("COMPLETED", "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z_R2")

    outcome, store, _, _ = gate(
        EvalSnowflake([*completed_attempt(), second, result_rows()]), request=capture, result=result
    )

    assert isinstance(outcome, EvalGateOutcome) and outcome.passed
    assert len(store.baselines[("verify", "eval:sales_agent")].run_names) == 2


def test_a_staged_config_whose_digest_state_trusts_is_not_staged_again() -> None:
    result = compile_eval()
    item = compiled_evals(result)[0]
    content = item.rendered.config_yaml.encode("utf-8")
    port = EvalSnowflake(completed_attempt())
    config_path = (
        "@DB.S.EVAL_CONFIGS/sales_agent/" + dict(item.rendered_artifact.component_fingerprints)["config"] + ".yaml"
    )
    port.stage_file(config_path, content)
    digest = port.staged_file_metadata[config_path].md5
    assert digest is not None
    entry = AppliedEntry(
        "f", "DB.S.X", "now", "run", "applied", "f", "m", component_fingerprints=(("config_stage_md5", digest),)
    )
    other = AppliedEntry("f", "DB.S.Y", "now", "run", "applied", "f", "m")
    port.remote_state = MappingProxyType({item.artifact_key: entry, "eval:other": other})

    outcome, _, _, _ = gate(port, result=result)

    assert isinstance(outcome, EvalGateOutcome)
    assert port.uploads == []


def test_the_eval_config_stage_comes_from_the_apply_block() -> None:
    inputs = InMemoryProjectInputs(revision="abcdef0", tree={"apply": {"eval_config_stage": {"stage": "MY_CONFIGS"}}})
    port = EvalSnowflake(completed_attempt())

    gate(port, inputs=inputs)

    assert [path.split("/")[0] for path, _ in port.uploads] == ["@DB.S.MY_CONFIGS"]


def test_the_lock_is_released_when_the_run_raises() -> None:
    class Dropped(EvalSnowflake):
        def read_state(self, state_table, target_name):  # type: ignore[no-untyped-def]
            raise SnowflakePortError("connection reset while reading state")

    lock = InMemoryStateStore()
    with pytest.raises(SnowflakePortError, match="connection reset"):
        gate(Dropped([]), state_store=lock)
    assert not lock.locked


class InterruptedAtStart(EvalSnowflake):
    """Starts the run, then is interrupted by `interrupt` before the START call returns."""

    def __init__(self, interrupt: Callable[[], None]) -> None:
        super().__init__([])
        self.interrupt = interrupt

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
        reply = super().query_in_context(scope, sql, params)
        if "EXECUTE_AI_EVALUATION('START'" in str(sql):
            self.interrupt()
        return reply


def _keyboard_interrupt() -> None:
    raise KeyboardInterrupt


def test_an_interrupted_run_names_the_runs_it_started_and_releases_both_locks() -> None:
    port = InterruptedAtStart(_keyboard_interrupt)
    lock = InMemoryStateStore()

    with pytest.raises(EvalRunsInterrupted) as raised:
        gate(port, state_store=lock)

    assert raised.value.run_names == ("EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z",)
    assert not lock.locked and port.run_locks.rows == {}


def test_sigterm_interrupts_the_run_as_ctrl_c_does_so_both_locks_are_released() -> None:
    port = InterruptedAtStart(lambda: signal.raise_signal(signal.SIGTERM))
    lock = InMemoryStateStore()
    before = signal.getsignal(signal.SIGTERM)

    with pytest.raises(EvalRunsInterrupted), terminated_as_interrupt():
        gate(port, state_store=lock)

    assert not lock.locked and port.run_locks.rows == {}
    assert signal.getsignal(signal.SIGTERM) == before


def test_sigterm_handling_leaves_a_worker_thread_unchanged() -> None:
    seen: list[object] = []

    def in_worker() -> None:
        with terminated_as_interrupt():
            seen.append(signal.getsignal(signal.SIGTERM))

    worker = threading.Thread(target=in_worker)
    worker.start()
    worker.join()
    assert seen == [signal.getsignal(signal.SIGTERM)]


def test_an_eval_run_takes_over_an_expired_lock_only_when_asked_and_never_a_live_one() -> None:
    def locked_by(ttl: int) -> EvalSnowflake:
        port = EvalSnowflake(completed_attempt())
        port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("crashed", "R", "h", ttl), break_stale=False)
        port.run_locks.now = 60.0
        return port

    breaking = EvalGateRequest(break_stale_lock=True)
    kept, _, _, _ = gate(locked_by(10))
    assert isinstance(kept, EvalGateRefused) and [item.code for item in kept.diagnostics] == ["SST-APL011"]
    broke, _, _, _ = gate(locked_by(10), request=breaking)
    assert isinstance(broke, EvalGateOutcome)
    assert [(item.code, item.message) for item in broke.diagnostics if item.code.startswith("SST-APL01")] == [
        ("SST-APL010", "broke a stale lock held by run crashed (R on h), expired 10.0")
    ]
    live, _, _, _ = gate(locked_by(600), request=breaking)
    assert isinstance(live, EvalGateRefused) and [item.code for item in live.diagnostics] == ["SST-APL011"]
