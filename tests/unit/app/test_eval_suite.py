"""The eval gate over in-memory ports: the lock, the publication check, capture, and gating."""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.app.evals.gate import capture_baseline
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.projection import empty_eval_suite_json
from snowflake_semantic_tools.app.evals.run import EvalRunsInterrupted
from snowflake_semantic_tools.app.evals.suite import (
    EvalGateOutcome,
    EvalGateRefused,
    EvalGateRequest,
    RunEvalGate,
    compiled_evals,
)
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.cli.signals import terminated_as_interrupt
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentTool
from snowflake_semantic_tools.domain.model.eval import (
    EvalCatalog,
    EvalDefaults,
    EvalRunConfig,
    EvalSystemMetric,
    ThresholdRange,
    eval_status_is_provisional,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock, PollClock
from tests.helpers.eval_builders import (
    STATUS_COLUMNS,
    EvalSnowflake,
    attempt,
    compile_eval,
    resolved_eval,
    result_rows,
    run_result,
    status_result,
)
from tests.helpers.eval_inputs import settled
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
    clock: FixedClock | None = None,
) -> tuple[EvalGateOutcome | EvalGateRefused, InMemoryEvalStateStore, InMemoryStateStore, InMemoryProjectInputs]:
    compiled = result or compile_eval()
    project = inputs or InMemoryProjectInputs(revision="abcdef0", evals=EvalCatalog((), (), EvalDefaults()))
    eval_store = store if store is not None else InMemoryEvalStateStore()
    lock = state_store or InMemoryStateStore()
    use_case = RunEvalGate(port, project, lock, eval_store, clock or FixedClock())
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


def test_an_apply_holding_the_lock_is_reported_alone_whatever_views_the_agent_uses() -> None:
    value = resolved_eval()
    origin = Origin("agents/sales/agent.yml", 1)
    tools = (AgentTool("cortex_analyst_text_to_sql", origin, semantic_view="orders"),)
    port = EvalSnowflake([])
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("apply-run"), break_stale=False)

    refused, _, _, _ = gate(port, result=compile_eval(replace(value, agent=replace(value.agent, tools=tools))))

    # No eval started, so it overlapped nothing, and the lock cannot say what the apply changes.
    assert isinstance(refused, EvalGateRefused)
    assert [item.code for item in refused.diagnostics] == ["SST-APL011"] and port.queries == []


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
    assert [item.code for item in outcome.diagnostics] == ["SST-APL037"]
    assert dict(outcome.data) == {"suite": "evals", **empty_eval_suite_json()}
    assert not lock.locked and port.scripts == []


def test_a_gate_without_a_baseline_is_refused_before_it_runs_and_records_the_gate() -> None:
    port = EvalSnowflake([])

    outcome, store, lock, inputs = gate(port)

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL758"]
    assert (outcome.data["gate_verdict"], outcome.data["gate_reasons"]) == ("no_signal", ["baseline_absent"])
    assert outcome.recapture == ("eval:sales_agent",)
    # Nothing was started, so the gate records no runs, and no commit names a run.
    assert port.scripts == [] and port.queries == []
    assert store.gates[("verify", "eval:sales_agent")].run_names == () and not lock.locked
    assert inputs.reads == ["config", "eval_catalog"]


def two_evals(*, blocking: bool = False) -> CompileResult:
    """The sales eval and the same eval of an ops agent beside it, `ops_agent` first."""
    sales = resolved_eval()
    ops = replace(sales, agent=replace(sales.agent, name="ops_agent"))
    if blocking:
        sales, ops = (settled(item, eval_tier="blocking") for item in (sales, ops))
    catalog = EvalCatalog((sales, ops), sales.custom_metrics, diagnostics=DiagnosticBag())
    targets = {
        "sales_agent": QualifiedName.parse("DB.S.SALES_AGENT"),
        "ops_agent": QualifiedName.parse("DB.S.OPS_AGENT"),
    }
    return CompileEvals(catalog, agent_targets=targets).run_result()


def test_an_eval_its_baseline_cannot_gate_is_refused_before_any_run_starts() -> None:
    result = gated_eval()
    store = captured_store(result)
    port = EvalSnowflake([])
    # An apply committed a new version since the baseline was captured.
    port.agent_versions[("DB.S.SALES_AGENT", "committed")] = "VERSION$2"

    outcome, _, _, _ = gate(port, store=store, result=result)

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    [refused] = outcome.diagnostics
    assert refused.code == "SST-VAL759"
    assert refused.message == (
        "eval 'eval:sales_agent' baseline is incompatible: it was captured on VERSION$1, and this run uses VERSION$2"
    )
    # No evaluation run was started, staged or polled for it.
    assert port.scripts == [] and port.queries == [] and port.uploads == []
    assert (outcome.data["gate_verdict"], outcome.data["gate_reasons"]) == ("no_signal", ["baseline_incompatible"])
    assert outcome.data["attempt_count"] == 0 and outcome.recapture == ("eval:sales_agent",)


def commented(port: EvalSnowflake, version: str, spec: str) -> EvalSnowflake:
    """Make `version` the sales agent's committed version, its comment carrying `spec`."""
    port.agent_versions[("DB.S.SALES_AGENT", "committed")] = version
    port.agent_version_rows["DB.S.SALES_AGENT"] = (
        AgentVersion(version, "LAST", comment=f"git:abc1234 sst-spec:{spec}"),
    )
    return port


def test_a_restored_agent_version_holding_the_baselines_spec_is_gated_on_it() -> None:
    result = gated_eval()
    store = InMemoryEvalStateStore()
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    gate(
        commented(EvalSnowflake(completed_attempt()), "VERSION$1", "a" * 64),
        request=capture,
        store=store,
        result=result,
    )
    assert store.baselines[("verify", "eval:sales_agent")].agent_spec_fingerprint == "a" * 64

    restored, _, _, _ = gate(
        commented(EvalSnowflake(completed_attempt()), "VERSION$3", "a" * 64), store=store, result=result
    )
    changed, _, _, _ = gate(commented(EvalSnowflake([]), "VERSION$4", "b" * 64), store=store, result=result)

    assert isinstance(restored, EvalGateOutcome) and restored.passed and restored.data["gate_verdict"] == "passed"
    assert isinstance(changed, EvalGateOutcome) and not changed.passed
    assert changed.diagnostics[0].message.endswith(
        "it was captured on VERSION$1, and this run uses VERSION$4, whose specification differs"
    )


def test_a_refused_eval_leaves_the_others_to_run_and_fails_the_suite() -> None:
    result = two_evals(blocking=True)
    store = InMemoryEvalStateStore()
    sales = next(item for item in compiled_evals(result) if item.name == "sales_agent")
    baseline = captured_store(compile_eval(sales.resolved)).baselines[("verify", "eval:sales_agent")]
    store.baselines[("verify", "eval:sales_agent")] = baseline
    port = EvalSnowflake(completed_attempt())
    port.agent_versions[("DB.S.OPS_AGENT", "committed")] = "VERSION$1"

    outcome, _, _, _ = gate(port, store=store, result=result)

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL758"]
    assert outcome.diagnostics[0].message == "eval 'eval:ops_agent' has no captured baseline"
    started = [statement for _, statement in port.scripts]
    assert len(started) == 1 and "EVAL_SALES_AGENT_" in started[0]
    evals = outcome.data["evals"]
    assert isinstance(evals, list)
    assert [(item["eval_key"], len(item["attempts"])) for item in evals] == [
        ("eval:ops_agent", 0),
        ("eval:sales_agent", 1),
    ]
    assert outcome.data["gate_reasons"] == ["baseline_absent"] and outcome.recapture == ("eval:ops_agent",)
    gates = store.gates
    assert gates[("verify", "eval:ops_agent")].unresolved and gates[("verify", "eval:ops_agent")].run_names == ()
    assert not gates[("verify", "eval:sales_agent")].unresolved


def test_with_fail_fast_a_refusal_starts_no_eval_at_all() -> None:
    port = EvalSnowflake([])
    port.agent_versions[("DB.S.OPS_AGENT", "committed")] = "VERSION$1"

    outcome, store, _, _ = gate(port, request=EvalGateRequest(fail_fast=True), result=two_evals())

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL758", "SST-VAL758"]
    assert port.scripts == [] and port.queries == []
    assert outcome.recapture == ("eval:ops_agent", "eval:sales_agent")


def test_an_eval_fail_fast_never_reached_is_not_gated() -> None:
    result = two_evals()
    store = InMemoryEvalStateStore()
    for item in compiled_evals(result):
        values = (("q", "answer_correctness", True), ("q", "grounding", True))
        record = capture_baseline(
            item, run_result(attempt("base", *values)), reason="initial", captured_at="2026-01-01T00:00:00Z"
        )
        store.baselines[("verify", item.artifact_key)] = record
    port = EvalSnowflake([])
    port.agent_versions[("DB.S.OPS_AGENT", "committed")] = "VERSION$1"
    port.execute_results.append(ExecResult(False, error=ExecutionError("cannot start")))

    outcome, _, _, _ = gate(port, request=EvalGateRequest(fail_fast=True), result=result, store=store)

    # The ops eval, first, could not start, so the suite stopped before the sales eval.
    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-APL023", "SST-SNO001"]
    assert set(store.gates) == {("verify", "eval:ops_agent")} and outcome.recapture == ()


def test_an_eval_whose_agent_version_does_not_resolve_is_gated_after_its_setup() -> None:
    port = EvalSnowflake([])
    del port.agent_versions[("DB.S.SALES_AGENT", "committed")]

    outcome, _, _, _ = gate(port)

    # Setup says why the eval cannot run; the gate, judged after it, finds no baseline.
    assert isinstance(outcome, EvalGateOutcome)
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL720", "SST-VAL758"]
    assert outcome.data["gate_reasons"] == ["baseline_absent"]


def gated_eval(tier: str | None = None) -> CompileResult:
    """The sales eval with `answer_correctness` gating at a score of 0.5, in `tier` when one is given."""
    resolved = resolved_eval()
    metric = EvalSystemMetric(
        resolved.config.system_metrics[0].origin, "answer_correctness", "v3", True, ThresholdRange(0.5)
    )
    config = replace(resolved.config, system_metrics=(metric,))
    return compile_eval(settled(replace(resolved, config=config), eval_tier=tier))


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


FIRST_RUN = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"


def retried_eval(*, retry: int, baseline_runs: int | None = None) -> CompileResult:
    """The sales eval gating `answer_correctness` at 0.5 in a blocking tier, with `retry` retries."""
    resolved = resolved_eval()
    metric = EvalSystemMetric(
        resolved.config.system_metrics[0].origin, "answer_correctness", "v3", True, ThresholdRange(0.5)
    )
    gating = replace(resolved, config=replace(resolved.config, system_metrics=(metric,)))
    return compile_eval(settled(gating, eval_tier="blocking", retry=retry, baseline_runs=baseline_runs))


def settle_reads() -> int:
    """The status reads, a poll interval apart, that span the default settle window."""
    options = EvalRunOptions("abcdef0")
    return options.partial_settle_ms // options.poll_interval_ms + 1


def ended(status: str, run_name: str = FIRST_RUN) -> QueryResult:
    return QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", status, ["judge timed out"]),))


def worse_rows() -> QueryResult:
    rows = result_rows()
    return replace(rows, rows=tuple((*row[:10], 0.0, *row[11:]) for row in rows.rows))


def captured_store(result: CompileResult) -> InMemoryEvalStateStore:
    store = InMemoryEvalStateStore()
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    captured, _, _, _ = gate(EvalSnowflake(completed_attempt()), request=capture, store=store, result=result)
    assert isinstance(captured, EvalGateOutcome) and captured.passed
    return store


@pytest.mark.parametrize("status", ("FAILED", "PARTIALLY_COMPLETED"))
def test_an_attempt_a_completed_retry_replaced_is_reported_and_the_gate_judges_the_retry(status: str) -> None:
    result = retried_eval(retry=1)
    store = captured_store(result)
    retry = status_result("COMPLETED", f"{FIRST_RUN}_R2")
    # A partial status ends the attempt only once it has read the same for the settle window.
    first = [ended(status)] * (settle_reads() if eval_status_is_provisional(status) else 1)

    passing, _, _, _ = gate(
        EvalSnowflake([*first, retry, result_rows()]), store=store, result=result, clock=PollClock()
    )
    regressed, _, _, _ = gate(
        EvalSnowflake([*first, retry, worse_rows()]), store=store, result=result, clock=PollClock()
    )

    assert isinstance(passing, EvalGateOutcome) and passing.passed
    assert passing.data["gate_verdict"] == "passed"
    [warning] = passing.diagnostics
    assert (warning.code, warning.severity) == ("SST-APL029", Severity.WARNING)
    assert warning.message == (
        f"eval 'eval:sales_agent': run '{FIRST_RUN}' ended {status} and was retried: judge timed out"
    )
    # Both attempts are reported, the one that did not pass with its status details.
    evals = passing.data["evals"]
    assert isinstance(evals, list)
    assert [(item["run_name"], item["terminal_status"], item["status_details"]) for item in evals[0]["attempts"]] == [
        (FIRST_RUN, status, ["judge timed out"]),
        (f"{FIRST_RUN}_R2", "COMPLETED", []),
    ]
    # The verdict is the retry's: a blocking regression in it still fails the run.
    assert isinstance(regressed, EvalGateOutcome) and not regressed.passed
    assert [item.code for item in regressed.diagnostics] == ["SST-APL029", "SST-VAL763"]
    assert regressed.data["gate_verdict"] == "regressed"


def test_an_eval_whose_every_attempt_failed_fails_with_no_signal() -> None:
    result = retried_eval(retry=1)
    store = captured_store(result)

    outcome, _, _, _ = gate(
        EvalSnowflake([ended("FAILED"), ended("FAILED", f"{FIRST_RUN}_R2")]), store=store, result=result
    )

    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-APL023", "SST-APL023", "SST-SNO001"]
    assert (outcome.data["gate_verdict"], outcome.data["gate_reasons"]) == ("no_signal", ["current_no_signal"])


def test_a_baseline_is_captured_from_its_completed_attempts_when_a_failed_one_was_retried() -> None:
    result = retried_eval(retry=1, baseline_runs=5)
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    names = [FIRST_RUN, *(f"{FIRST_RUN}_R{number}" for number in range(2, 7))]
    script: list[QueryResult | Exception] = [status_result("COMPLETED"), result_rows(), ended("FAILED", names[1])]
    for name in names[2:]:
        script.extend((status_result("COMPLETED", name), result_rows()))

    outcome, store, _, _ = gate(EvalSnowflake(script), request=capture, result=result)

    assert isinstance(outcome, EvalGateOutcome) and outcome.passed
    assert store.baselines[("verify", "eval:sales_agent")].run_names == (names[0], *names[2:])
    retried = [item for item in outcome.diagnostics if item.code == "SST-APL029"]
    assert [item.severity for item in retried] == [Severity.WARNING] and names[1] in retried[0].message
    assert not outcome.diagnostics.has_errors


def test_a_baseline_with_too_few_completed_attempts_is_refused() -> None:
    result = retried_eval(retry=0, baseline_runs=2)
    capture = EvalGateRequest(capture_baseline=True, reason="initial")
    store = InMemoryEvalStateStore()
    script: list[QueryResult | Exception] = [*completed_attempt(), ended("FAILED", f"{FIRST_RUN}_R2")]

    outcome, _, _, _ = gate(EvalSnowflake(script), request=capture, result=result, store=store)

    # The failed attempt reports its own outcome, then the shortfall; nothing is captured.
    assert isinstance(outcome, EvalGateOutcome) and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-APL023", "SST-VAL734"]
    assert outcome.diagnostics[-1].message == "eval config for 'sales_agent': 1 baseline_runs completed, 2 required"
    assert (outcome.data["gate_verdict"], outcome.data["captured_baselines"]) == ("not_evaluated", [])
    assert store.baselines == {}


def test_a_capture_takes_the_baseline_runs_of_the_eval_before_the_default() -> None:
    resolved = resolved_eval()
    eval_with_runs = settled(
        replace(resolved, config=replace(resolved.config, run=EvalRunConfig(label="ci"))), baseline_runs=2
    )
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

    gate(port, inputs=inputs, store=captured_store(compile_eval()))

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

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        reply = super().query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)
        if "EXECUTE_AI_EVALUATION('START'" in str(sql):
            self.interrupt()
        return reply


def _keyboard_interrupt() -> None:
    raise KeyboardInterrupt


def test_an_interrupted_run_names_the_runs_it_started_and_releases_both_locks() -> None:
    port = InterruptedAtStart(_keyboard_interrupt)
    lock = InMemoryStateStore()

    with pytest.raises(EvalRunsInterrupted) as raised:
        gate(port, state_store=lock, store=captured_store(compile_eval()))

    assert raised.value.run_names == ("EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z",)
    assert not lock.locked and port.run_locks.rows == {}


def test_sigterm_interrupts_the_run_as_ctrl_c_does_so_both_locks_are_released() -> None:
    port = InterruptedAtStart(lambda: signal.raise_signal(signal.SIGTERM))
    lock = InMemoryStateStore()
    before = signal.getsignal(signal.SIGTERM)

    with pytest.raises(EvalRunsInterrupted), terminated_as_interrupt():
        gate(port, state_store=lock, store=captured_store(compile_eval()))

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
