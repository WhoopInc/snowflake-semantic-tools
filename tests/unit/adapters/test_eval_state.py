from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.adapters.snowflake.eval_state import (
    InMemoryEvalStateStore,
    _baseline_from_payload,
    _baseline_payload,
    _gate_from_payload,
    _gate_payload,
)
from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalGateState,
    EvalRegression,
)


def baseline() -> EvalBaselineRecord:
    return EvalBaselineRecord(
        "eval:a",
        "d" * 64,
        "c" * 64,
        "LAST",
        (("answer_correctness", "v3"),),
        (EvalBaselineMetric("q", "answer_correctness", (True, False), (0.8, None)),),
        ("run-1", "run-2"),
        "2026-09-01T00:00:00Z",
        "2026-10-01T00:00:00Z",
        "initial",
    )


def gate() -> EvalGateState:
    return EvalGateState(
        "eval:a",
        "blocking",
        1,
        (EvalRegression("q", "answer_correctness"),),
        True,
        "2026-09-10T00:00:00Z",
        ("run-3",),
    )


def test_eval_state_payloads_round_trip_without_raw_eval_data() -> None:
    baseline_payload = _baseline_payload(baseline())
    gate_payload = _gate_payload(gate())

    assert _baseline_from_payload(baseline_payload) == baseline()
    assert _gate_from_payload(gate_payload) == gate()
    document = repr((baseline_payload, gate_payload))
    assert "input_query" not in document
    assert "ground_truth" not in document
    assert "trace" not in document


def test_in_memory_eval_state_store_is_target_scoped() -> None:
    store = InMemoryEvalStateStore()
    store.write_baseline("dev", baseline())
    store.write_gate("dev", gate())

    assert store.read_baseline("dev", "eval:a") == baseline()
    assert store.read_gate("dev", "eval:a") == gate()
    assert store.read_baseline("prod", "eval:a") is None
    assert store.read_gate("prod", "eval:a") is None


def test_in_memory_batch_baseline_write_publishes_all_records() -> None:
    store = InMemoryEvalStateStore()
    second = replace(baseline(), eval_key="eval:b")

    store.write_baselines("dev", (baseline(), second))

    assert store.read_baseline("dev", "eval:a") == baseline()
    assert store.read_baseline("dev", "eval:b") == second
