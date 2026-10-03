from __future__ import annotations

import json
from dataclasses import replace

import pytest

from snowflake_semantic_tools.adapters.snowflake.eval_state import (
    SnowflakeEvalStateStore,
    _baseline_from_payload,
    _baseline_payload,
    _gate_payload,
)
from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineMetric,
    EvalBaselineRecord,
    EvalGateState,
    EvalRegression,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.recorded_snowflake import ReadOnlySnowflake, RecordedSnowflake, ScriptedSnowflake

TABLE = QualifiedName.parse("DB.S.SST_STATE_EVALS")


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
    assert gate_payload == {
        "eval_key": "eval:a",
        "tier": "blocking",
        "regression_count": 1,
        "regressions": [{"question_key": "q", "metric_name": "answer_correctness"}],
        "unresolved": True,
        "evaluated_at": "2026-09-10T00:00:00Z",
        "run_names": ["run-3"],
    }
    document = repr((baseline_payload, gate_payload))
    assert "input_query" not in document
    assert "ground_truth" not in document
    assert "trace" not in document


def test_in_memory_eval_state_store_is_target_scoped() -> None:
    store = InMemoryEvalStateStore()
    store.write_baselines("dev", (baseline(),))
    store.write_gate("dev", gate())

    assert store.read_baseline("dev", "eval:a") == baseline()
    assert store.gates == {("dev", "eval:a"): gate()}
    assert store.read_baseline("prod", "eval:a") is None


def test_in_memory_batch_baseline_write_publishes_all_records() -> None:
    store = InMemoryEvalStateStore()
    second = replace(baseline(), eval_key="eval:b")

    store.write_baselines("dev", (baseline(), second))

    assert store.read_baseline("dev", "eval:a") == baseline()
    assert store.read_baseline("dev", "eval:b") == second


def _payload_row(payload: dict[str, object]) -> QueryResult:
    # The driver returns an OBJECT column as JSON text.
    return QueryResult(("PAYLOAD",), ((json.dumps(payload),),))


def test_reading_eval_state_never_creates_the_table_so_a_read_only_role_can_read() -> None:
    recorded = ScriptedSnowflake(
        query_results=(_payload_row(_baseline_payload(baseline())),),
        existing=(TABLE.sql,),
    )
    store = SnowflakeEvalStateStore(ReadOnlySnowflake(recorded), TABLE)

    assert store.read_baseline("dev", "eval:a") == baseline()
    assert recorded.scripts == []
    assert [sql.split(" WHERE ")[0] for sql, _ in recorded.queries] == [f"SELECT PAYLOAD FROM {TABLE.sql}"]


def test_a_missing_eval_state_table_reads_as_no_baseline() -> None:
    recorded = RecordedSnowflake(existing=())
    store = SnowflakeEvalStateStore(ReadOnlySnowflake(recorded), TABLE)

    assert store.read_baseline("dev", "eval:a") is None
    assert (recorded.scripts, recorded.queries) == ([], [])


def test_writing_eval_state_still_ensures_the_table_first() -> None:
    port = ScriptedSnowflake()
    store = SnowflakeEvalStateStore(port, TABLE)

    store.write_gate("dev", gate())
    store.write_baselines("dev", (baseline(),))

    create = f"CREATE TABLE IF NOT EXISTS {TABLE.sql} ("
    assert [script[0].startswith(create) for script in port.scripts] == [True, False, True, False]
    assert port.scripts[1][0].startswith(f"MERGE INTO {TABLE.sql} AS target")
    assert port.scripts[3][0] == "BEGIN" and port.scripts[3][-1] == "COMMIT"
    with pytest.raises(SnowflakePortError, match="read-only"):
        SnowflakeEvalStateStore(ReadOnlySnowflake(port), TABLE).write_gate("dev", gate())
