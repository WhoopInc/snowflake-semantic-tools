"""`sst.compare/1`: a comparison written as the schema says, and read back exactly or refused."""

from __future__ import annotations

import json
from typing import Any

import pytest

from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, KNOWN, PENDING, PINNED, Amount, ModelCost
from snowflake_semantic_tools.domain.evaluate.compare_json import SCHEMA, CompareReport, report_from_json, report_json
from tests.helpers.compare_reports import compared, model, run

# The keys docs/design/compare-report-schema.md lists, at each level, in its order.
ROOT_KEYS = ["schema", "generated_at", "cost_read_at", "target", "run_id", "git_sha", "evals"]
EVAL_KEYS = [
    "eval",
    "kind",
    "subject",
    "dataset",
    "runs_per_model",
    "metrics",
    "models",
    "questions",
    "verdict",
    "notes",
]
MODEL_KEYS = [
    *("model", "role", "version", "alias", "lifecycle", "status", "error", "rank", "run_names", "metrics"),
    *("latency_ms", "tokens", "cost", "request_ids"),
]


def report() -> CompareReport:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0, 0.0, route=("DB.A.X",))),
        model("opus", CANDIDATE, run("o1", 1.0, 1.0, route=())),
        costs={"pinned": ModelCost(Amount(KNOWN, 0.4), Amount(PENDING), Amount(KNOWN, 12.0))},
    )
    return CompareReport("2026-10-22T14:03:11-04:00", None, "dev", "c7f2a19e", "703f69a", (evaluation,))


def document() -> dict[str, Any]:
    written: dict[str, Any] = json.loads(json.dumps(report_json(report())))
    return written


def test_a_report_writes_every_key_the_schema_lists_in_its_order() -> None:
    written = document()
    assert list(written) == ROOT_KEYS and written["schema"] == SCHEMA
    [evaluation] = written["evals"]
    assert list(evaluation) == EVAL_KEYS
    assert list(evaluation["subject"]) == ["name", "qualified_name", "released_version", "pinned_model"]
    assert evaluation["dataset"] == {"name": "EVAL_SALES", "version": "SST_ABC", "questions": 2}
    assert evaluation["metrics"] == [
        {"name": "answer_correctness", "gated": True, "threshold": 0.5},
        {"name": "logical_consistency", "gated": False, "threshold": None},
    ]
    pinned = evaluation["models"][0]
    assert list(pinned) == MODEL_KEYS
    assert pinned["metrics"]["answer_correctness"] == {"mean": 0.5, "min": 0.5, "max": 0.5, "passed": 1, "total": 2}
    assert pinned["latency_ms"] == {"mean": 150, "p95": 200}
    assert pinned["tokens"] == {"agent_input": 100, "agent_output": 20, "llm_calls": 3, "judge": 10}
    assert pinned["cost"] == {
        "agent_credits": {"state": "known", "value": 0.4},
        "sql_credits": {"state": "pending", "value": None},
        "judge_tokens": {"state": "known", "value": 12.0},
        "credits_per_question": {"state": "known", "value": 0.2},
    }
    question = evaluation["questions"][1]
    assert question == {
        "id": "q2",
        "text": "Question 2?",
        "results": {
            "pinned": {"passed": False, "score": 0.0, "reason": "why 2", "route": ["DB.A.X"]},
            "opus": {"passed": True, "score": 1.0, "reason": "why 2", "route": []},
        },
        "changes": {"opus": "fixes"},
    }
    assert list(evaluation["verdict"]) == ["kind", "model", "summary", "pin_suggestion", "excluded"]


def test_a_written_report_reads_back_and_writes_the_same_document() -> None:
    written = document()
    assert report_json(report_from_json(written)) == written


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (lambda doc: doc.update(schema="sst.compare/2"), "the report's schema is 'sst.compare/2', not 'sst.compare/1'"),
        (lambda doc: doc.pop("run_id"), "the report has no 'run_id'"),
        (lambda doc: doc.update(evals={}), "the report.evals is not a list"),
        (
            lambda doc: doc["evals"][0].update(runs_per_model=True),
            "the report.evals[0].runs_per_model is not an integer",
        ),
        (lambda doc: doc["evals"][0]["models"][0]["cost"]["sql_credits"].update(state="late"), "is 'late', not known"),
        (lambda doc: doc["evals"][0]["models"][0].update(request_ids=[1]), "request_ids is not a list of strings"),
        (lambda doc: doc["evals"][0]["models"][0]["latency_ms"].update(mean="1"), "latency_ms.mean is not an integer"),
        (lambda doc: doc["evals"][0]["metrics"][0].update(threshold="0.5"), "threshold is not a number"),
        (lambda doc: doc["evals"][0].update(subject=[]), "the report.evals[0].subject is not an object"),
    ],
)
def test_a_report_sst_did_not_write_is_refused_naming_the_key(edit: Any, message: str) -> None:
    written = document()
    edit(written)
    with pytest.raises(ValueError, match=message.replace("[", r"\[").replace("]", r"\]").replace(".", r"\.")):
        report_from_json(written)
