"""SST-VAL758: eval baseline is absent.

The gate finds no captured baseline for the eval; with one it compares the run.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.eval_inputs import codes, only

PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))


def baseline() -> EvalBaselineRecord:
    runs = run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING))
    return capture_baseline(gated_eval(), runs, reason="initial", captured_at="2026-09-01T00:00:00Z")


def test_sst_val758_fires() -> None:
    _, diagnostics = evaluate_gate(
        gated_eval(), run_result(attempt("run-3", *PASSING)), None, now="2026-09-02T00:00:00Z"
    )
    diagnostic = only(diagnostics, "SST-VAL758")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval 'eval:sales_agent' has no captured baseline"


def test_sst_val758_silent() -> None:
    current = run_result(attempt("run-3", *PASSING))
    _, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-02T00:00:00Z")
    assert "SST-VAL758" not in codes(diagnostics)
