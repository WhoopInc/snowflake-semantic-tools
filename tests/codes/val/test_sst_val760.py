"""SST-VAL760: eval baseline is nearing expiry.

The gate warns in the baseline's last week, and not before.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import attempt, gated_eval, run_result

PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))


def baseline() -> EvalBaselineRecord:
    runs = run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING))
    return capture_baseline(gated_eval(), runs, reason="initial", captured_at="2026-09-01T00:00:00Z")


def test_sst_val760_fires() -> None:
    current = run_result(attempt("run-3", *PASSING))
    _, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-25T00:00:00Z")
    diagnostic = only(diagnostics, "SST-VAL760")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval 'eval:sales_agent' baseline expires on 2026-10-01T00:00:00Z"


def test_sst_val760_silent() -> None:
    current = run_result(attempt("run-3", *PASSING))
    _, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-20T00:00:00Z")
    assert "SST-VAL760" not in codes(diagnostics)
