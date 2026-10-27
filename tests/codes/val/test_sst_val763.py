"""SST-VAL763: blocking eval regressed.

A gated metric passed a question in every baseline attempt and failed it now.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.gate import evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.val_codes import baseline


def test_sst_val763_fires() -> None:
    current = run_result(attempt("run-3", ("q", "answer_correctness", False), ("q", "grounding", True)))
    verdict, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-02T00:00:00Z")
    diagnostic = only(diagnostics, "SST-VAL763")
    assert diagnostic.severity is Severity.ERROR and not ERROR_REGISTRY["SST-VAL763"].demotable
    assert diagnostic.message == "eval 'eval:sales_agent' regressed on 1 question/metric pair(s): answer_correctness"
    assert not verdict.passed


def test_sst_val763_silent() -> None:
    # The ungated custom metric failing is not a regression.
    current = run_result(attempt("run-3", ("q", "answer_correctness", True), ("q", "grounding", False)))
    verdict, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-02T00:00:00Z")
    assert "SST-VAL763" not in codes(diagnostics)
    assert verdict.passed
