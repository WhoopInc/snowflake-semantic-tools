"""SST-VAL761: eval baseline is expired.

From its expiry on, a baseline no longer gates, and the error cannot be demoted.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.gate import evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.val_codes import PASSING, baseline


def test_sst_val761_fires() -> None:
    current = run_result(attempt("run-3", *PASSING))
    verdict, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-10-01T00:00:00Z")
    diagnostic = only(diagnostics, "SST-VAL761")
    assert diagnostic.severity is Severity.ERROR and not ERROR_REGISTRY["SST-VAL761"].demotable
    assert diagnostic.message == "eval 'eval:sales_agent' baseline expired on 2026-10-01T00:00:00Z"
    assert verdict.reason == "baseline_expired"


def test_sst_val761_silent() -> None:
    current = run_result(attempt("run-3", *PASSING))
    _, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-30T23:59:59Z")
    assert "SST-VAL761" not in codes(diagnostics)
