"""SST-VAL759: eval baseline is incompatible.

The current run resolved another agent version than the baseline captured, and the message says so.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.gate import evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.val_codes import PASSING, baseline


def test_sst_val759_fires() -> None:
    current = run_result(attempt("run-3", *PASSING, agent_version="VERSION$2"))
    verdict, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-02T00:00:00Z")
    diagnostic = only(diagnostics, "SST-VAL759")
    assert diagnostic.severity is Severity.ERROR
    # The message names what differs: here only the agent version.
    assert diagnostic.message == (
        "eval 'eval:sales_agent' baseline is incompatible: it was captured on VERSION$1, and this run uses VERSION$2"
    )
    assert verdict.reason == "baseline_incompatible"


def test_sst_val759_silent() -> None:
    current = run_result(attempt("run-3", *PASSING))
    _, diagnostics = evaluate_gate(gated_eval(), current, baseline(), now="2026-09-02T00:00:00Z")
    assert "SST-VAL759" not in codes(diagnostics)
