"""SST-VAL745: in-place edit to a metric a retained run references.

A baseline records each custom metric's definition; editing the prompt under the same name
is reported by name against the run that scored the old one. An unchanged metric still gates.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.eval_inputs import codes, only

PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))
BASELINE = capture_baseline(
    gated_eval(),
    run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING)),
    reason="initial",
    captured_at="2026-09-01T00:00:00Z",
)


def with_prompt(prompt: str) -> CompiledEval:
    compiled = gated_eval()
    [metric] = compiled.resolved.custom_metrics
    return replace(compiled, resolved=replace(compiled.resolved, custom_metrics=(replace(metric, prompt=prompt),)))


def test_sst_val745_fires() -> None:
    edited = with_prompt("Score from 0 to 5, strictly. If tied, choose the lower score.")
    verdict, diagnostics = evaluate_gate(edited, run_result(attempt("run-3", *PASSING)), BASELINE, now="2026-09-02Z")
    diagnostic = only(diagnostics, "SST-VAL745")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'grounding' is referenced by retained run 'run-2'"
    assert diagnostic.subject == "eval_metric:grounding"
    assert "SST-VAL759" not in codes(diagnostics) and not verdict.passed


def test_sst_val745_silent() -> None:
    unchanged = with_prompt("Score from 0 to 5. If tied, choose the lower score.")
    verdict, diagnostics = evaluate_gate(unchanged, run_result(attempt("run-3", *PASSING)), BASELINE, now="2026-09-02Z")
    assert "SST-VAL745" not in codes(diagnostics)
    assert verdict.passed
