"""SST-VAL734: threshold gates before the baseline runs complete.

A baseline captured over two runs cannot gate an eval that now requires three; one that
holds every required run gates it.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import attempt, gated_eval, run_result

PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))
BASELINE = capture_baseline(
    gated_eval(),
    run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING)),
    reason="initial",
    captured_at="2026-09-01T00:00:00Z",
)


def requiring(runs: int) -> CompiledEval:
    compiled = gated_eval()
    run = compiled.resolved.config.run
    assert run is not None
    config = replace(compiled.resolved.config, run=replace(run, baseline_runs=runs))
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def test_sst_val734_fires() -> None:
    current = run_result(attempt("run-3", *PASSING))
    verdict, diagnostics = evaluate_gate(requiring(3), current, BASELINE, now="2026-09-02T00:00:00Z")
    diagnostic = only(diagnostics, "SST-VAL734")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval config for 'sales_agent': 2 baseline_runs completed, 3 required"
    assert diagnostic.subject == "eval:sales_agent"
    assert verdict.reason == "baseline_incomplete" and not verdict.passed


def test_sst_val734_silent() -> None:
    current = run_result(attempt("run-3", *PASSING))
    verdict, diagnostics = evaluate_gate(requiring(2), current, BASELINE, now="2026-09-02T00:00:00Z")
    assert "SST-VAL734" not in codes(diagnostics)
    assert verdict.passed
