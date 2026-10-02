"""SST-VAL746: system judge versions move on Snowflake's cadence.

Capturing a baseline through the eval suite records the version each system metric's judge
came with; gating a run against that baseline records nothing new.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.gate import capture_baseline, recorded_judges
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_builders import attempt, gated_eval, run_result
from tests.helpers.eval_inputs import codes, only

PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))


def test_sst_val746_fires() -> None:
    compiled = gated_eval()
    baseline = capture_baseline(
        compiled,
        run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING)),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )
    diagnostic = only(recorded_judges(compiled), "SST-VAL746")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "eval metric 'answer_correctness': judge for version 'v3' recorded"
    assert diagnostic.subject == "eval:sales_agent"
    assert ("answer_correctness", "v3") in baseline.metric_versions


def test_sst_val746_silent() -> None:
    # A custom metric chooses its judge directly, so an eval with only custom metrics records none.
    compiled = gated_eval()
    config = replace(compiled.resolved.config, system_metrics=())
    custom_only = replace(compiled, resolved=replace(compiled.resolved, config=config))
    assert "SST-VAL746" not in codes(recorded_judges(custom_only))
