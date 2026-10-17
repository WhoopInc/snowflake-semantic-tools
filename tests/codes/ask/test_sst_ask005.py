"""SST-ASK005: an unpublished eval run's config enables a metric only the evaluation service scores.

Fires for `logical_consistency`; `answer_correctness`, which the run judges, stays quiet.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.inline import RunInlineEvals
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import EvalSystemMetric, ResolvedEval
from tests.helpers.ask_builders import inline_setup
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _with_metric(name: str) -> ResolvedEval:
    base = resolved_eval()
    metric = EvalSystemMetric(Origin("config.yml", 1, 1), name, "v3")
    return replace(base, config=replace(base.config, system_metrics=(metric,)))


def test_sst_ask005_fires() -> None:
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(_with_metric("logical_consistency")),))
    diagnostic = only(outcome.diagnostics, "SST-ASK005")
    assert diagnostic.severity is Severity.INFO
    assert (
        diagnostic.message == "eval 'sales_agent': system metric 'logical_consistency' is not scored without publishing"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_ask005_silent() -> None:
    outcome = RunInlineEvals(FakeSnowflake()).run((inline_setup(_with_metric("answer_correctness")),))
    assert "SST-ASK005" not in codes(outcome.diagnostics)
