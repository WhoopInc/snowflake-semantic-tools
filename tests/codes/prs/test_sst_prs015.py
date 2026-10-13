"""SST-PRS015: an eval ground truth gives `immutable_reason` without `immutable: true`."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalGroundTruth,
    EvalQuestion,
    ResolvedEval,
)
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def _with(ground_truth: EvalGroundTruth) -> ResolvedEval:
    question = EvalQuestion(Origin("dataset.yml"), "What sold most?", ground_truth)
    return replace(VALUE, dataset=replace(VALUE.dataset, questions=(question,)))


def test_sst_prs015_fires() -> None:
    truth = EvalGroundTruth(Origin("dataset.yml"), (), "Pizza", immutable=False, immutable_reason="frozen")
    [diagnostic] = eval_catalog_findings("SST-PRS015", _with(truth))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("'questions[0].ground_truth.immutable_reason' requires 'immutable: true'")
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs015_silent() -> None:
    truth = EvalGroundTruth(Origin("dataset.yml"), (), "Pizza", immutable=True, immutable_reason="frozen")
    assert eval_catalog_findings("SST-PRS015", _with(truth)) == []


def _output_findings(output: str) -> list[str]:
    truth = EvalGroundTruth(Origin("dataset.yml"), (), output)
    return [item.context["field"] for item in eval_catalog_findings("SST-PRS015", _with(truth))]


def test_sst_prs015_silent_for_a_date_that_scopes_the_expectation() -> None:
    # The evals guide requires absolute dates, so a date written out is not a figure that drifts.
    for output in (
        "It covers customers whose first order was in 2024.",
        "It compares Q3 2024 with H1 2024 and with fiscal year 2023.",
        "It reports the month ending 2024-03-31, then 2024-04.",
        "It starts on March 5, 2024 and covers all of April 2024.",
        "It covers every order between 2023 and 2024, and from 2021 to 2022.",
    ):
        assert _output_findings(output) == [], output


def test_sst_prs015_fires_for_a_figure_beside_a_date_or_shaped_like_a_year() -> None:
    for output in ("It reports 12 orders in 2024.", "It reports 2000 orders.", "Revenue rose 5% since 2023."):
        assert _output_findings(output) == ["questions[0].ground_truth.ground_truth_output"], output
