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
