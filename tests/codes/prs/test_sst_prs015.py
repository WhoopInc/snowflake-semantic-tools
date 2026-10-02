"""SST-PRS015: an eval ground truth gives `immutable_reason` without `immutable: true`."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalGroundTruth,
    EvalQuestion,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval

VALUE = resolved_eval()


def _found(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def _with(ground_truth: EvalGroundTruth) -> ResolvedEval:
    question = EvalQuestion(Origin("dataset.yml"), "What sold most?", ground_truth)
    return replace(VALUE, dataset=replace(VALUE.dataset, questions=(question,)))


def test_sst_prs015_fires() -> None:
    truth = EvalGroundTruth(Origin("dataset.yml"), (), "Pizza", immutable=False, immutable_reason="frozen")
    [diagnostic] = _found("SST-PRS015", _with(truth))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("'questions[0].ground_truth.immutable_reason' requires 'immutable: true'")
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs015_silent() -> None:
    truth = EvalGroundTruth(Origin("dataset.yml"), (), "Pizza", immutable=True, immutable_reason="frozen")
    assert _found("SST-PRS015", _with(truth)) == []
