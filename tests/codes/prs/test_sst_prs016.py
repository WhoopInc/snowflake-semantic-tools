"""SST-PRS016: a numeric field, here an eval run's `concurrency`, is outside its range."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalCatalog, EvalRunConfig, ResolvedEval
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval

VALUE = resolved_eval()


def _found(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def test_sst_prs016_fires() -> None:
    run = EvalRunConfig(label="ci", concurrency=0)
    [diagnostic] = _found("SST-PRS016", replace(VALUE, config=replace(VALUE.config, run=run)))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/evals/config.yml: 'run.concurrency' is 0, expected >= 1"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs016_silent() -> None:
    assert (
        _found("SST-PRS016", replace(VALUE, config=replace(VALUE.config, run=EvalRunConfig(label="ci", concurrency=1))))
        == []
    )
