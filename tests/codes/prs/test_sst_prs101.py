"""SST-PRS101: an eval config enables no system metric and names no custom metric."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.eval import CustomEvalMetric, EvalCatalog, ResolvedEval
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.eval_builders import resolved_eval

VALUE = resolved_eval()


def _found(
    code: str, value: ResolvedEval = VALUE, metrics: tuple[CustomEvalMetric, ...] | None = None
) -> list[Diagnostic]:
    custom = value.custom_metrics if metrics is None else metrics
    catalog = EvalCatalog((value,), custom)
    return [item for item in validate_eval_catalog(catalog, allowed_models=("claude-sonnet-4-6",)) if item.code == code]


def test_sst_prs101_fires() -> None:
    config = replace(VALUE.config, system_metrics=(), custom_metric_names=())
    [diagnostic] = _found("SST-PRS101", replace(VALUE, config=config), ())
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/evals/config.yml: 'metrics' is present and empty"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs101_silent() -> None:
    assert _found("SST-PRS101") == []
