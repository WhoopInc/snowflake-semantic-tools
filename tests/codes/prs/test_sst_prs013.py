"""SST-PRS013: a closed field, here an eval system metric's version, holds a value it does not allow."""

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


def test_sst_prs013_fires() -> None:
    metric = replace(VALUE.config.system_metrics[0], version="v2")
    [diagnostic] = _found("SST-PRS013", replace(VALUE, config=replace(VALUE.config, system_metrics=(metric,))))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("is 'v2', expected one of v3")
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs013_silent() -> None:
    assert _found("SST-PRS013") == []
