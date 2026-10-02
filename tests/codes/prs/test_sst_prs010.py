"""SST-PRS010: a rendered eval source table name is longer than Snowflake's object name limit."""

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


def test_sst_prs010_fires() -> None:
    assert VALUE.config.dataset is not None
    dataset = replace(VALUE.config.dataset, source_table_template="SRC_" + "X" * 130)
    [diagnostic] = _found("SST-PRS010", replace(VALUE, config=replace(VALUE.config, dataset=dataset)))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"'SRC_{'X' * 130}' is 134 chars, over the 128 limit"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs010_silent() -> None:
    assert _found("SST-PRS010") == []
