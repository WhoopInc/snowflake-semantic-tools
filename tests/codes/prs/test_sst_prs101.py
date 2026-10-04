"""SST-PRS101: an eval config enables no system metric and names no custom metric."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs101_fires() -> None:
    config = replace(VALUE.config, system_metrics=(), custom_metric_names=())
    [diagnostic] = eval_catalog_findings("SST-PRS101", replace(VALUE, config=config), ())
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/evals/config.yml: 'metrics' is present and empty"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs101_silent() -> None:
    assert eval_catalog_findings("SST-PRS101") == []
