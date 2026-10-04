"""SST-PRS013: a closed field, here an eval system metric's version, holds a value it does not allow."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs013_fires() -> None:
    metric = replace(VALUE.config.system_metrics[0], version="v2")
    [diagnostic] = eval_catalog_findings(
        "SST-PRS013", replace(VALUE, config=replace(VALUE.config, system_metrics=(metric,)))
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("is 'v2', expected one of v3")
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs013_silent() -> None:
    assert eval_catalog_findings("SST-PRS013") == []
