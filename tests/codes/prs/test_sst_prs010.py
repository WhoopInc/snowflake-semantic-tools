"""SST-PRS010: a rendered eval source table name is longer than Snowflake's object name limit."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs010_fires() -> None:
    assert VALUE.config.dataset is not None
    dataset = replace(VALUE.config.dataset, source_table_template="SRC_" + "X" * 130)
    [diagnostic] = eval_catalog_findings("SST-PRS010", replace(VALUE, config=replace(VALUE.config, dataset=dataset)))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"'SRC_{'X' * 130}' is 134 chars, over the 128 limit"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs010_silent() -> None:
    assert eval_catalog_findings("SST-PRS010") == []
