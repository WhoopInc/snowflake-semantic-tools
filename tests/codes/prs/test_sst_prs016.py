"""SST-PRS016: a numeric field, here an eval run's `concurrency`, is outside its range."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalRunConfig
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs016_fires() -> None:
    run = EvalRunConfig(label="ci", concurrency=0)
    [diagnostic] = eval_catalog_findings("SST-PRS016", replace(VALUE, config=replace(VALUE.config, run=run)))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/evals/config.yml: 'run.concurrency' is 0, expected >= 1"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs016_silent() -> None:
    assert (
        eval_catalog_findings(
            "SST-PRS016", replace(VALUE, config=replace(VALUE.config, run=EvalRunConfig(label="ci", concurrency=1)))
        )
        == []
    )
