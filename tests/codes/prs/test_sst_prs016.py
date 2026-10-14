"""SST-PRS016: a numeric field, here an eval's `concurrency` setting, is outside its range."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import settled
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs016_fires() -> None:
    [diagnostic] = eval_catalog_findings("SST-PRS016", settled(VALUE, concurrency=0))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/evals/config.yml: 'config.concurrency' is 0, expected >= 1"
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs016_silent() -> None:
    assert eval_catalog_findings("SST-PRS016", settled(VALUE, concurrency=1)) == []
