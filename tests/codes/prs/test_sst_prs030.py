"""SST-PRS030: a synonym holds a character it may not, or enrich refused a proposed one."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.enrich import rejected_synonyms


def test_sst_prs030_fires() -> None:
    [diagnostic] = rejected_synonyms(["gross\ntotal"], artifact="orders.amount", subject="dbt_model:orders", limit=4)
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRS030", Severity.WARNING)
    assert diagnostic.message == "orders.amount: synonym 'gross\\u000atotal' contains control characters"
    assert diagnostic.subject == "dbt_model:orders"


def test_sst_prs030_silent() -> None:
    assert rejected_synonyms(["gross total"], artifact="orders.amount", subject="dbt_model:orders", limit=4) == ()
