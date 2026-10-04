"""SST-VAL312: a model a view uses declares neither primary_key nor unique_keys."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.val_codes import dbt_model_findings


def test_sst_val312_fires() -> None:
    [found] = dbt_model_findings("SST-VAL312", primary_key=())
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:orders: 'orders' declares neither primary_key nor unique_keys"
    assert found.subject == "dbt_model:orders"


def test_sst_val312_silent() -> None:
    assert dbt_model_findings("SST-VAL312", primary_key=(), unique_keys=(("customer_id", "ordered_at"),)) == []
