"""SST-VAL310: a column a model names as a key is not on the model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.val_codes import dbt_model_findings


def test_sst_val310_fires() -> None:
    [found] = dbt_model_findings("SST-VAL310", primary_key=("order_number",))
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:orders: primary_key names 'order_number', absent from 'orders'"
    assert found.subject == "dbt_model:orders"


def test_sst_val310_silent() -> None:
    assert dbt_model_findings("SST-VAL310") == []
