"""SST-VAL223: a column appears in both a model's primary_key and its unique_keys."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_model_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.semantic_members import ORDERS


def _found(unique_keys: tuple[tuple[str, ...], ...]) -> list[Diagnostic]:
    model = dataclasses.replace(ORDERS, unique_keys=unique_keys)
    return [item for item in _dbt_model_diagnostics({"orders": model}) if item.code == "SST-VAL223"]


def test_sst_val223_fires() -> None:
    [found] = _found((("order_id",),))
    assert found.severity is Severity.ERROR
    assert (
        found.message == "dbt_model:orders: column 'order_id' on 'orders' appears in both primary_key and unique_keys"
    )
    assert found.subject == "dbt_model:orders"


def test_sst_val223_silent() -> None:
    assert _found((("customer_id", "ordered_at"),)) == []
