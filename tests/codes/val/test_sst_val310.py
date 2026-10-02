"""SST-VAL310: a column a model names as a key is not on the model."""

from __future__ import annotations

import dataclasses
from typing import Any

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_model_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.semantic_members import ORDERS


def _found(code: str, **changes: Any) -> list[Diagnostic]:
    model = dataclasses.replace(ORDERS, **changes)
    return [item for item in _dbt_model_diagnostics({"orders": model}) if item.code == code]


def test_sst_val310_fires() -> None:
    [found] = _found("SST-VAL310", primary_key=("order_number",))
    assert found.severity is Severity.ERROR
    assert found.message == "dbt_model:orders: primary_key names 'order_number', absent from 'orders'"
    assert found.subject == "dbt_model:orders"


def test_sst_val310_silent() -> None:
    assert _found("SST-VAL310") == []
