"""SST-VAL312: a model a view uses declares neither primary_key nor unique_keys."""

from __future__ import annotations

import dataclasses
from typing import Any

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _dbt_model_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.semantic_members import ORDERS


def _found(code: str, **changes: Any) -> list[Diagnostic]:
    model = dataclasses.replace(ORDERS, **changes)
    return [item for item in _dbt_model_diagnostics({"orders": model}) if item.code == code]


def test_sst_val312_fires() -> None:
    [found] = _found("SST-VAL312", primary_key=())
    assert found.severity is Severity.WARNING
    assert found.message == "dbt_model:orders: 'orders' declares neither primary_key nor unique_keys"
    assert found.subject == "dbt_model:orders"


def test_sst_val312_silent() -> None:
    assert _found("SST-VAL312", primary_key=(), unique_keys=(("customer_id", "ordered_at"),)) == []
