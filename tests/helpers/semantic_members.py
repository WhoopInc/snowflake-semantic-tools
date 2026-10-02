"""Small metrics, models and relationships for the tests that call the semantic checks directly."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.adapters.yaml.semantic.checks.metrics import _metric_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel


def column(name: str, data_type: str, column_type: str | None, **fields: Any) -> DbtColumn:
    """One described column."""
    return DbtColumn(name, f"The {name}.", data_type, column_type, **fields)


ORDERS = DbtModel(
    "model.t.orders",
    "orders",
    "DB.S.ORDERS",
    ("order_id",),
    (),
    (
        column("order_id", "NUMBER", "dimension"),
        column("customer_id", "NUMBER", "dimension"),
        column("ordered_at", "TIMESTAMP_NTZ", "time_dimension"),
        column("state", "VARCHAR", "dimension"),
        column("total", "NUMBER", "fact"),
        column("cost", "NUMBER", "fact"),
        column("secret", "NUMBER", "fact", excluded=True),
    ),
    has_contract=True,
)
BALANCES = DbtModel(
    "model.t.balances",
    "balances",
    "DB.S.BALANCES",
    ("account_id", "as_of"),
    (),
    (
        column("account_id", "NUMBER", "dimension"),
        column("as_of", "DATE", "time_dimension"),
        column("balance", "NUMBER", "fact"),
    ),
    has_contract=True,
)
MODELS = {"orders": ORDERS, "balances": BALANCES}


def metric(name: str, expr: str, *, tables: tuple[str, ...] = ("orders",), **changes: Any) -> MetricDef:
    """A described table-scoped metric over `tables`, or a derived one when `derived=True`."""
    derived = bool(changes.get("derived"))
    return MetricDef(
        name,
        expr,
        f"The {name}.",
        (),
        () if derived else tables,
        has_tables_key=not derived and bool(tables),
        **changes,
    )


def metric_findings(*metrics: MetricDef, code: str) -> list[Diagnostic]:
    """What the metric checks report under `code`, against `MODELS`."""
    return [item for item in _metric_diagnostics(metrics, dict(MODELS), {}) if item.code == code]
