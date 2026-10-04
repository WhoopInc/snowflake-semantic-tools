"""SST-VAL218: the connected spot check finds a distinct range's rows overlapping."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Relationship, SemanticView, Table
from tests.helpers.sql_values import authored
from tests.helpers.val_codes import validated_live

VIEW = SemanticView(
    "DB.S.MENU",
    (
        Table("ORDERS", "DB.S.ORDERS"),
        Table("PERIODS", "DB.S.PERIODS", primary_key=("PERIOD_ID",), distinct_range=("STARTS_AT", "ENDS_AT")),
    ),
    metrics=(Metric("ROWS", authored("COUNT(1)"), "ORDERS"),),
    relationships=(
        Relationship(
            "ORDERS_TO_PERIODS",
            "ORDERS",
            ("ORDERED_AT",),
            "PERIODS",
            ("STARTS_AT",),
            range_bounds=("STARTS_AT", "ENDS_AT"),
        ),
    ),
)


def test_sst_val218_fires() -> None:
    [found] = [item for item in validated_live(VIEW, {" JOIN ": (1, 5, 3, 9)}) if item.code == "SST-VAL218"]
    assert found.severity is Severity.ERROR
    assert found.message == (
        "relationship 'orders_to_periods': 'PERIODS' declares distinct_range over (STARTS_AT, ENDS_AT) "
        "but the ranges overlap, for example [1, 5) and [3, 9)"
    )
    assert found.subject == "semantic_view:menu"


def test_sst_val218_silent() -> None:
    assert [item for item in validated_live(VIEW, {}) if item.code == "SST-VAL218"] == []
