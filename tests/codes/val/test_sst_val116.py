"""SST-VAL116: two relationships join a metric's table to one table and the metric names neither.

A `prop` code: generated relationship sets show the rule fires for every pair two or more
relationships join, and never once the metric pins its path.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _multipath_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from tests.helpers.semantic_members import metric

VIEWS = (("semantic_view:v", frozenset(("orders", "customers"))),)
TOTAL = "SUM({{ ref('orders', 'total') }})"


def _joins(count: int) -> tuple[Relationship, ...]:
    return tuple(
        Relationship(f"ORDERS_TO_CUSTOMERS_{index}", "ORDERS", (f"KEY_{index}",), "CUSTOMERS", ("CUSTOMER_ID",))
        for index in range(count)
    )


def test_sst_val116_fires() -> None:
    [found] = [
        item
        for item in _multipath_diagnostics(_joins(2), (metric("total", TOTAL),), VIEWS)
        if item.code == "SST-VAL116"
    ]
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total' has 2 paths to 'customers' and declares no using_relationships"
    assert found.subject == "metric:total"


def test_sst_val116_silent() -> None:
    pinned = metric("total", TOTAL, using_relationships=("ORDERS_TO_CUSTOMERS_0",))
    assert [item for item in _multipath_diagnostics(_joins(2), (pinned,), VIEWS) if item.code == "SST-VAL116"] == []
    single = _multipath_diagnostics(_joins(1), (metric("total", TOTAL),), VIEWS)
    assert [item for item in single if item.code == "SST-VAL116"] == []


@given(count=st.integers(min_value=2, max_value=6), pin=st.booleans())
def test_sst_val116_fires_exactly_when_an_unpinned_metric_has_several_paths(count: int, pin: bool) -> None:
    pinned = ("ORDERS_TO_CUSTOMERS_0",) if pin else ()
    found = [
        item
        for item in _multipath_diagnostics(_joins(count), (metric("total", TOTAL, using_relationships=pinned),), VIEWS)
        if item.code == "SST-VAL116"
    ]
    assert [item.context["count"] for item in found] == ([] if pin else [count])
