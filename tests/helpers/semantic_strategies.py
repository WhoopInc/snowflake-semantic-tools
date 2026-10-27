"""Hypothesis strategies for the metric restrictions Snowflake does not enforce."""

from __future__ import annotations

from hypothesis import strategies as st

from snowflake_semantic_tools.domain.model.authored import MetricDef
from tests.helpers.semantic_members import metric

# The derived-metric restrictions, SST-VAL102 to SST-VAL107.
CRITICAL = ("SST-VAL102", "SST-VAL103", "SST-VAL104", "SST-VAL105", "SST-VAL106", "SST-VAL107")
WINDOW_FUNCTIONS = ("LAG", "LEAD", "ROW_NUMBER", "RANK", "DENSE_RANK", "FIRST_VALUE", "LAST_VALUE", "NTILE")
AGGREGATES = ("SUM", "AVG", "MIN", "MAX", "COUNT", "MEDIAN")
BASES = ("total", "cost", "orders_placed")


def base_metrics() -> tuple[MetricDef, ...]:
    """Three table-scoped, additive metrics a derived metric may build on."""
    return (
        metric("total", "SUM({{ ref('orders', 'total') }})"),
        metric("cost", "SUM({{ ref('orders', 'cost') }})"),
        metric("orders_placed", "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"),
    )


@st.composite
def combination(draw: st.DrawFn) -> str:
    """A derived expression combining base metrics with arithmetic and DIV0, and no window."""
    names = draw(st.lists(st.sampled_from(BASES), min_size=1, max_size=4))
    terms = [f"{{{{ metric('{name}') }}}}" for name in names]
    expression = terms[0]
    for term in terms[1:]:
        operator = draw(st.sampled_from(("+", "-", "*", "DIV0")))
        expression = f"DIV0({expression}, {term})" if operator == "DIV0" else f"({expression} {operator} {term})"
    return expression
