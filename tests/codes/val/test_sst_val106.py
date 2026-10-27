"""SST-VAL106: a table-scoped metric references a derived metric.

A `prop` code: generated metrics show the rule holds across the inputs it is about.
"""

from __future__ import annotations

from hypothesis import given

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings
from tests.helpers.semantic_strategies import base_metrics, combination


def test_sst_val106_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    derived = metric("doubled", "{{ metric('total') }} * 2", derived=True)
    regular = metric("uses_derived", "SUM({{ ref('orders', 'cost') }}) + {{ metric('doubled') }}")
    [found] = metric_findings(base, derived, regular, code="SST-VAL106")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "metric 'uses_derived' is table-scoped and references derived metric 'doubled'"
    assert found.subject == "metric:uses_derived"


def test_sst_val106_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert (
        metric_findings(
            base, metric("uses_base", "SUM({{ ref('orders', 'cost') }}) + {{ metric('total') }}"), code="SST-VAL106"
        )
        == []
    )


@given(expression=combination())
def test_sst_val106_fires_whatever_the_derived_metric_computes(expression: str) -> None:
    derived = metric("derived_one", expression, derived=True)
    regular = metric("uses_derived", "SUM({{ ref('orders', 'cost') }}) + {{ metric('derived_one') }}")
    [found] = metric_findings(*base_metrics(), derived, regular, code="SST-VAL106")
    assert found.subject == "metric:uses_derived"
