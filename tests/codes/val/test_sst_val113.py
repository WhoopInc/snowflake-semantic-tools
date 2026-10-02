"""SST-VAL113: a derived metric declares a join path."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val113_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    derived = metric("doubled", "{{ metric('total') }} * 2", derived=True, using_relationships=("ORDERS_TO_X",))
    [found] = metric_findings(base, derived, code="SST-VAL113")
    assert found.severity is Severity.ERROR
    assert found.message == "derived metric 'doubled' declares using_relationships"
    assert found.subject == "metric:doubled"


def test_sst_val113_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})", using_relationships=("ORDERS_TO_X",))
    assert metric_findings(base, code="SST-VAL113") == []
