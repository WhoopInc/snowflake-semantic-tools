"""SST-VAL108: a derived metric carries a table list."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric, metric_findings


def test_sst_val108_fires() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    derived = dataclasses.replace(
        metric("doubled", "{{ metric('total') }} * 2", derived=True), tables=("orders",), has_tables_key=True
    )
    [found] = metric_findings(base, derived, code="SST-VAL108")
    assert found.severity is Severity.ERROR
    assert found.message == "derived metric 'doubled' declares tables:"
    assert found.subject == "metric:doubled"


def test_sst_val108_silent() -> None:
    base = metric("total", "SUM({{ ref('orders', 'total') }})")
    assert metric_findings(base, metric("doubled", "{{ metric('total') }} * 2", derived=True), code="SST-VAL108") == []
