"""SST-VAL003: a describable object has no description."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.semantic.dbt import description_diagnostics
from tests.helpers.semantic_members import metric

TOTAL = "SUM({{ ref('orders', 'total') }})"


def test_sst_val003_fires() -> None:
    [found] = description_diagnostics((), (dataclasses.replace(metric("total", TOTAL), description=None),))
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'total' has no description"
    assert found.subject == "metric:total"


def test_sst_val003_silent() -> None:
    assert description_diagnostics((), (metric("total", TOTAL),)) == ()
