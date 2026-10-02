"""SST-VAL003: a describable object has no description."""

from __future__ import annotations

import dataclasses

from snowflake_semantic_tools.adapters.yaml.semantic.checks.dbt import _description_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_members import metric

TOTAL = "SUM({{ ref('orders', 'total') }})"


def test_sst_val003_fires() -> None:
    [found] = _description_diagnostics((), (dataclasses.replace(metric("total", TOTAL), description=None),))
    assert found.severity is Severity.WARNING
    assert found.message == "metric 'total' has no description"
    assert found.subject == "metric:total"


def test_sst_val003_silent() -> None:
    assert _description_diagnostics((), (metric("total", TOTAL),)) == ()
