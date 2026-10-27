"""SST-VAL001: two members of one type share a name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.semantic_members import metric, metric_findings

TOTAL = "SUM({{ ref('orders', 'total') }})"


def test_sst_val001_fires() -> None:
    [found] = metric_findings(
        metric("total", TOTAL), metric("Total", "SUM({{ ref('orders', 'cost') }})"), code="SST-VAL001"
    )
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'total' is declared more than once"
    assert found.subject == "metric:total"


def test_sst_val001_names_every_declaration() -> None:
    # GitHub #270: the finding named neither declaration.
    first, second = Origin("metrics/total.yml", 2, 3), Origin("metrics/Total.yml", 2, 3)
    [found] = metric_findings(
        metric("total", TOTAL, origin=first),
        metric("Total", "SUM({{ ref('orders', 'cost') }})", origin=second),
        code="SST-VAL001",
    )
    assert (found.origin, found.related) == (first, (second,))


def test_sst_val001_silent() -> None:
    assert (
        metric_findings(metric("total", TOTAL), metric("cost", "SUM({{ ref('orders', 'cost') }})"), code="SST-VAL001")
        == []
    )
