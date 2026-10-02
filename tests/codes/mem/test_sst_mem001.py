"""SST-MEM001: a member declares a table that is a dbt model and that no view lists."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, member, membership


def test_sst_mem001_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("suppliers",))).diagnostics, "SST-MEM001")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:m declares table 'suppliers', which no semantic_view lists"
    assert diagnostic.subject == "metric:m"


def test_sst_mem001_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",))).diagnostics, "SST-MEM001") == []
