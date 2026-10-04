"""SST-MEM005: a member attaches to no view at all."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem005_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("suppliers",))).diagnostics, "SST-MEM005")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:m attaches to no semantic_view"
    assert diagnostic.subject == "metric:m"


def test_sst_mem005_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",))).diagnostics, "SST-MEM005") == []
