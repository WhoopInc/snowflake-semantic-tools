"""SST-MEM011: a member's implicit attachment reaches more than one view."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem011_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("orders",))).diagnostics, "SST-MEM011")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "metric:m attaches to 2 artifacts"
    assert diagnostic.subject == "metric:m"


def test_sst_mem011_silent() -> None:
    assert coded(membership(member("metric", "m", ("products",))).diagnostics, "SST-MEM011") == []
