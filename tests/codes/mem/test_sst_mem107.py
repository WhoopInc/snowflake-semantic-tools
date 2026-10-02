"""SST-MEM107: attachment skipped a poisoned member whose references did not resolve."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, member, membership


def test_sst_mem107_fires() -> None:
    [diagnostic, *_] = coded(
        membership(member("metric", "m", ("orders",), poisoned=True), unresolved={"metric:m": 2}).diagnostics,
        "SST-MEM107",
    )
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "metric:m skipped: 2 unresolved references"
    assert diagnostic.subject == "metric:m"


def test_sst_mem107_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",), poisoned=True)).diagnostics, "SST-MEM107") == []
