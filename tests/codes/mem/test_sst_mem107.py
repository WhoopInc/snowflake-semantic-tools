"""SST-MEM107: attachment skipped a poisoned member whose references did not resolve."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem107_fires() -> None:
    [diagnostic, *_] = coded(
        membership(
            member("metric", "m", ("orders",), poisoned=True), unresolved={"metric:m": ("SST-REF001", "SST-REF002")}
        ).diagnostics,
        "SST-MEM107",
    )
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "metric:m skipped: 2 unresolved references"
    assert diagnostic.subject == "metric:m"
    # The skip follows from the first unresolved reference, so it is that one's cascade.
    assert diagnostic.caused_by == "SST-REF001" and diagnostic.cascaded


def test_sst_mem107_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",), poisoned=True)).diagnostics, "SST-MEM107") == []
