"""SST-MEM006: a derived metric declares `tables:`, though derived members are view-scoped."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem006_fires() -> None:
    [diagnostic, *_] = coded(
        membership(member("metric", "m", ("orders",)), facts={"metric:m": MemberFacts(derived=True)}).diagnostics,
        "SST-MEM006",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:m is derived and declares tables:"
    assert diagnostic.subject == "metric:m"


def test_sst_mem006_silent() -> None:
    assert (
        coded(
            membership(member("metric", "m", None), facts={"metric:m": MemberFacts(derived=True)}).diagnostics,
            "SST-MEM006",
        )
        == []
    )
