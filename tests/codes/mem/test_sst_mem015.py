"""SST-MEM015: a verified query references a private metric, which cannot be selected from outside its view."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def _flagged(private: bool) -> list[Diagnostic]:
    """The SST-MEM015 findings for verified query `q`, which references metric `revenue`."""
    revenue = member("metric", "revenue", ("orders",))
    query = member("verified_query", "q", ("orders",), "SELECT {{ metric('revenue') }} FROM orders")
    facts = {
        "metric:revenue": MemberFacts(private=private),
        "verified_query:q": MemberFacts(referenced_metrics=("revenue",)),
    }
    return coded(membership(revenue, query, facts=facts).diagnostics, "SST-MEM015")


def test_sst_mem015_fires() -> None:
    [diagnostic] = _flagged(private=True)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "metric:revenue is private and is referenced by verified_query:q"
    assert diagnostic.subject == "verified_query:q"


def test_sst_mem015_silent() -> None:
    assert _flagged(private=False) == []
