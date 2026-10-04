"""SST-MEM007: a composed metric references a metric whose tables its own `tables:` leaves out."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership

COMPOSED = MemberFacts(referenced_metrics=("base",))


def test_sst_mem007_fires() -> None:
    composed = member("metric", "composed", ("orders",), "{{ metric('base') }} + 1")
    base = member("metric", "base", ("customers",))
    result = membership(composed, base, facts={"metric:composed": COMPOSED})
    [diagnostic] = coded(result.diagnostics, "SST-MEM007")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:composed: transitive tables customers are not in its own tables:"
    assert diagnostic.subject == "metric:composed"


def test_sst_mem007_silent() -> None:
    composed = member("metric", "composed", ("orders",), "{{ metric('base') }} + 1")
    base = member("metric", "base", ("orders",))
    assert coded(membership(composed, base, facts={"metric:composed": COMPOSED}).diagnostics, "SST-MEM007") == []
