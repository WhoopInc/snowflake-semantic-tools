"""SST-MEM016: a metric's join path is attached to one of its views and missing from another."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts
from tests.helpers.resolve_builders import coded, member, membership

JOINED = MemberFacts(using_relationships=("orders_to_customers",))
RELATIONSHIP = member("relationship", "orders_to_customers", ("orders", "customers"))


def test_sst_mem016_fires() -> None:
    # The metric needs only orders, so it lands in both views; its join reaches customers,
    # which only the sales view holds.
    metric = member("metric", "m", ("orders",))
    [diagnostic] = coded(membership(metric, RELATIONSHIP, facts={"metric:m": JOINED}).diagnostics, "SST-MEM016")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "metric:m attaches to semantic_view:sales and semantic_view:menu with conflicting scope"
    )
    assert diagnostic.subject == "metric:m"


def test_sst_mem016_silent() -> None:
    metric = member("metric", "m", ("orders", "customers"))
    assert coded(membership(metric, RELATIONSHIP, facts={"metric:m": JOINED}).diagnostics, "SST-MEM016") == []
