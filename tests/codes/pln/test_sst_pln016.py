"""SST-PLN016: the plan's per-type counts of creates, replaces, unchanged artifacts, and prunes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from tests.helpers.plan_codes import plan, view


def test_sst_pln016_fires() -> None:
    [diagnostic] = plan_notices(plan((view("sales"), view("orders"))))
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN016", Severity.INFO)
    assert diagnostic.message == "semantic_view: 2 create, 0 replace, 0 noop, 0 prune"


def test_sst_pln016_silent() -> None:
    # A plan with no changes, such as one a dependency cycle stopped, has nothing to count.
    cyclic = plan((view("a", depends_on=("semantic_view:b",)), view("b", depends_on=("semantic_view:a",))))
    assert plan_notices(cyclic) == ()
