"""SST-PLN019: another session holds a lock on an object the plan writes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln019_fires() -> None:
    sales = view("sales")
    planned = plan((sales,), preflight=Preflight("dev", "DEPLOYER", locked=frozenset((sales.target.folded,))))
    [diagnostic] = only(planned, "SST-PLN019")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "DB.SCH.SALES is being modified by another session"


def test_sst_pln019_silent() -> None:
    locked = frozenset((view("orders").target.folded,))
    assert only(plan((view("sales"),), preflight=Preflight("dev", "DEPLOYER", locked=locked)), "SST-PLN019") == []
