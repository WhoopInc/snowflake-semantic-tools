"""SST-PLN012: the warehouse the profile names does not exist, or the role cannot see it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from tests.helpers.plan_codes import only, plan, view


def test_sst_pln012_fires() -> None:
    preflight = Preflight("dev", "DEPLOYER", warehouse="REPORTING_WH", warehouse_usable=False)
    [diagnostic] = only(plan((view("sales"),), preflight=preflight), "SST-PLN012")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "warehouse REPORTING_WH does not exist or is not authorised"


def test_sst_pln012_silent() -> None:
    preflight = Preflight("dev", "DEPLOYER", warehouse="REPORTING_WH", warehouse_usable=True)
    assert only(plan((view("sales"),), preflight=preflight), "SST-PLN012") == []
