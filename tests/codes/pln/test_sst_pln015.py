"""SST-PLN015: an artifact's proposed definition matches what Snowflake holds."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def test_sst_pln015_fires() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
    )
    [diagnostic] = [item for item in plan_notices(planned) if item.code == "SST-PLN015"]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "semantic_view:sales: NOOP"
    assert diagnostic.subject == "semantic_view:sales"


def test_sst_pln015_silent() -> None:
    assert [item.code for item in plan_notices(plan((view("sales"),)))] == ["SST-PLN016"]
