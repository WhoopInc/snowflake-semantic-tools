"""SST-PLN024: an object holds the target name and state records no SST entry for it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, OwnershipMarker
from tests.helpers.plan_codes import entry, live, manifest_of, only, plan, view


def test_sst_pln024_fires() -> None:
    sales = view("sales")
    planned = plan((sales,), observed=(live(sales),))
    [diagnostic] = only(planned, "SST-PLN024")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:sales: DB.SCH.SALES exists without trusted SST ownership"
    assert planned.changes[0].reason is ChangeReason.UNMANAGED_OBJECT


def test_sst_pln024_silent() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
    )
    assert only(planned, "SST-PLN024") == [] and planned.changes[0].action is Action.NOOP
