"""SST-PLN014: the live object no longer carries the marker apply recorded, so it changed out of band."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from tests.helpers.plan_codes import entry, live, manifest_of, only, plan, view


def test_sst_pln014_fires() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    planned = plan((sales,), observed=(live(sales),), applied=recorded, manifest=planned_manifest)
    [diagnostic] = only(planned, "SST-PLN014")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:sales was changed out of band"
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln014_silent() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan((sales,), observed=(marked,), applied=recorded, manifest=planned_manifest)
    assert only(planned, "SST-PLN014") == []
    assert planned.changes[0].action is Action.NOOP
