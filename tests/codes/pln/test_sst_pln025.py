"""SST-PLN025: state records the artifact at a target other than the one declared."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import ChangeReason, OwnershipMarker
from tests.helpers.plan_codes import entry, live, manifest_of, only, plan, view


def _planned(recorded_at: str) -> list[Diagnostic]:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    recorded = entry(sales, planned_manifest.manifest_id, qualified_name=recorded_at)
    planned = plan((sales,), observed=(marked,), applied={sales.key: recorded}, manifest=planned_manifest)
    if only(planned, "SST-PLN025"):
        assert planned.changes[0].reason is ChangeReason.TARGET_MOVED
    return only(planned, "SST-PLN025")


def test_sst_pln025_fires() -> None:
    [diagnostic] = _planned("OLD.PLACE.SALES")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:sales: recorded target 'OLD.PLACE.SALES' differs from declared target 'DB.SCH.SALES'"
    )


def test_sst_pln025_silent() -> None:
    assert _planned("DB.SCH.SALES") == []
