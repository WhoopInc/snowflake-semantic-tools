"""SST-PLN023: an unquoted declared name matches its live object only after case folding."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from tests.helpers.plan_codes import entry, live, manifest_of, only, plan, view


def _planned(raw_name: str) -> list[Diagnostic]:
    sales = view("Sales")
    planned_manifest = manifest_of(sales)
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint), raw_name=raw_name)
    planned = plan(
        (sales,),
        observed=(marked,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
    )
    assert planned.changes[0].action is Action.NOOP
    return only(planned, "SST-PLN023")


def test_sst_pln023_fires() -> None:
    [diagnostic] = _planned("sales")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:sales: declared 'SALES', live object is 'sales'"


def test_sst_pln023_silent() -> None:
    assert _planned("SALES") == []
