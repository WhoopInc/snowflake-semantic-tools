"""SST-PLN021: under --prune, a schema holds live objects the project does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from tests.helpers.plan_codes import entry, live, manifest_of, only, plan, view


def test_sst_pln021_fires() -> None:
    planned = plan((), observed=(live(view("legacy")), live(view("old"))), include_prune=True)
    [diagnostic] = only(planned, "SST-PLN021")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "2 objects in DB.SCH are not declared here"


def test_sst_pln021_silent() -> None:
    # A declaration that differs from its live object only by case is that object.
    declared = view("Sales")
    planned_manifest = manifest_of(declared)
    marked = live(
        declared, marker=OwnershipMarker(planned_manifest.manifest_id, declared.fingerprint), raw_name="SALES"
    )
    planned = plan(
        (declared,),
        observed=(marked,),
        applied={declared.key: entry(declared, planned_manifest.manifest_id)},
        include_prune=True,
        manifest=planned_manifest,
    )
    assert only(planned, "SST-PLN021") == []
