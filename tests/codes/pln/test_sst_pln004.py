"""SST-PLN004: a marked prune candidate has no matching entry in state, so it is skipped."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from tests.helpers.plan_codes import entry, live, only, plan, view


def test_sst_pln004_fires() -> None:
    orphan = view("orphan")
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    [diagnostic] = only(plan((), observed=(marked,), include_prune=True), "SST-PLN004")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "DB.SCH.ORPHAN carries an SST marker and is absent from state; skipped"


def test_sst_pln004_silent() -> None:
    orphan = view("orphan")
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    planned = plan((), observed=(marked,), applied={orphan.key: entry(orphan, "a" * 64)}, include_prune=True)
    assert only(planned, "SST-PLN004") == []
