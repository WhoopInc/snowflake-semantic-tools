"""SST-PLN003: a prune candidate carries no SST ownership marker, so it is skipped."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def test_sst_pln003_fires() -> None:
    orphan = view("orphan")
    planned = plan((), observed=(live(orphan),), include_prune=True)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN003")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "DB.SCH.ORPHAN has no SST ownership marker; skipped"
    assert planned.changes == ()


def test_sst_pln003_silent() -> None:
    orphan = view("orphan")
    planned_manifest = manifest_of()
    owned = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    planned = plan(
        (),
        observed=(owned,),
        applied={orphan.key: entry(orphan, "a" * 64)},
        include_prune=True,
        manifest=planned_manifest,
    )
    assert coded(planned.diagnostics, "SST-PLN003") == []
    assert [change.action for change in planned.changes] == [Action.PRUNE]
