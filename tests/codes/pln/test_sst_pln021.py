"""SST-PLN021: under --prune, a schema holds live objects the project does not declare and the plan leaves."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ObservedArtifact, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view

MANIFEST = "a" * 64


def _orphan(name: str) -> ObservedArtifact:
    """A live view SST applied and still marks, which the project no longer declares."""
    orphan = view(name)
    return live(orphan, marker=OwnershipMarker(MANIFEST, orphan.fingerprint))


def test_sst_pln021_fires() -> None:
    planned = plan((), observed=(live(view("legacy")), live(view("old"))), include_prune=True)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN021")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "DB.SCH holds objects this project does not declare and the plan leaves in place (2)"


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
    assert coded(planned.diagnostics, "SST-PLN021") == []


def test_sst_pln021_silent_when_prune_drops_every_orphan() -> None:
    # The orphan becomes a PRUNE change, so nothing is left undeclared.
    planned = plan(
        (),
        observed=(_orphan("orphan"),),
        applied={"semantic_view:orphan": entry(view("orphan"), MANIFEST)},
        include_prune=True,
    )
    assert [change.action for change in planned.changes] == [Action.PRUNE]
    assert coded(planned.diagnostics, "SST-PLN021") == []


def test_sst_pln021_counts_only_what_the_plan_skips() -> None:
    # One orphan is pruned; one carries no marker (SST-PLN003); one has no state entry (SST-PLN004).
    planned = plan(
        (),
        observed=(_orphan("dropped"), live(view("unmarked")), _orphan("unrecorded")),
        applied={"semantic_view:dropped": entry(view("dropped"), MANIFEST)},
        include_prune=True,
    )
    assert [change.key for change in planned.changes] == ["semantic_view:dropped"]
    assert {item.code for item in planned.diagnostics} >= {"SST-PLN003", "SST-PLN004"}
    [diagnostic] = coded(planned.diagnostics, "SST-PLN021")
    assert diagnostic.message.endswith("(2)")
