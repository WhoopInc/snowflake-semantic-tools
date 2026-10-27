"""Plan of an object `sst state adopt` recorded: SST did not publish it, and it carries no marker.

Its first plan updates it, which writes SST's marker; one that changed since it was adopted is
blocked as changed out of band, and `--overwrite` publishes over it as for any other edit.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, OwnershipMarker
from snowflake_semantic_tools.domain.model.lifecycle.drift import live_digest
from snowflake_semantic_tools.domain.state import ADOPTED
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def test_an_adopted_object_without_a_marker_is_updated_to_write_it() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    adopted = replace(entry(sales, planned_manifest.manifest_id), outcome=ADOPTED)

    planned = plan((sales,), observed=(live(sales),), applied={sales.key: adopted}, manifest=planned_manifest)

    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.ADOPTED)
    assert coded(planned.diagnostics, "SST-PLN014") == []


def test_an_adopted_object_whose_properties_changed_since_is_blocked() -> None:
    sales = view("sales")
    adopted = replace(
        entry(sales, manifest_of(sales).manifest_id), outcome=ADOPTED, live_digest=live_digest({"secure": "N"})
    )
    edited = replace(live(sales), live_digest=live_digest({"secure": "Y"}))

    planned = plan((sales,), observed=(edited,), applied={sales.key: adopted})

    assert planned.changes[0].action is Action.BLOCKED
    [blocked] = coded(planned.diagnostics, "SST-PLN014")
    assert "since `sst state adopt` recorded it" in blocked.message


def test_overwrite_publishes_over_an_adopted_object_that_changed() -> None:
    sales = view("sales")
    adopted = replace(
        entry(sales, manifest_of(sales).manifest_id), outcome=ADOPTED, live_digest=live_digest({"secure": "N"})
    )
    edited = replace(live(sales), live_digest=live_digest({"secure": "Y"}))

    [change] = plan((sales,), observed=(edited,), applied={sales.key: adopted}, overwrite=True).changes

    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.OVERWRITE)


def test_an_adopted_object_that_gained_another_marker_is_out_of_band() -> None:
    sales = view("sales")
    adopted = replace(entry(sales, manifest_of(sales).manifest_id), outcome=ADOPTED)
    claimed = live(sales, marker=OwnershipMarker("c" * 64, "d" * 64))

    planned = plan((sales,), observed=(claimed,), applied={sales.key: adopted})

    assert planned.changes[0].action is Action.BLOCKED
    assert coded(planned.diagnostics, "SST-PLN014")


def test_an_unrecorded_object_stays_unmanaged() -> None:
    sales = view("sales")

    planned = plan((sales,), observed=(live(sales),))

    assert (planned.changes[0].action, planned.changes[0].reason) == (Action.BLOCKED, ChangeReason.UNMANAGED_OBJECT)
