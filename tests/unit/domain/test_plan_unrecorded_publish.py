"""Plan after an apply that published an object and was stopped before it recorded it.

State still records the write before; the live marker names the definition being planned. The
object is updated, which records it, instead of being refused as changed out of band.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view

EARLIER_MANIFEST = "a" * 64
EARLIER_FINGERPRINT = "b" * 64


def test_an_object_carrying_the_planned_definitions_marker_is_updated_not_refused() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    earlier = replace(entry(sales, EARLIER_MANIFEST), fingerprint=EARLIER_FINGERPRINT)
    published = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))

    planned = plan((sales,), observed=(published,), applied={sales.key: earlier}, manifest=planned_manifest)

    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.UNRECORDED_PUBLISH)
    assert coded(planned.diagnostics, "SST-PLN014") == [] and not change.diagnostics


def test_the_planned_definition_published_under_an_earlier_manifest_is_updated_too() -> None:
    # The project changed elsewhere since the stopped apply, so the manifest moved on; this
    # artifact's definition did not, and that is what the marker vouches for.
    sales = view("sales")
    earlier = replace(entry(sales, EARLIER_MANIFEST), fingerprint=EARLIER_FINGERPRINT)
    published = live(sales, marker=OwnershipMarker("c" * 64, sales.fingerprint))

    [change] = plan((sales,), observed=(published,), applied={sales.key: earlier}).changes

    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.UNRECORDED_PUBLISH)


def test_a_marker_naming_neither_the_recorded_nor_the_planned_definition_is_still_out_of_band() -> None:
    sales = view("sales")
    earlier = replace(entry(sales, EARLIER_MANIFEST), fingerprint=EARLIER_FINGERPRINT)
    other = live(sales, marker=OwnershipMarker(EARLIER_MANIFEST, "d" * 64))

    planned = plan((sales,), observed=(other,), applied={sales.key: earlier})

    assert planned.changes[0].action is Action.BLOCKED
    assert [item.code for item in coded(planned.diagnostics, "SST-PLN014")] == ["SST-PLN014"]


def test_an_object_without_a_marker_is_still_out_of_band() -> None:
    sales = view("sales")
    earlier = replace(entry(sales, EARLIER_MANIFEST), fingerprint=EARLIER_FINGERPRINT)

    planned = plan((sales,), observed=(live(sales),), applied={sales.key: earlier})

    assert planned.changes[0].action is Action.BLOCKED
    assert coded(planned.diagnostics, "SST-PLN014")


def test_an_object_state_does_not_record_stays_unmanaged_whatever_its_marker_names() -> None:
    # Without an entry the object may be another target's or project's: SST never claims it.
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    published = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))

    planned = plan((sales,), observed=(published,), manifest=planned_manifest)

    assert planned.changes[0].action is Action.BLOCKED
    assert [item.code for item in coded(planned.diagnostics, "SST-PLN024")] == ["SST-PLN024"]
