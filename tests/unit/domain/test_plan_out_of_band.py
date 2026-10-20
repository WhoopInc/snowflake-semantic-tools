"""SST-PLN014 says which of the marker's conditions failed, so a reader knows what changed by hand."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, OwnershipMarker
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def _detail(marker: OwnershipMarker | None, *, moved: bool = False, overwrite: bool = False) -> tuple[str, Action]:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    found = live(sales, marker=marker)
    if moved:
        found = replace(found, qualified_name=QualifiedName.parse(f"{sales.target.sql}_OLD"))
    planned = plan(
        (sales,),
        observed=(found,),
        applied={sales.key: entry(sales, planned_manifest.manifest_id)},
        manifest=planned_manifest,
        overwrite=overwrite,
    )
    code = "SST-PLN043" if overwrite else "SST-PLN014"
    [diagnostic] = coded(planned.diagnostics, code)
    return str(diagnostic.context["detail"]), planned.changes[0].action


def test_a_marker_naming_another_publish_says_so() -> None:
    detail, action = _detail(OwnershipMarker("b" * 64, "c" * 64))
    assert (detail, action) == ("its ownership marker names another publish than the one state records", Action.BLOCKED)


def test_an_object_under_another_name_than_state_records_names_both() -> None:
    sales = view("sales")
    detail, _ = _detail(OwnershipMarker("b" * 64, "c" * 64), moved=True)
    assert detail == f"state records it as {sales.target.sql}, not {sales.target.sql}_OLD"


def test_overwrite_reports_the_same_detail_and_updates() -> None:
    blocked, _ = _detail(None)
    overwritten, action = _detail(None, overwrite=True)
    assert overwritten == blocked == "its comment was changed and no longer carries SST's ownership marker"
    assert action is Action.UPDATE
    assert ChangeReason.OVERWRITE.value == "overwrite"
