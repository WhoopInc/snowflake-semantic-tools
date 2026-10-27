"""SST-PLN014: the live object changed after apply recorded it, its marker or a property it published."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeSet,
    OwnershipMarker,
    RenderedArtifact,
    drift_properties,
    live_digest,
)
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, live, manifest_of, plan, view


def test_sst_pln014_fires() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    planned = plan((sales,), observed=(live(sales),), applied=recorded, manifest=planned_manifest)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN014")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:sales was changed out of band (its comment was changed and no longer carries SST's "
        "ownership marker); "
        "apply leaves it as it is"
    )
    assert planned.changes[0].action is Action.BLOCKED


def test_sst_pln014_silent() -> None:
    sales = view("sales")
    planned_manifest = manifest_of(sales)
    recorded = {sales.key: entry(sales, planned_manifest.manifest_id)}
    marked = live(sales, marker=OwnershipMarker(planned_manifest.manifest_id, sales.fingerprint))
    planned = plan((sales,), observed=(marked,), applied=recorded, manifest=planned_manifest)
    assert coded(planned.diagnostics, "SST-PLN014") == []
    assert planned.changes[0].action is Action.NOOP


# One hand edit per kind that keeps the comment, as apply then plan would read the object.
_EDITS: dict[str, tuple[dict[str, Any], dict[str, Any], str]] = {
    "AGENT": ({"newest_spec": "a" * 64}, {"newest_spec": "b" * 64}, "specification"),
    "CORTEX SEARCH SERVICE": (
        {"shown": {"target_lag": "1 day", "warehouse": "WH"}},
        {"shown": {"target_lag": "7 days", "warehouse": "WH"}},
        "target_lag",
    ),
    "FUNCTION": ({"shown": {"is_secure": "N"}}, {"shown": {"is_secure": "Y"}}, "secure"),
    "PROCEDURE": (
        {"shown": {"is_secure": "N"}, "described": {"execute as": "CALLER", "body": "BEGIN RETURN 1; END;"}},
        {"shown": {"is_secure": "N"}, "described": {"execute as": "OWNER", "body": "BEGIN RETURN 2; END;"}},
        "body, execute_as",
    ),
    "SEMANTIC VIEW": (
        {"definition": "create or replace semantic view V metrics (T.M as AVG(T.X))"},
        {"definition": "create or replace semantic view V metrics (T.M as MAX(T.X))"},
        "definition",
    ),
}


def _digest(object_type: str, facts: dict[str, Any]) -> str | None:
    return live_digest(
        drift_properties(
            object_type,
            facts.get("shown", {}),
            described=facts.get("described"),
            definition=facts.get("definition"),
            newest_spec=facts.get("newest_spec"),
        )
    )


def _planned(object_type: str, recorded: str | None, observed: str | None) -> tuple[RenderedArtifact, ChangeSet]:
    artifact = replace(view("thing"), object_type=object_type)
    planned_manifest = manifest_of(artifact)
    applied = {artifact.key: replace(entry(artifact, planned_manifest.manifest_id), live_digest=recorded)}
    marked = live(artifact, marker=OwnershipMarker(planned_manifest.manifest_id, artifact.fingerprint))
    found = replace(marked, object_type=object_type, live_digest=observed)
    return artifact, plan((artifact,), observed=(found,), applied=applied, manifest=planned_manifest)


@pytest.mark.parametrize("object_type", sorted(_EDITS))
def test_sst_pln014_fires_for_a_property_edited_under_an_unchanged_marker(object_type: str) -> None:
    before, after, changed = _EDITS[object_type]
    _, planned = _planned(object_type, _digest(object_type, before), _digest(object_type, after))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN014")
    assert diagnostic.message == (
        f"semantic_view:thing was changed out of band (its {changed} changed; its marker did not); "
        "apply leaves it as it is"
    )
    assert planned.changes[0].action is Action.BLOCKED


@pytest.mark.parametrize("object_type", sorted(_EDITS))
def test_sst_pln014_silent_for_an_equal_digest_or_one_never_recorded(object_type: str) -> None:
    before, after, _ = _EDITS[object_type]
    for recorded, observed in (
        (_digest(object_type, before), _digest(object_type, before)),
        (None, _digest(object_type, after)),
        (_digest(object_type, before), None),
    ):
        _, planned = _planned(object_type, recorded, observed)
        assert coded(planned.diagnostics, "SST-PLN014") == []
        assert planned.changes[0].action is Action.NOOP
