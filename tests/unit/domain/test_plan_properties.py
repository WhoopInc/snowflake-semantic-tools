"""What `plan --full` says an update changes on a live object, property by property."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, OwnershipMarker
from snowflake_semantic_tools.domain.plan.properties import PropertyChange, changed_properties
from tests.helpers.artifact_builders import change, marker, observed, rendered

MANIFEST = "a" * 64


def test_an_update_names_only_the_properties_that_differ() -> None:
    artifact = rendered()
    live = observed(artifact, ownership=marker(artifact))
    assert changed_properties(change(artifact, Action.UPDATE, live=live), MANIFEST) == ()
    moved = replace(live, qualified_name=QualifiedName.from_parts("db", "schema", '"V"'))
    assert changed_properties(change(artifact, Action.UPDATE, live=moved), MANIFEST) == (
        PropertyChange("target", 'DB.SCHEMA."V"', "DB.SCHEMA.V"),
    )
    stale = OwnershipMarker("b" * 64, "c" * 64)
    assert changed_properties(change(artifact, Action.UPDATE, live=observed(artifact, ownership=stale)), MANIFEST) == (
        PropertyChange("fingerprint", "c" * 64, artifact.fingerprint),
        PropertyChange("manifest_id", "b" * 64, MANIFEST),
    )


def test_a_missing_marker_reads_as_empty() -> None:
    artifact = rendered()
    names = [
        item.name for item in changed_properties(change(artifact, Action.UPDATE, live=observed(artifact)), MANIFEST)
    ]
    assert names == ["fingerprint", "manifest_id"]


def test_an_agent_names_the_aliases_and_tags_an_update_sets() -> None:
    artifact = replace(rendered(), desired_alias="prod", desired_tags=("b", "a"))
    live = replace(observed(artifact, ownership=marker(artifact)), aliases=("old", "prod"), tags=("a",))
    assert changed_properties(change(artifact, Action.UPDATE, live=live), MANIFEST) == (
        PropertyChange("aliases", "old, prod", "prod"),
        PropertyChange("tags", "a", "a, b"),
    )
    bare = replace(artifact, desired_alias=None, desired_tags=())
    assert changed_properties(change(bare, Action.UPDATE, live=live), MANIFEST)[0] == PropertyChange(
        "aliases", "old, prod", ""
    )


def test_only_an_update_of_an_observed_object_names_properties() -> None:
    artifact = rendered()
    live = observed(artifact)
    assert changed_properties(change(artifact, Action.CREATE, live=live), MANIFEST) == ()
    assert changed_properties(change(artifact, Action.PRUNE, live=live), MANIFEST) == ()
    assert changed_properties(change(artifact, Action.UPDATE), MANIFEST) == ()
    unrendered = replace(change(artifact, Action.UPDATE, live=live), rendered=None)
    assert changed_properties(unrendered, MANIFEST) == ()
