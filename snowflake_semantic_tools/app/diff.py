"""Read the states `sst diff` compares: the compiled manifest, a saved plan, or a live target.

Each reader returns a state by artifact key, as `domain.plan.diff` compares them. The live state
is what a target holds of SST's own: every artifact its state table records, observed by the
ownership marker on its object, so an object dropped or replaced by hand drops out or differs.
A composite artifact has no single object to observe, so the state table's entry stands for it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, Registry
from snowflake_semantic_tools.domain.plan.diff import ArtifactState
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, SavedPlan

ArtifactStates = Mapping[str, ArtifactState]


class LiveStatePort(CatalogPort, StatePort, Protocol):
    """What reading a live target needs: its state table, and the markers on its objects."""


def manifest_states(manifest: Manifest) -> dict[str, ArtifactState]:
    """Return what the compiled manifest renders, by artifact key."""
    return {
        key: ArtifactState(entry.fingerprint, entry.publish_target, manifest.manifest_id)
        for key, entry in manifest.artifacts.items()
    }


def plan_states(plan: SavedPlan) -> dict[str, ArtifactState]:
    """Return what a saved plan would leave published: everything it renders, less what it prunes."""
    return {
        change.key: ArtifactState(change.fingerprint, change.target or "", plan.manifest_id)
        for change in plan.changes
        if change.action != Action.PRUNE.value and change.fingerprint is not None
    }


def live_states(
    port: LiveStatePort,
    state_table: QualifiedName,
    target_name: str,
    registry: Registry = SEMANTIC_REGISTRY,
) -> tuple[dict[str, ArtifactState] | None, tuple[Diagnostic, ...]]:
    """Return what a target holds of SST's artifacts, by key; None when its state cannot be read.

    Never writes. An artifact whose object is gone, or carries no SST marker, is not held.

    Raises:
        SnowflakePortError: a read Snowflake refused.

    Diagnostics:
        SST-MAN022: the state table exists and cannot be read.
    """
    entries = port.read_state(state_table, target_name)
    if entries is None:
        return None, (D("SST-MAN022", path=state_table.sql, detail="the state table cannot be read"),)
    held: dict[str, ArtifactState] = {}
    for key, entry in sorted(entries.items()):
        if entry.outcome == DEACTIVATED:
            continue
        artifact_type = registry.artifacts.get(split_artifact_key(key)[0])
        object_type = artifact_type.object_type if artifact_type is not None else ""
        if not object_type:
            held[key] = ArtifactState(entry.fingerprint, entry.qualified_name, entry.manifest_id)
            continue
        marker = port.describe_marker(QualifiedName.parse(entry.qualified_name), object_type)
        if marker is not None:
            held[key] = ArtifactState(marker.fingerprint, entry.qualified_name, marker.manifest_id)
    return held, ()
