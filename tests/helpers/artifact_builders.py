"""Rendered artifacts, live observations, changes, states and manifests the plan and apply tests build on."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    ChangeSet,
    ObservedArtifact,
    OwnershipMarker,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.state import (
    APPLIED,
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    ImpactIndex,
    Manifest,
    State,
)
from tests.helpers.manifests import build_minimal_manifest
from tests.helpers.sql_values import statement


def target() -> TargetIdentity:
    return TargetIdentity("verify", "account", Identifier.parse("db"), Identifier.parse("schema"))


def empty_state(applied: dict[str, AppliedEntry] | None = None) -> State:
    """A state for `target()` that records `applied`, and nothing when it is not given."""
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType(dict(applied or {})))


def rendered(name: str = "V", *, depends_on: tuple[str, ...] = ()) -> RenderedArtifact:
    return RenderedArtifact.create(
        key=f"semantic_view:{name.casefold()}",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "schema", name),
        ddl=statement(f"CREATE OR REPLACE SEMANTIC VIEW DB.SCHEMA.{name} TABLES (T AS DB.SCHEMA.T) COPY GRANTS"),
        depends_on=depends_on,
        required_relations=(QualifiedName.from_parts("db", "schema", "t"),),
    )


def marker(artifact: RenderedArtifact) -> OwnershipMarker:
    return OwnershipMarker("a" * 64, artifact.fingerprint)


def observed(artifact: RenderedArtifact, *, ownership: OwnershipMarker | None = None) -> ObservedArtifact:
    return ObservedArtifact(
        artifact.key,
        artifact.target.name.folded,
        artifact.target,
        "SEMANTIC VIEW",
        "OWNER",
        "now",
        ownership.text if ownership else None,
        ownership,
        (),
    )


def changeset(*changes: Change) -> ChangeSet:
    return ChangeSet("m" * 64, target(), tuple(changes), DiagnosticBag(), "now")


def change(
    artifact: RenderedArtifact,
    action: Action = Action.CREATE,
    *,
    live: ObservedArtifact | None = None,
) -> Change:
    return Change(
        artifact.key,
        "semantic_view",
        action,
        ChangeReason.NOT_PRESENT if action is Action.CREATE else ChangeReason.FINGERPRINT_DIFFERS,
        None if action is Action.PRUNE else artifact,
        live,
        artifact.depends_on,
        100,
    )


def state() -> State:
    return State.empty(target())


def manifest(artifacts: dict[str, RenderedArtifact]) -> Manifest:
    return build_minimal_manifest(
        artifacts,
        project={},
        sources={},
        members={},
        files={},
        impact=ImpactIndex(),
        diagnostics_summary={},
    )


def applied_entry(target: str, fingerprint: str, *, outcome: str = APPLIED) -> AppliedEntry:
    """An entry applied to `target` with `fingerprint` and `outcome`."""
    return AppliedEntry(fingerprint, target, "now", "run", outcome, fingerprint, "a" * 64)
