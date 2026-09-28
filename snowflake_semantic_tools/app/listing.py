"""Read-only projections for list and debug commands."""

from __future__ import annotations

from dataclasses import dataclass

from ..domain.state.model import Manifest, State


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    key: str
    target: str
    fingerprint: str
    applied_fingerprint: str | None
    status: str


def list_artifacts(manifest: Manifest, state: State | None = None) -> tuple[ArtifactSummary, ...]:
    state_entries = state.applied if state else {}
    return tuple(
        ArtifactSummary(
            key,
            entry.publish_target,
            entry.fingerprint,
            state_entries[key].fingerprint if key in state_entries else None,
            "applied" if key in state_entries and state_entries[key].fingerprint == entry.fingerprint else "pending",
        )
        for key, entry in sorted(manifest.artifacts.items())
    )
