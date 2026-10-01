"""What `sst list` shows: each compiled artifact beside what state recorded for it, read-only."""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.state import DEACTIVATED, FAILED_AFTER_WRITE, AppliedEntry, Manifest, State


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    """One manifest artifact beside what state recorded for it.

    Attributes:
        target: The qualified name the artifact publishes to.
        fingerprint: The manifest's fingerprint for the artifact.
        applied_fingerprint: The fingerprint state recorded; None when it records nothing for the key.
        status: `deactivated` once a prune retired it, `failed` when a publish wrote and did not
            complete, `applied` when state records this fingerprint, else `pending`.
        version: The published version state records for an extension-backed artifact; None
            when it records none.
    """

    key: str
    target: str
    fingerprint: str
    applied_fingerprint: str | None
    status: str
    version: str | None = None


def list_artifacts(manifest: Manifest, state: State | None = None) -> tuple[ArtifactSummary, ...]:
    """Summarize every manifest artifact, in key order, against what `state` recorded for it.

    Without a state every artifact is `pending`; an entry for an artifact the manifest no
    longer holds is not listed.
    """
    state_entries = state.applied if state else {}
    return tuple(
        ArtifactSummary(
            key,
            entry.publish_target,
            entry.fingerprint,
            state_entries[key].fingerprint if key in state_entries else None,
            _status(state_entries.get(key), entry.fingerprint),
            # The published version of an extension-backed artifact, when state records one.
            dict(state_entries[key].component_fingerprints).get("alias") if key in state_entries else None,
        )
        for key, entry in sorted(manifest.artifacts.items())
    )


def _status(recorded: AppliedEntry | None, fingerprint: str) -> str:
    if recorded is None:
        return "pending"
    if recorded.outcome == DEACTIVATED:
        return DEACTIVATED
    if recorded.outcome == FAILED_AFTER_WRITE:
        # Something was written, but the publish did not complete.
        return "failed"
    return "applied" if recorded.fingerprint == fingerprint else "pending"
