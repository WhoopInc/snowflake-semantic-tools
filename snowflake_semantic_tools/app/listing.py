"""What `sst list` shows: each compiled artifact beside what state recorded for it, read-only.

A semantic view's members -- its metrics, filters and the rest -- and the tables the views read
are listed from the manifest too, each with the artifacts that carry or read it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.state import (
    DEACTIVATED,
    FAILED_AFTER_WRITE,
    PARTIAL_WRITE,
    AppliedEntry,
    Manifest,
    State,
)


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
    if recorded.outcome in (FAILED_AFTER_WRITE, PARTIAL_WRITE):
        # Something was written, but the publish did not complete.
        return "failed"
    return "applied" if recorded.fingerprint == fingerprint else "pending"


@dataclass(frozen=True, slots=True)
class MemberSummary:
    """One member of a semantic view, such as a metric, or one table the views read.

    Attributes:
        key: `<member type>:<name>`, or `table:<dbt model>` for a table.
        artifacts: The artifacts the member attaches to, or that read the table, in key order.
        relation: The relation a table resolves to; empty for any other member.
    """

    key: str
    name: str
    artifacts: tuple[str, ...]
    relation: str = ""


def list_members(manifest: Manifest, member_type: str) -> tuple[MemberSummary, ...]:
    """Summarize each member of one type the manifest records, in key order, with what it attaches to."""
    prefix = f"{member_type}:"
    return tuple(
        MemberSummary(key, key[len(prefix) :], _names(entry, "attached_to"))
        for key, entry in sorted(manifest.members.items())
        if key.startswith(prefix)
    )


def list_tables(manifest: Manifest) -> tuple[MemberSummary, ...]:
    """Summarize each dbt model an artifact reads, in name order, with its relation and its readers."""
    return tuple(
        MemberSummary(
            f"table:{name}",
            name,
            _names(entry, "referenced_by"),
            str(entry.get("relation") or "") if isinstance(entry, Mapping) else "",
        )
        for name, entry in sorted(manifest.dbt_models.items())
    )


def _names(entry: object, field: str) -> tuple[str, ...]:
    """Return the sorted text values of a manifest entry's list field; empty when it has none."""
    values = entry.get(field) if isinstance(entry, Mapping) else None
    return tuple(sorted(str(value) for value in values)) if isinstance(values, list) else ()
