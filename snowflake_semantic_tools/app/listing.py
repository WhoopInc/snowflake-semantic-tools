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
        version: The published version state records: a skill's or plugin's alias, the committed
            version an agent serves, a profile row's VERSION; None when it records none.
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
            _version(state_entries.get(key)),
        )
        for key, entry in sorted(manifest.artifacts.items())
    )


def _version(recorded: AppliedEntry | None) -> str | None:
    """The version state records as published: an extension's alias, else its `version` component."""
    if recorded is None:
        return None
    components = dict(recorded.component_fingerprints)
    return components.get("alias") or components.get("version")


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
        tables: The dbt models and sources under the tables a member reads; empty for a table.
        uses: The members a member uses, such as a derived metric's metrics.
        columns: The columns a member reads, as `<table>.<column>`.
        expression: A fact's, dimension's, filter's or metric's resolved expression; else empty.
        members: The members that read a table; empty for a member.
    """

    key: str
    name: str
    artifacts: tuple[str, ...]
    relation: str = ""
    tables: tuple[str, ...] = ()
    uses: tuple[str, ...] = ()
    columns: tuple[str, ...] = ()
    expression: str = ""
    members: tuple[str, ...] = ()


def list_members(manifest: Manifest, member_type: str) -> tuple[MemberSummary, ...]:
    """Summarize each member of one type, in key order: what it attaches to, and what it reads."""
    prefix = f"{member_type}:"
    return tuple(
        MemberSummary(
            key,
            key[len(prefix) :],
            _names(entry, "attached_to"),
            tables=_names(entry, "tables"),
            uses=_names(entry, "uses"),
            columns=_names(entry, "columns"),
            expression=str(entry.get("expr") or "") if isinstance(entry, Mapping) else "",
        )
        for key, entry in sorted(manifest.members.items())
        if key.startswith(prefix)
    )


def list_tables(manifest: Manifest) -> tuple[MemberSummary, ...]:
    """Summarize each dbt model an artifact reads, in name order: its relation, readers and members."""
    readers: dict[str, list[str]] = {}
    for key, entry in sorted(manifest.members.items()):
        for name in _names(entry, "tables"):
            readers.setdefault(name, []).append(key)
    return tuple(
        MemberSummary(
            f"table:{name}",
            name,
            _names(entry, "referenced_by"),
            str(entry.get("relation") or "") if isinstance(entry, Mapping) else "",
            members=tuple(readers.get(str(name).casefold(), ())),
        )
        for name, entry in sorted(manifest.dbt_models.items())
    )


def _names(entry: object, field: str) -> tuple[str, ...]:
    """Return the sorted text values of a manifest entry's list field; empty when it has none."""
    values = entry.get(field) if isinstance(entry, Mapping) else None
    return tuple(sorted(str(value) for value in values)) if isinstance(values, list) else ()
