"""Fingerprint each member of a definition, so two releases are compared member by member offline.

A member's fingerprint is the hash of its properties as its definition states them, keyed
`<kind>:<name>` with the name casefolded, the way `diff_definitions` matches members. Compile records
them in the manifest and apply in state, so `changed_members` can say which members an update
changes when plan does not read the object, and lineage which members changed since a manifest.
"""

from __future__ import annotations

from collections.abc import Iterable

from snowflake_semantic_tools.domain.model.lifecycle.member import Definition
from snowflake_semantic_tools.domain.state.documents import content_hash

Fingerprints = tuple[tuple[str, str], ...]


def member_fingerprints(definition: Definition) -> Fingerprints:
    """Return each member's key with the hash of its properties, sorted by key."""
    return tuple(
        sorted(
            (
                f"{member.kind}:{member.name.casefold()}",
                content_hash(
                    [[name, list(value) if isinstance(value, tuple) else value] for name, value in member.properties]
                ),
            )
            for member in definition.members
        )
    )


def changed_members(before: Iterable[tuple[str, str]], after: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    """Return the keys of the members whose fingerprints differ, or that only one side has, sorted."""
    old, new = dict(before), dict(after)
    return tuple(sorted(key for key in old.keys() | new.keys() if old.get(key) != new.get(key)))
