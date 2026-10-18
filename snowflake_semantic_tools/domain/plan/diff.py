"""Compare two states of a project's artifacts, as `sst diff` reports them and `state:` selects.

A state maps each artifact key to what is recorded for it: the fingerprint of its rendered form,
the object it publishes to, and the manifest that rendered it. Compared against an earlier state,
each key is in one of four states, the same four `state:<state>` selects: `new` (only in the later
one), `orphaned` (only in the earlier one), `modified` (in both, fingerprints differing), and
`unmodified`. Fingerprints are of normalised renders, so two states that differ only in how
Snowflake reads an object back compare equal.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.lifecycle.member import Definition, MemberDiff
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions

NEW, MODIFIED, UNMODIFIED, ORPHANED = "new", "modified", "unmodified", "orphaned"
STATES = (NEW, MODIFIED, UNMODIFIED, ORPHANED)


def state_classes(current: Mapping[str, str | None], previous: Mapping[str, str | None]) -> dict[str, frozenset[str]]:
    """Return the keys in each of the four states, comparing fingerprints by key.

    Args:
        current: Each key's fingerprint in the later state.
        previous: Each key's fingerprint in the earlier state.
    """
    unchanged = frozenset(
        key for key, fingerprint in current.items() if key in previous and previous[key] == fingerprint
    )
    return {
        NEW: frozenset(key for key in current if key not in previous),
        MODIFIED: frozenset(key for key in current if key in previous and key not in unchanged),
        UNMODIFIED: unchanged,
        ORPHANED: frozenset(key for key in previous if key not in current),
    }


@dataclass(frozen=True, slots=True)
class ArtifactState:
    """What one state records for one artifact.

    Attributes:
        fingerprint: The fingerprint of the artifact's rendered form; None when it has none.
        target: The qualified name of the object it publishes to; empty when it has none.
        manifest_id: The manifest that rendered it; empty when the state does not say.
    """

    fingerprint: str | None
    target: str = ""
    manifest_id: str = ""


@dataclass(frozen=True, slots=True)
class Difference:
    """One artifact the two states disagree on.

    Attributes:
        status: `new`, `modified`, or `orphaned`, comparing `after` with `before`.
        before, after: What each state records; None where the artifact is absent.
        properties: The names of the recorded fields that differ, for a modified artifact.
        member_diff: What differs member by member, for a modified artifact both states hold
            the definitions of; None when they were not compared.
    """

    key: str
    status: str
    before: ArtifactState | None
    after: ArtifactState | None
    properties: tuple[str, ...] = ()
    member_diff: tuple[MemberDiff, ...] | None = None

    @property
    def artifact_type(self) -> str:
        """Return the artifact's type, the kind part of its key."""
        return split_artifact_key(self.key)[0]

    @property
    def name(self) -> str:
        """Return the artifact's name, the name part of its key."""
        return split_artifact_key(self.key)[1]


def compare_states(before: Mapping[str, ArtifactState], after: Mapping[str, ArtifactState]) -> tuple[Difference, ...]:
    """Return every artifact the two states disagree on, in key order; empty when they agree."""
    classes = state_classes(
        {key: item.fingerprint for key, item in after.items()},
        {key: item.fingerprint for key, item in before.items()},
    )
    differing = sorted(classes[NEW] | classes[MODIFIED] | classes[ORPHANED])
    return tuple(_difference(key, before.get(key), after.get(key), classes) for key in differing)


def _difference(
    key: str, before: ArtifactState | None, after: ArtifactState | None, classes: Mapping[str, frozenset[str]]
) -> Difference:
    """Describe one differing key: its state, and for a modified one which recorded fields differ."""
    status = next(state for state in (NEW, MODIFIED, ORPHANED) if key in classes[state])
    properties: tuple[str, ...] = ()
    if before is not None and after is not None:
        # A target or manifest one state does not record is not a difference; a fingerprint is.
        known = tuple(field for field in ("target", "manifest_id") if getattr(before, field) and getattr(after, field))
        properties = tuple(
            field for field in ("fingerprint", *known) if getattr(before, field) != getattr(after, field)
        )
    return Difference(key, status, before, after, properties)


def member_differences(
    differences: tuple[Difference, ...],
    before: Mapping[str, Definition],
    after: Mapping[str, Definition],
    *,
    across_targets: bool,
) -> tuple[Difference, ...]:
    """Return the differences with each modified artifact's member diff, where both definitions are known.

    Across two targets, each publishing to its own databases, every fingerprint differs, so a
    modified artifact whose members all agree -- compared with physical names replaced by their
    labels -- is no difference and is left out. Within one target the fingerprint decides, and an
    artifact whose members all agree stays modified: what changed is what the members do not show.
    """
    kept = []
    for item in differences:
        if item.status != MODIFIED or item.key not in before or item.key not in after:
            kept.append(item)
            continue
        members = diff_definitions(before[item.key], after[item.key])
        if across_targets and not members:
            continue
        kept.append(replace(item, member_diff=members))
    return tuple(kept)
