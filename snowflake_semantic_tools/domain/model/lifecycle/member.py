"""The parts of an artifact two sides are compared by, and what a comparison found of each.

`domain.plan.member_diff` builds and lays out these; they live in the lifecycle model so a
`Change` can carry what its update changes, member by member.
"""

from __future__ import annotations

from dataclasses import dataclass

# A property value: one string, or a list of them.
Value = str | tuple[str, ...]

ADDED, REMOVED, CHANGED = "added", "removed", "changed"


@dataclass(frozen=True, slots=True)
class Member:
    """One part of an artifact, with the properties SST authors for it.

    Attributes:
        kind: What the member is, as a person names it: `metric`, `dimension`, `tool`, `file`.
        name: Its name as the desired side spells it, for display; matched casefolded.
        properties: Each property's name and value, in display order.
    """

    kind: str
    name: str
    properties: tuple[tuple[str, Value], ...] = ()

    @property
    def key(self) -> tuple[str, str]:
        """Return what two sides' members are matched by: the kind, and the name casefolded."""
        return self.kind, self.name.casefold()


@dataclass(frozen=True, slots=True)
class Definition:
    """Every member one side holds of one artifact, in display order.

    Raises:
        ValueError: two members share a kind and a casefolded name.
    """

    members: tuple[Member, ...] = ()

    def __post_init__(self) -> None:
        keys = [member.key for member in self.members]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise ValueError(f"a definition names {duplicates[0][0]} {duplicates[0][1]!r} twice")


@dataclass(frozen=True, slots=True)
class PropertyDiff:
    """One property a member's two sides disagree on: its value before, and after.

    For an added member `before` is None, and for a removed one `after` is.
    """

    name: str
    before: Value | None
    after: Value | None


@dataclass(frozen=True, slots=True)
class MemberDiff:
    """One member the two sides disagree on.

    Attributes:
        op: `added` when only the after side holds it, `removed` when only the before side
            does, `changed` when both do and a property both state differs.
        properties: The properties that differ: every property of an added or removed member.
    """

    kind: str
    name: str
    op: str
    properties: tuple[PropertyDiff, ...]

    @property
    def label(self) -> str:
        """Return the member as a person names it: `metric orders.total_revenue`."""
        return f"{self.kind} {self.name}"
