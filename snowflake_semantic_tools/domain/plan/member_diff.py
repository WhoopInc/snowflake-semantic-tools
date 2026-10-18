"""Compare two definitions of one artifact member by member, as `sst plan` and `sst diff` show them.

A `Definition` lists what an artifact is made of -- a semantic view's tables, metrics and
dimensions, an agent's instruction sections and tools, a skill's files -- each `Member` with the
properties SST authors, already in one comparable form. `diff_definitions` reports each member
only one side holds, and for a member both hold, each property whose values differ.

A property only one side states is never compared. The live side states a property only when
it read one for the member, or knows the read shows nothing for an empty one; a property it
cannot read, such as one Snowflake's DESCRIBE does not report, is left out rather than taken for
empty, so it can never look removed. What the comparison cannot see is not shown as changed:
`sst plan` then says only that the definition changed.

`property_lines` and `unified_text` lay a diff out as a person reads it: a scalar as its old and
new value, a list item by item, a list of sample values as a count, and text as a line diff.
Everything a reader is shown passes `printable`, so text read from Snowflake cannot carry a
terminal escape sequence. `normalize` replaces the physical names in a definition with logical
labels, so two targets that publish the same project to different databases compare equal.
"""

from __future__ import annotations

import difflib
import re
from collections.abc import Callable, Iterable, Mapping, Sequence

from snowflake_semantic_tools.domain.model.lifecycle.member import (
    ADDED,
    CHANGED,
    REMOVED,
    Definition,
    Member,
    MemberDiff,
    PropertyDiff,
    Value,
)

__all__ = [
    "ADDED",
    "CHANGED",
    "COUNTED",
    "REMOVED",
    "Definition",
    "Member",
    "MemberDiff",
    "NameMap",
    "PropertyDiff",
    "Value",
    "diff_definitions",
    "normalize",
    "printable",
    "property_lines",
    "summary",
    "unified_text",
    "unique",
]

# Properties shown as a count rather than item by item: their items are refreshed data.
COUNTED = frozenset(("sample_values",))
# C0 and C1 control characters, but for tab and newline, and DEL: none reach a reader.
_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def diff_definitions(before: Definition, after: Definition) -> tuple[MemberDiff, ...]:
    """Return every member the two definitions disagree on; empty when they agree.

    Members are listed in the after side's order, then those only the before side holds, in its
    order. A member both hold is changed only when a property both state differs; a property
    only one side states is not compared, and nor is a member of a kind one side did not read.
    """
    read = _kinds_read(before, after)
    earlier = {member.key: member for member in before.members if read(member)}
    later = {member.key for member in after.members if read(member)}
    diffs: list[MemberDiff] = []
    for member in after.members:
        if not read(member):
            continue
        previous = earlier.get(member.key)
        if previous is None:
            diffs.append(_whole(member, ADDED))
            continue
        old = dict(previous.properties)
        changed = tuple(
            PropertyDiff(name, old[name], value)
            for name, value in member.properties
            if name in old and old[name] != value
        )
        if changed:
            diffs.append(MemberDiff(member.kind, member.name, CHANGED, changed))
    diffs.extend(_whole(member, REMOVED) for member in before.members if read(member) and member.key not in later)
    return tuple(diffs)


def _kinds_read(before: Definition, after: Definition) -> Callable[[Member], bool]:
    """Whether both sides read a member's kind."""
    return lambda member: all(side.kinds is None or member.kind in side.kinds for side in (before, after))


def _whole(member: Member, op: str) -> MemberDiff:
    """A member only one side holds, with every property it states on that side."""
    properties = tuple(
        PropertyDiff(name, None, value) if op == ADDED else PropertyDiff(name, value, None)
        for name, value in member.properties
        if value not in ("", ())
    )
    return MemberDiff(member.kind, member.name, op, properties)


def unique(members: Iterable[Member]) -> tuple[Member, ...]:
    """Return the members, a name its kind already holds suffixed `#2`, `#3`: a source may repeat one."""
    seen: dict[tuple[str, str], int] = {}
    kept = []
    for member in members:
        count = seen[member.key] = seen.get(member.key, 0) + 1
        kept.append(member if count == 1 else Member(member.kind, f"{member.name}#{count}", member.properties))
    return tuple(kept)


def summary(diffs: Sequence[MemberDiff]) -> tuple[str, ...]:
    """Return each differing member as one short phrase, `+`/`-` marking an added or removed one.

    Example:
        `("metric orders.total_revenue", "+dimension customers.signup_channel")`.
    """
    marks = {ADDED: "+", REMOVED: "-", CHANGED: ""}
    return tuple(f"{marks[item.op]}{item.label}" for item in diffs)


def printable(text: str) -> str:
    """Return `text` without control characters but tab and newline, so it cannot drive a terminal."""
    return _CONTROL.sub("", text)


def property_lines(diff: PropertyDiff) -> tuple[tuple[str, str], ...]:
    """Return a property's difference as `(sign, text)` lines: `-` old, `+` new, ` ` context, `~` note.

    A list of sample values is one `~` line counting its items. Any other list is shown item
    by item, or as one `~` line when only its order changed. A value holding a newline on
    either side is a line diff with one line of context, `...` between distant hunks. Any other
    value is its old line and its new one; an empty one reads `(none)`.
    """
    before, after = diff.before, diff.after
    if diff.name in COUNTED and _is_list(before, after):
        return (("~", f"{_count(before)} -> {_count(after)} values"),)
    if _is_list(before, after):
        return _list_lines(_items(before), _items(after))
    old, new = _text(before), _text(after)
    if "\n" in old or "\n" in new:
        return _text_lines(old, new)
    lines: list[tuple[str, str]] = []
    if before is not None:
        lines.append(("-", printable(old) or "(none)"))
    if after is not None:
        lines.append(("+", printable(new) or "(none)"))
    return tuple(lines)


def _is_list(*values: Value | None) -> bool:
    return any(isinstance(value, tuple) for value in values)


def _items(value: Value | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return value if isinstance(value, tuple) else (value,)


def _count(value: Value | None) -> int:
    return len(_items(value))


def _text(value: Value | None) -> str:
    return "" if value is None else value if isinstance(value, str) else ", ".join(value)


def _list_lines(old: tuple[str, ...], new: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    removed = [item for item in old if item not in new]
    added = [item for item in new if item not in old]
    if not removed and not added:
        return (("~", "reordered"),)
    return (*(("-", printable(item)) for item in removed), *(("+", printable(item)) for item in added))


def _text_lines(old: str, new: str) -> tuple[tuple[str, str], ...]:
    lines: list[tuple[str, str]] = []
    hunks = 0
    for line in difflib.unified_diff(old.splitlines(), new.splitlines(), n=1, lineterm=""):
        if line.startswith(("---", "+++")):
            continue
        if line.startswith("@@"):
            if hunks:
                lines.append((" ", "..."))
            hunks += 1
            continue
        lines.append((line[:1], printable(line[1:])))
    return tuple(lines)


def unified_text(
    sections: Iterable[tuple[str, str, Sequence[MemberDiff]]],
    *,
    before_side: str = "live",
    after_side: str = "desired",
) -> str:
    """Return the diffs as a unified diff a person or an editor reads: one section per artifact.

    Each section is `(artifact key, what it publishes to, its member diffs)`; one with no
    diffs is left out. A member's hunk is headed `@@ <kind> <name> @@`, and each property it
    changes is named on a line of its own before its lines.

    Example:
        `--- live/semantic_view:sales  SALES_DB.SEMANTIC_VIEWS.SALES`,
        `+++ desired/semantic_view:sales`, `@@ metric orders.total_revenue @@`,
        `   comment:`, `-  Revenue before tax.`, `+  Revenue before tax, in dollars.`
    """
    out: list[str] = []
    for key, target, diffs in sections:
        if not diffs:
            continue
        out.append(printable(f"--- {before_side}/{key}" + (f"  {target}" if target else "")))
        out.append(printable(f"+++ {after_side}/{key}"))
        for item in diffs:
            out.append(printable(f"@@ {item.op} {item.label} @@"))
            for prop in item.properties:
                out.append(printable(f"   {prop.name}:"))
                out.extend(f"{sign}  {text}" for sign, text in property_lines(prop))
    return "\n".join(out) + ("\n" if out else "")


# A physical object name, folded, mapped to the label that stands for it in every target.
NameMap = Mapping[tuple[str, str, str], str]


def normalize(definition: Definition, names: NameMap) -> Definition:
    """Return the definition with each physical name `names` maps replaced by its label.

    A value that is one qualified name is replaced whole; inside any other text, each mapped
    name written unquoted is replaced where it appears as a whole name, not as the end of a
    longer dotted one. A name `names` does not map, such as one a procedure body hard-codes, is
    kept as written.
    """
    if not names:
        return definition
    by_text = {".".join(parts): label for parts, label in names.items()}
    pattern = re.compile(
        r"(?<![\w$.])(" + "|".join(re.escape(text) for text in sorted(by_text, key=len, reverse=True)) + r")(?![\w$])",
        re.IGNORECASE,
    )

    def mapped(value: str) -> str:
        whole = by_text.get(_folded_text(value))
        if whole is not None:
            return whole
        return pattern.sub(lambda match: by_text[match.group(1).upper()], value)

    def value_of(value: Value) -> Value:
        return tuple(mapped(item) for item in value) if isinstance(value, tuple) else mapped(value)

    return Definition(
        tuple(
            Member(
                member.kind, mapped(member.name), tuple((name, value_of(value)) for name, value in member.properties)
            )
            for member in definition.members
        ),
        definition.kinds,
    )


def _folded_text(value: str) -> str:
    """The text of `value` as three unquoted dotted parts, upper-cased; empty when it is not one."""
    parts = value.strip().split(".")
    if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z_][\w$]*", part) for part in parts):
        return ""
    return ".".join(part.upper() for part in parts)
