"""Move an item's old settings under its `config:` block, comments and values as written.

An item -- a view, an agent, a tool member, an eval config -- once wrote its settings among its
content keys, some of them nested (`spec.models.orchestration`). Each `Move` names one such key
and the `config:` key it became. The moved lines keep their comments and their value's text,
re-indented; a parent left with nothing in it goes too, its own comments carried to the first key
moved out of it. The settings, in the order they were written, join an existing block `config:`
after its last key, or a new one written where the first of them was -- before the top-level key
that held it, when it was nested.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import (
    Edit,
    comments_above,
    entry_span,
    indent,
    is_blank,
    lines_edit,
    shift,
    value_end,
)
from snowflake_semantic_tools.domain.migrate.outline import Entry, Node
from snowflake_semantic_tools.domain.migrate.refs import Rewrite

_DEFAULT_UNIT = 2


@dataclass(frozen=True, slots=True)
class Move:
    """Move the setting at `path` within an item to `key` under its settings block.

    Attributes:
        value: YAML to write as the moved key's value in place of the one written; None keeps
            the value's text as written.
        drop: Delete the setting and write nothing in its place: its value has no new key.
    """

    path: tuple[str, ...]
    key: str
    value: str | None = None
    drop: bool = False


@dataclass(frozen=True, slots=True)
class _Found:
    move: Move
    chain: tuple[Entry, ...]

    @property
    def leaf(self) -> Entry:
        return self.chain[-1]

    @property
    def dropped(self) -> bool:
        return self.move.drop or self.leaf.value.null


def move_settings(lines: Sequence[str], item: Node, moves: Sequence[Move], changes: Changes, *, block: str) -> None:
    """Record the edits that move each setting `moves` names, found in `item`, under `block:`.

    A setting written with no value (`x:`, `x: null`) meant "unset", which is now an absent key,
    so it is only deleted. A setting is left where it is, and reported, when `block:` is not a
    block mapping or already sets the key, or the setting is not a plain block-style key on a
    line of its own. Nothing moves when none is found.
    """
    target = item.entry(block)
    found = [_Found(move, chain) for move in moves if (chain := _chain(item, move.path)) is not None]
    if not found:
        return
    if target is not None and (not target.value.is_map or target.value.flow or not target.value.entries):
        for entry in found:
            changes.leave(
                entry.leaf.line, entry.leaf.col, ".".join(entry.move.path), f"{block}: is not a block mapping"
            )
        return
    taken = {entry.key for entry in target.value.entries} if target is not None else set()
    kept: list[_Found] = []
    for entry in found:
        line = lines[entry.leaf.line]
        reason = None
        if entry.move.key in taken:
            reason = f"{block}.{entry.move.key} is already set"
        elif (
            any(link.value.flow for link in entry.chain[:-1])
            or line[entry.leaf.col :].split(":")[0] != entry.leaf.key
            or indent(lines[entry.chain[0].line]) != entry.chain[0].col
        ):
            reason = "the setting is not a plain block-style key on a line of its own"
        if reason is not None:
            changes.leave(entry.leaf.line, entry.leaf.col, ".".join(entry.move.path), reason)
        else:
            kept.append(entry)
    if kept:
        _record(lines, item, sorted(kept, key=lambda entry: entry.leaf.line), target, changes, block)


def delete_key(lines: Sequence[str], entry: Entry, changes: Changes) -> None:
    """Delete a key no reader takes any more, with the comments directly above it."""
    start = comments_above(lines, entry.line, entry.col)
    end = entry_span(lines, entry)[1]
    if end < len(lines) and is_blank(lines[end]) and (start == 0 or is_blank(lines[start - 1])):
        end += 1
    changes.add(lines_edit(start, end, ""), entry.key, "", "removed")


def _chain(item: Node, path: tuple[str, ...]) -> tuple[Entry, ...] | None:
    """Return the entries from the item down to the setting at `path`; None when one is missing."""
    chain: list[Entry] = []
    node: Node | None = item
    for key in path:
        entry = node.entry(key) if node is not None and node.is_map else None
        if entry is None:
            return None
        chain.append(entry)
        node = entry.value
    return tuple(chain)


def _record(
    lines: Sequence[str], item: Node, kept: list[_Found], target: Entry | None, changes: Changes, block: str
) -> None:
    """Delete each kept setting, or the parent it empties, and insert them all under `block:`."""
    moved = {(entry.leaf.line, entry.leaf.col) for entry in kept}
    col = target.value.entries[0].col if target is not None else item.col + _unit(item)
    deleted: dict[int, int] = {}
    carried: dict[tuple[int, int], list[str]] = {}
    emptied: set[tuple[int, int]] = set()
    for entry in kept:
        top = next((link for link in entry.chain[:-1] if _emptied(link, moved)), entry.leaf)
        start = comments_above(lines, top.line, top.col)
        # Each emptied parent's own comments travel with the first setting moved out of it;
        # every parent below an emptied one is emptied too.
        below = entry.chain[entry.chain.index(top) : -1] if top is not entry.leaf else ()
        for link in below:
            if (link.line, link.col) not in emptied:
                emptied.add((link.line, link.col))
                note = [
                    " " * col + line.lstrip() for line in lines[comments_above(lines, link.line, link.col) : link.line]
                ]
                carried.setdefault((entry.leaf.line, entry.leaf.col), []).extend(note)
        deleted.setdefault(start, entry_span(lines, top)[1])
    values = [entry for entry in kept if not entry.dropped]
    texts = [_moved_text(lines, entry, col, carried.get((entry.leaf.line, entry.leaf.col), [])) for entry in values]
    # One span per emptied parent or lone setting; a parent's span holds all it emptied.
    for start, end in sorted(deleted.items()):
        if end < len(lines) and is_blank(lines[end]) and (start == 0 or is_blank(lines[start - 1])):
            end += 1
        changes.edits.append(lines_edit(start, end, ""))
    if values:
        body = "".join(texts)
        if target is not None:
            at = value_end(lines, target.value, target.col)
        else:
            at, body = _new_block(lines, item, values, body, block)
        changes.edits.append(Edit(at, 0, at, 0, body))
    for entry in kept:
        before = ".".join(entry.move.path)
        after, kind = ("", "removed") if entry.dropped else (f"{block}.{entry.move.key}", "setting")
        changes.rewrites.append(Rewrite(entry.leaf.line + 1, entry.leaf.col + 1, before, after, kind))


def _emptied(entry: Entry, moved: set[tuple[int, int]]) -> bool:
    """Whether every key under a block mapping moves out, directly or through one it empties."""
    value = entry.value
    if not value.is_map or value.flow or not value.entries:
        return False
    return all((child.line, child.col) in moved or _emptied(child, moved) for child in value.entries)


def _moved_text(lines: Sequence[str], entry: _Found, col: int, carried: list[str]) -> str:
    """Return the setting's comments and lines, renamed to its new key and indented to `col`."""
    leaf = entry.leaf
    start = comments_above(lines, leaf.line, leaf.col)
    end = entry_span(lines, leaf)[1]
    first = lines[leaf.line]
    if entry.move.value is not None:
        block = [f"{' ' * leaf.col}{entry.move.key}: {entry.move.value}\n"]
    else:
        block = [first[: leaf.col] + entry.move.key + first[leaf.col + len(leaf.key) :], *lines[leaf.line + 1 : end]]
    text = shift("".join([*lines[start : leaf.line], *block]), col - leaf.col)
    return "".join(line if line.endswith("\n") else line + "\n" for line in [*carried, *text.splitlines(True)])


def _new_block(lines: Sequence[str], item: Node, kept: list[_Found], body: str, block: str) -> tuple[int, str]:
    """Return where a new `block:` goes, and its text: header, then the moved settings."""
    header = f"{' ' * item.col}{block}:\n"
    first = kept[0]
    if len(first.chain) == 1:
        return comments_above(lines, first.leaf.line, first.leaf.col), header + body
    holder = first.chain[0]
    at = comments_above(lines, holder.line, holder.col)
    spacer = "\n" if at > 0 and is_blank(lines[at - 1]) else ""
    return at, header + body + spacer


def _unit(item: Node) -> int:
    """Return how far the item indents what it nests, from its first nested block; else 2."""
    for entry in item.entries:
        value = entry.value
        if value.flow:
            continue
        if value.is_map and value.entries:
            return value.entries[0].col - entry.col
        if value.is_seq and value.items:
            return max(value.col - entry.col, _DEFAULT_UNIT)
    return _DEFAULT_UNIT
