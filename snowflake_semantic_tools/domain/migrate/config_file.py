"""`sst migrate split` for `sst_config.yml`: the blocks that left `skills:`, and the tool routes.

- `skills.catalog`'s keys move to the `skills:` root, and a `plugins:` block takes the same
  location (`+database`, `+schema`, `+bundle_stage`) with the root's `+certified` and
  `+version_prefix`, since the catalog published plugins too;
- `skills.stage` becomes the top-level `profiles:` block, and `skills.extensions` the top-level
  `extensions:` block;
- the always-fixed keys `+flatten`, `+prune_deleted`, `+auto_compress` and `+layout`, and the
  removed `+registry_table` of the catalog, go;
- a routed block's `+key` written with no value meant "unset", which is now an absent key, so
  it goes too;
- each tool group folder that becomes immutable gains a `tools.<folder>: {+immutable: true}`
  route.

A block that already exists at the top level is not overwritten: the move is reported instead.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import Edit, comments_above, entry_span, lines_edit, shift
from snowflake_semantic_tools.domain.migrate.outline import Entry, Node
from snowflake_semantic_tools.domain.migrate.refs import Rewrite
from snowflake_semantic_tools.domain.migrate.settings import delete_key

_FIXED = frozenset(("+flatten", "+prune_deleted", "+auto_compress", "+layout"))
_CATALOG_REMOVED = _FIXED | {"+registry_table"}
_PLUGIN_KEYS = ("+database", "+schema", "+bundle_stage")
_PLUGIN_ROOT_KEYS = ("+certified", "+version_prefix")
_ROUTED = ("semantic_views", "agents", "tools", "evals", "skills", "plugins", "coco_desktop_profiles")


def migrate_config(
    lines: Sequence[str], root: Node, routes: Sequence[tuple[str, ...]], *, has_plugins: bool
) -> Changes:
    """Record the rewrites of `sst_config.yml` that `sst migrate split` makes.

    Args:
        routes: The tool folders, as path parts under the tools directory, to route immutable.
        has_plugins: The project declares plugins, which published through the catalog.
    """
    changes = Changes()
    skills = root.entry("skills")
    moved: set[str] = set()
    if skills is not None and skills.value.is_map and not skills.value.flow:
        moved = _skills(lines, root, skills, changes, has_plugins=has_plugins)
    for entry in root.entries:
        if entry.key in _ROUTED and entry.value.is_map and not entry.value.flow:
            _nulls(lines, entry.value, changes, skip=moved if entry.key == "skills" else set())
    if routes:
        _routes(lines, root, routes, changes)
    return changes


def _skills(lines: Sequence[str], root: Node, skills: Entry, changes: Changes, *, has_plugins: bool) -> set[str]:
    """Move `catalog`, `stage` and `extensions` out of `skills:`; return the keys moved.

    When nothing else is left in `skills:`, the whole block is replaced by the blocks that
    left it, so no edit inside it is needed.
    """
    block = skills.value
    inner = Changes()
    blocks: list[str] = []
    handled: set[str] = set()
    remaining = [entry for entry in block.entries if entry.key not in ("catalog", "stage", "extensions")]
    catalog = block.entry("catalog")
    if catalog is not None:
        moved = _catalog(lines, block, catalog, inner)
        if moved is not None:
            handled.add("catalog")
            remaining.extend(moved)
            if has_plugins and root.entry("plugins") is None:
                blocks.append(_plugins(lines, block, catalog))
    for key, top in (("stage", "coco_desktop_profiles"), ("extensions", "extensions")):
        entry = block.entry(key)
        if entry is None:
            continue
        if root.entry(top) is not None or not entry.value.is_map or entry.value.flow:
            changes.leave(entry.line, entry.col, f"skills.{key}", f"move it to the top-level {top}: block by hand")
            continue
        handled.add(key)
        start = comments_above(lines, entry.line, entry.col)
        drop = _FIXED if key == "stage" else frozenset()
        body = "".join(_moved_entry(lines, child, 2, drop) for child in entry.value.entries)
        blocks.append("\n" + shift("".join(lines[start : entry.line]), -entry.col) + f"{top}:\n" + body)
        inner.add(lines_edit(start, entry_span(lines, entry)[1], ""), f"skills.{key}", top, "setting")
    changes.untouched.extend(inner.untouched)
    changes.rewrites.extend(inner.rewrites)
    if not remaining and handled:
        start = comments_above(lines, skills.line, skills.col)
        changes.edits.append(lines_edit(start, entry_span(lines, skills)[1], "".join(blocks).lstrip("\n")))
        return handled
    changes.edits.extend(inner.edits)
    if blocks:
        end = entry_span(lines, skills)[1]
        changes.edits.append(Edit(end, 0, end, 0, "".join(blocks)))
    return handled


def _catalog(lines: Sequence[str], skills: Node, catalog: Entry, changes: Changes) -> list[Entry] | None:
    """Move the catalog's keys to the `skills:` root; return them, or None when it cannot."""
    if not catalog.value.is_map or catalog.value.flow:
        changes.leave(catalog.line, catalog.col, "skills.catalog", "move its keys to skills: by hand")
        return None
    kept = [entry for entry in catalog.value.entries if entry.key not in _CATALOG_REMOVED and not entry.value.null]
    clash = [entry.key for entry in kept if skills.entry(entry.key) is not None]
    if clash:
        changes.leave(catalog.line, catalog.col, "skills.catalog", f"skills: already sets {', '.join(clash)}")
        return None
    start = comments_above(lines, catalog.line, catalog.col)
    text = "".join(lines[start : catalog.line]) + "".join(
        _moved_entry(lines, entry, catalog.col, _CATALOG_REMOVED) for entry in kept
    )
    changes.add(lines_edit(start, entry_span(lines, catalog)[1], text), "skills.catalog", "skills", "setting")
    return kept


def _moved_entry(lines: Sequence[str], entry: Entry, col: int, drop: frozenset[str]) -> str:
    """Return an entry with its comments, re-indented to `col`; '' for a key in `drop` or unset."""
    if entry.key in drop or entry.value.null:
        return ""
    start = comments_above(lines, entry.line, entry.col)
    return _ended(shift("".join(lines[start : entry_span(lines, entry)[1]]), col - entry.col))


def _plugins(lines: Sequence[str], skills: Node, catalog: Entry) -> str:
    """Return a `plugins:` block publishing where the catalog published plugins."""
    picked = [entry for key in _PLUGIN_KEYS if (entry := catalog.value.entry(key)) is not None]
    picked += [entry for key in _PLUGIN_ROOT_KEYS if (entry := skills.entry(key)) is not None]
    body = "".join(
        " " * 2 + lines[entry.line][entry.col :].rstrip("\n") + "\n" for entry in picked if not entry.value.null
    )
    return "\n# The skills catalog published plugins too; they keep its location.\nplugins:\n" + body


def _nulls(lines: Sequence[str], block: Node, changes: Changes, *, skip: set[str]) -> None:
    """Delete each `+key` written with no value, in a block and its folder routes."""
    for entry in block.entries:
        if entry.key in skip:
            continue
        if entry.key.startswith("+") and entry.value.null:
            delete_key(lines, entry, changes)
        elif not entry.key.startswith("+") and entry.value.is_map and not entry.value.flow:
            _nulls(lines, entry.value, changes, skip=set())


def _routes(lines: Sequence[str], root: Node, routes: Sequence[tuple[str, ...]], changes: Changes) -> None:
    """Add a `+immutable: true` route under `tools:` for each folder that has none."""
    tools = root.entry("tools")
    if tools is None:
        end = len(lines)
        changes.edits.append(Edit(end, 0, end, 0, "\ntools:\n" + "".join(_route_lines(route, 2) for route in routes)))
        changes.rewrites.extend(Rewrite(end + 1, 1, "", "tools." + ".".join(route), "setting") for route in routes)
        return
    if not tools.value.is_map or tools.value.flow or not tools.value.entries:
        changes.leave(
            tools.line, tools.col, "tools", "add the +immutable routes by hand: tools: is not a block mapping"
        )
        return
    for route in routes:
        node = tools.value
        depth = 0
        while depth < len(route) and (found := node.entry(route[depth])) is not None:
            if not found.value.is_map or found.value.flow or not found.value.entries:
                changes.leave(found.line, found.col, "tools." + ".".join(route), "add +immutable: true by hand")
                break
            node = found.value
            depth += 1
        else:
            if depth == len(route) and node.get("+immutable") is not None:
                continue
            end = entry_span(lines, node.entries[-1])[1]
            text = _route_lines(route[depth:], node.entries[0].col)
            changes.add(Edit(end, 0, end, 0, text), "", "tools." + ".".join(route), "setting")


def _route_lines(parts: tuple[str, ...], col: int) -> str:
    """Return nested route keys from `col`, ending in `+immutable: true`."""
    text = "".join(" " * (col + 2 * depth) + f"{part}:\n" for depth, part in enumerate(parts))
    return text + " " * (col + 2 * len(parts)) + "+immutable: true\n"


def _ended(text: str) -> str:
    return text if not text or text.endswith("\n") else text + "\n"
