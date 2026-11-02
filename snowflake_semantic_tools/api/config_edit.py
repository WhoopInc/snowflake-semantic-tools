"""Edit `sst_config.yml` by key path, keeping every comment, blank line and quote the file has.

An editor changes one setting at a time, a per-target value, a folder route's key, a variable,
and needs the file written back as a person would: the key set in place, or added where the
configuration reference orders it, and a folder route that loses its last key gone with it. A
per-target value is written in the grammar `domain.resolve.target_value` reads, so what an editor
shows of it and what a run resolves it to are the same.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import (
    CommentedMap,
    block_list,
    insert_key,
    load_editable,
    mapping_root,
    new_editable,
    ordered_mapping,
    scalar,
)
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.config_schema import CHILDREN
from snowflake_semantic_tools.domain.resolve.target_value import TargetValue, format_target_value

CONFIG_FILE = "sst_config.yml"
# What an edit may set a key to: text, a flag, a number, or a list of text.
Value = str | bool | int | float | Sequence[str]


def edit_config(text: str, path: Sequence[str], value: Value | None) -> str:
    """Return `text` with the key at `path` set to `value`, or removed when `value` is None.

    Mappings along `path` that are missing are added; a key is added before the first key the
    configuration reference lists after it. Removing a key removes each mapping it leaves empty,
    its top-level block included: an empty mapping says nothing a missing one does not, and a
    folder route's last key takes the route.

    Raises:
        ProjectError: the text is not YAML, its top is not a mapping, or a key along `path`
            holds something other than a mapping.

    Diagnostics:
        SST-LOD001: the text does not parse, or a key along `path` is not a mapping.
    """
    if not path:
        raise ValueError("a key path names at least one key")
    document = load_editable(text, CONFIG_FILE) if text.strip() else None
    root = mapping_root(document) if document is not None else ordered_mapping(())
    if root is None:
        raise _refused(path, "the file's top is not a mapping")
    if value is None:
        _remove(root, path)
    else:
        insert_key(_parent(root, path, create=True), path[-1], _yaml_value(value), _order(path))
    return (document or new_editable(root)).dump()


def edit_target_value(text: str, path: Sequence[str], value: TargetValue) -> str:
    """Return `text` with the per-target value at `path` written as `value`, in the resolver's grammar."""
    return edit_config(text, path, format_target_value(value))


def _parent(root: CommentedMap, path: Sequence[str], *, create: bool) -> CommentedMap:
    """The mapping holding the last key of `path`, adding missing ones when `create`."""
    current = root
    for depth, key in enumerate(path[:-1]):
        found = current.get(key)
        if found is None and create:
            insert_key(current, key, ordered_mapping(()), _order(path[: depth + 1]))
            found = current[key]
        if not isinstance(found, CommentedMap):
            raise _refused(path, f"{'.'.join(path[: depth + 1])} is not a mapping")
        current = found
    return current


def _remove(root: CommentedMap, path: Sequence[str]) -> None:
    """Remove the key at `path`, then each mapping it leaves empty, deepest first."""
    chain: list[CommentedMap] = [root]
    for key in path[:-1]:
        found = chain[-1].get(key)
        if not isinstance(found, CommentedMap):
            return
        chain.append(found)
    chain[-1].pop(path[-1], None)
    for depth in range(len(path) - 1, 0, -1):
        if chain[depth]:
            return
        chain[depth - 1].pop(path[depth - 1])


def _order(path: Sequence[str]) -> tuple[str, ...]:
    """The keys the reference lists beside the last key of `path`, in its order.

    A folder route takes its block's keys, so a key below a route is ordered as its block orders it.
    """
    parent = ".".join(path[:-1])
    if parent not in CHILDREN:
        parent = path[0]
    children: Mapping[str, object] = CHILDREN.get(parent, {})
    return tuple(name for name in children if not name.startswith("<"))


def _yaml_value(value: Value) -> object:
    if isinstance(value, str):
        return scalar(value)
    if isinstance(value, (bool, int, float)):
        return value
    return block_list(tuple(value))


def _refused(path: Sequence[str], why: str) -> ProjectError:
    diagnostic = D("SST-LOD001", file=CONFIG_FILE, line=1, col=1, detail=f"cannot set {'.'.join(path)}: {why}")
    return ProjectError(diagnostic.message, diagnostics=(diagnostic,))
