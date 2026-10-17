"""Write enrich's updates into a dbt model's YAML file, editing it in place.

A model's or column's `sst` block is edited where it already is -- bare `meta.sst` wins over
`config.meta.sst`, as the manifest reader reads them -- and a new block goes under
`config.meta.sst`, which a model gets before its `columns`. A column's keys take their place in
`WRITTEN_KEYS` order, and a model's `synonyms` theirs in `MODEL_KEYS` order; a model or column
the file does not describe yet is appended. Nothing else in the file changes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import NoReturn

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import (
    CommentedMap,
    CommentedSeq,
    EditableYaml,
    WrittenFile,
    block_list,
    insert_key,
    load_editable,
    new_editable,
    scalar,
)
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.enrich import WRITTEN_KEYS, ColumnUpdate

# The order of the model-level `meta.sst` keys, so the table synonyms enrich adds sit after keys.
MODEL_KEYS = ("primary_key", "unique_keys", "synonyms", "distinct_range")


def _value(value: object) -> object:
    if isinstance(value, tuple):
        return block_list(value)
    if isinstance(value, str):
        return scalar(value)
    return value


def _misshapen(path: str, field: str, expected: str) -> NoReturn:
    """Refuse to edit a dbt YAML file whose part enrich writes into has another shape.

    Diagnostics:
        SST-DBT034: `field` in the file at `path` is not `expected`; raised.
    """
    diagnostic = D("SST-DBT034", path=path, field=field, expected=expected)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _child_map(parent: CommentedMap, key: str, path: str) -> CommentedMap:
    """Return the mapping under `key`, adding an empty one when the key is absent or empty."""
    child = parent.get(key)
    if child is None:
        child = CommentedMap()
        parent[key] = child
    if not isinstance(child, CommentedMap):
        _misshapen(path, key, "a mapping")
    return child


def _sst_block(node: CommentedMap, path: str) -> CommentedMap:
    """Return the model's or column's `sst` mapping where the manifest reads it, else a new one.

    A new one goes under `config.meta`; a new `config` goes before the node's `columns`.
    """
    meta = node.get("meta")
    if isinstance(meta, CommentedMap) and meta.get("sst") is not None:
        return _child_map(meta, "sst", path)
    if "config" not in node and "columns" in node:
        node.insert(list(node).index("columns"), "config", CommentedMap())
    config = _child_map(node, "config", path)
    return _child_map(_child_map(config, "meta", path), "sst", path)


def _named(entries: CommentedSeq, name: str) -> CommentedMap | None:
    wanted = name.casefold()
    return next(
        (
            entry
            for entry in entries
            if isinstance(entry, CommentedMap) and str(entry.get("name") or "").casefold() == wanted
        ),
        None,
    )


def _list(parent: CommentedMap, key: str, path: str) -> CommentedSeq:
    entries = parent.get(key)
    if entries is None:
        entries = CommentedSeq()
        parent[key] = entries
    if not isinstance(entries, CommentedSeq):
        _misshapen(path, key, "a list")
    return entries


def _model_entry(root: CommentedMap, model: str, path: str, section: str = "models") -> CommentedMap:
    """Return the model's entry under `section`, appending one when the file does not describe it.

    `section` is `models`, or `snapshots` or `seeds` for a snapshot or seed.
    """
    entries = _list(root, section, path)
    entry = _named(entries, model)
    if entry is None:
        entry = CommentedMap(name=model)
        entries.append(entry)
    return entry


def _apply(entry: CommentedMap, updates: Sequence[ColumnUpdate] | None, synonyms: Sequence[str], path: str) -> None:
    """Set the model's table synonyms, then make its column updates; None leaves its columns alone."""
    if synonyms:
        insert_key(_sst_block(entry, path), "synonyms", block_list(tuple(synonyms)), MODEL_KEYS)
    if updates is None:
        return
    columns = _list(entry, "columns", path)
    for update in updates:
        column = _named(columns, update.name)
        if column is None:
            column = CommentedMap(name=scalar(update.name))
            columns.append(column)
        sst = _sst_block(column, path)
        for key, value in update.values:
            insert_key(sst, key, _value(value), WRITTEN_KEYS)


def _document(text: str | None, path: str) -> EditableYaml:
    if text is None or not text.strip():
        return new_editable(CommentedMap(version=2))
    document = load_editable(text, path)
    if not isinstance(document.root, CommentedMap):
        _misshapen(path, "its root", "a mapping")
    return document


def write_model_updates(
    text: str | None,
    path: str,
    updates: Mapping[str, Sequence[ColumnUpdate]],
    synonyms: Mapping[str, Sequence[str]] | None = None,
    sections: Mapping[str, str] | None = None,
) -> WrittenFile:
    """Apply each model's column updates and table synonyms to one dbt YAML file's text.

    Args:
        text: The file's text; None for a file enrich creates.
        updates: The column updates of each model the file describes, by model name.
        synonyms: The table synonyms of each model the file describes, by model name.
        sections: The top-level key each model is described under, by model name; `models`
            when absent, `snapshots` or `seeds` for a snapshot or seed.

    Raises:
        ProjectError: The file is not YAML (SST-LOD001), or a part enrich must edit has another
            shape (SST-DBT034).
    """
    document = _document(text, path)
    root = document.root
    assert isinstance(root, CommentedMap)
    synonyms = synonyms or {}
    sections = sections or {}
    for model in dict.fromkeys((*updates, *synonyms)):
        entry = _model_entry(root, model, path, sections.get(model, "models"))
        _apply(entry, updates.get(model), synonyms.get(model, ()), path)
    return WrittenFile(document.dump(), document.reformats())
