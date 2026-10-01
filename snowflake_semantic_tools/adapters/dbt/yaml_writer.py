"""Write enrich's column updates into a dbt model's YAML file, editing it in place.

A column's `sst` block is edited where it already is -- bare `meta.sst` wins over
`config.meta.sst`, as the manifest reader reads them -- and a new block goes under
`config.meta.sst`. Keys enrich adds take their place in `WRITTEN_KEYS` order; a model or column
the file does not describe yet is appended. Nothing else in the file changes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

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
from snowflake_semantic_tools.domain.model.enrich import WRITTEN_KEYS, ColumnUpdate


def _value(value: object) -> object:
    if isinstance(value, tuple):
        return block_list(value)
    if isinstance(value, str):
        return scalar(value)
    return value


def _child_map(parent: CommentedMap, key: str, path: str) -> CommentedMap:
    """Return the mapping under `key`, adding an empty one when the key is absent or empty."""
    child = parent.get(key)
    if child is None:
        child = CommentedMap()
        parent[key] = child
    if not isinstance(child, CommentedMap):
        raise ProjectError(f"{path}: {key} must be a mapping to hold SST metadata")
    return child


def _sst_block(column: CommentedMap, path: str) -> CommentedMap:
    """Return the column's `sst` mapping where the manifest reads it, creating it under config.meta."""
    meta = column.get("meta")
    if isinstance(meta, CommentedMap) and meta.get("sst") is not None:
        return _child_map(meta, "sst", path)
    config = _child_map(column, "config", path)
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
        raise ProjectError(f"{path}: {key} must be a list")
    return entries


def _model_entry(root: CommentedMap, model: str, path: str) -> CommentedMap:
    """Return the model's entry in `models:`, appending one when the file does not describe it."""
    models = _list(root, "models", path)
    entry = _named(models, model)
    if entry is None:
        entry = CommentedMap(name=model)
        models.append(entry)
    return entry


def _apply(entry: CommentedMap, updates: Sequence[ColumnUpdate], path: str) -> None:
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
        raise ProjectError(f"{path}: a dbt YAML file must be a mapping")
    return document


def write_model_updates(text: str | None, path: str, updates: Mapping[str, Sequence[ColumnUpdate]]) -> WrittenFile:
    """Apply each model's column updates to one dbt YAML file's text.

    Args:
        text: The file's text; None for a file enrich creates.
        updates: The column updates of each model the file describes, by model name.

    Raises:
        ProjectError: The file is not YAML, or a part enrich must edit has another shape.
    """
    document = _document(text, path)
    root = document.root
    assert isinstance(root, CommentedMap)
    for model, model_updates in updates.items():
        _apply(_model_entry(root, model, path), model_updates, path)
    return WrittenFile(document.dump(), document.reformats())
