"""Write table synonyms into semantic view files, editing them in place.

A view file holds `{{ ref() }}` and other templates that are not YAML, so each is replaced by
a placeholder before the file is loaded, exactly as the loader does, and put back after it is
written. A table's synonyms go to its entry in the view's `table_config`, which is added after
`tables` when the view has none.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import (
    CommentedMap,
    CommentedSeq,
    WrittenFile,
    block_list,
    load_editable,
)
from snowflake_semantic_tools.adapters.yaml.parse import _neutralize_templates
from snowflake_semantic_tools.domain.enrich import TableSynonymEdit


def _view(root: object, name: str, path: str) -> CommentedMap:
    views = root.get("semantic_views") if isinstance(root, CommentedMap) else None
    if not isinstance(views, CommentedSeq):
        raise ProjectError(f"{path}: has no semantic_views list to write table synonyms into")
    wanted = name.casefold()
    for view in views:
        if isinstance(view, CommentedMap) and str(view.get("name") or "").casefold() == wanted:
            return view
    raise ProjectError(f"{path}: declares no semantic view '{name}'")


def _table_config(view: CommentedMap, path: str) -> CommentedMap:
    config = view.get("table_config")
    if config is None:
        config = CommentedMap()
        keys = list(view)
        position = keys.index("tables") + 1 if "tables" in keys else len(keys)
        view.insert(position, "table_config", config)
    if not isinstance(config, CommentedMap):
        raise ProjectError(f"{path}: table_config of view '{view.get('name')}' must be a mapping")
    return config


def _table_entry(config: CommentedMap, table: str, path: str) -> CommentedMap:
    wanted = table.casefold()
    key = next((existing for existing in config if str(existing).casefold() == wanted), table)
    entry = config.get(key)
    if entry is None:
        entry = CommentedMap()
        config[key] = entry
    if not isinstance(entry, CommentedMap):
        raise ProjectError(f"{path}: table_config.{key} must be a mapping")
    return entry


def write_table_synonyms(text: str, path: str, edits: Sequence[TableSynonymEdit]) -> WrittenFile:
    """Apply table-synonym edits to one semantic view file's text.

    Raises:
        ProjectError: The file has a malformed template, is not YAML, lacks a view an edit
            names, or holds `table_config` in another shape.
    """
    neutralized, templates = _neutralize_templates(text, path)
    document = load_editable(neutralized, path)
    for edit in edits:
        entry = _table_entry(_table_config(_view(document.root, edit.view, path), path), edit.table, path)
        entry["synonyms"] = block_list(edit.synonyms)
    written = document.dump()
    for placeholder, source in templates.items():
        written = written.replace(placeholder, source.raw)
    return WrittenFile(written, document.reformats())
