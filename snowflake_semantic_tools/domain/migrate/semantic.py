"""`sst migrate refs` for semantic model files: bare references, non-additive entries, view settings.

The reference pass writes each bare name as the call its field takes: `using_relationships` and a
view's `relationships` / `exclude_relationships` as `relationship()`, its `metrics` /
`exclude_metrics` as `metric()`, and a tag's `name` as `tag()`. A view's `columns` /
`exclude_columns` entry `table.column` becomes `ref('table', 'column')`, and a bare `column` the
same call of the one model the view's `tables:` names; any other entry without a template call
is reported and left alone. The structure pass turns each `non_additive_dimensions` entry's
`dimension` (and `table`) into one `ref: "{{ ref('<model>', '<column>') }}"`, renames a view's
`dimensions:`, which SST 0.3.2 read as `columns:`, to `columns:` when it has none, and moves a
view's `enabled`, `tags` and `max_staleness` under its `config:`.

A non-additive entry without `table:` named a dimension of its metric's own table, the one model
the metric's expression names with `ref()`, as SST 0.3 and the 1.0 drafts resolved it; a metric
naming no model or several is reported and left alone.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence

from snowflake_semantic_tools.domain.migrate.calls import bare, quoted_call
from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import Edit, comments_above, entry_span, lines_edit
from snowflake_semantic_tools.domain.migrate.outline import Entry, Node
from snowflake_semantic_tools.domain.migrate.settings import Move, move_settings
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.parse.records import singular_roots
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls

# Each list root a semantic file may hold, by its singular root.
LIST_ROOTS = {singular: root for singular, root in singular_roots(SEMANTIC_REGISTRY).items() if root != "tools"}
VIEW_MOVES = (Move(("enabled",), "enabled"), Move(("tags",), "tags"), Move(("max_staleness",), "max_staleness"))
# A view's scope keys, with the function each entry is a call of.
_SCOPE = (
    ("relationships", "relationship"),
    ("exclude_relationships", "relationship"),
    ("metrics", "metric"),
    ("exclude_metrics", "metric"),
)
# A view's column scope keys, and the key SST 0.3.2 also read as `columns`.
_COLUMN_SCOPE = ("columns", "exclude_columns")
_DIMENSIONS = "dimensions"
_NAME = r"[A-Za-z_][A-Za-z0-9_$]*"
_COLUMN = re.compile(rf"(?:(?P<table>{_NAME})\.)?(?P<column>{_NAME})")
_COLUMN_CALL = "\"{{ ref('<model>', '<column>') }}\""


def members(root: Node, list_root: str) -> Iterator[Node]:
    """Yield each mapping the document declares under `list_root` or its singular root."""
    singular = next(key for key, value in LIST_ROOTS.items() if value == list_root)
    for entry in root.entries:
        if entry.key == list_root:
            yield from entry.value.maps()
        elif entry.key == singular and entry.value.is_map:
            yield entry.value


def bare_items(sequence: Node | None, function: str, changes: Changes) -> None:
    """Write each bare scalar item of a sequence as one call of `function`."""
    for item in sequence.items if sequence is not None and sequence.is_seq else ():
        name = bare(item.text)
        if name is not None:
            changes.replace(item, quoted_call(function, name), "reference")


def tag_names(tags: Node | None, changes: Changes) -> None:
    """Write each bare tag `name:` of a `tags:` list as a `tag()` call."""
    for entry in tags.maps() if tags is not None and tags.is_seq else ():
        name = entry.get("name")
        text = bare(name.text) if name is not None else None
        if name is not None and text is not None:
            changes.replace(name, quoted_call("tag", text), "reference")


def semantic_references(root: Node) -> Changes:
    """Record the reference pass's rewrites for one semantic model file."""
    changes = Changes()
    for metric in members(root, "snowflake_metrics"):
        bare_items(metric.get("using_relationships"), "relationship", changes)
    for view in members(root, "semantic_views"):
        for key, function in _SCOPE:
            bare_items(view.get(key), function, changes)
        model = _view_model(view)
        for key in _COLUMN_SCOPE if view.get("columns") is not None else (*_COLUMN_SCOPE, _DIMENSIONS):
            _column_items(view.get(key), model, changes)
        tag_names(view.get("tags"), changes)
        tag_names(view.find("config", "tags"), changes)
    return changes


def semantic_structure(lines: Sequence[str], root: Node) -> Changes:
    """Record the structure pass's rewrites for one semantic model file."""
    changes = Changes()
    for metric in members(root, "snowflake_metrics"):
        entries = metric.get("non_additive_dimensions")
        for entry in entries.maps() if entries is not None and entries.is_seq else ():
            _non_additive(lines, metric, entry, changes)
    for view in members(root, "semantic_views"):
        _dimensions_as_columns(lines, view, changes)
        move_settings(lines, view, VIEW_MOVES, changes, block="config")
    return changes


def metric_model(metric: Node) -> str | None:
    """Return the one model a metric's expression names with `ref()`; None for none or several."""
    expr = metric.get("expr")
    try:
        calls = scan_template_calls(expr.text or "") if expr is not None else ()
    except TemplateSyntaxError:
        return None
    models: dict[str, str] = {}
    for call in calls:
        if call.function == "ref" and call.args:
            models.setdefault(call.args[0].casefold(), call.args[0])
    return next(iter(models.values())) if len(models) == 1 else None


def _model_of(table: Node | None, metric: Node) -> str | None:
    """Return the model a non-additive entry's `table:` names, as a bare name or a `ref()`."""
    if table is None:
        return metric_model(metric)
    return _named_model(table.text or "", ("ref",))


def _named_model(text: str, functions: tuple[str, ...]) -> str | None:
    """Return the model `text` names, bare or as a one-argument call of one of `functions`."""
    name = bare(text)
    if name is not None:
        return name
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return None
    one = len(calls) == 1 and calls[0].function in functions and len(calls[0].args) == 1
    return calls[0].args[0] if one else None


def _view_model(view: Node) -> str | None:
    """Return the one model a view's `tables:` names, bare or by `ref()` or `table()`; None otherwise."""
    tables = view.get("tables")
    items = tables.items if tables is not None and tables.is_seq else ()
    named = [_named_model(item.text or "", ("ref", "table")) for item in items]
    found = {name.casefold(): name for name in named if name is not None}
    return next(iter(found.values())) if len(found) == 1 and None not in named else None


def _column_items(sequence: Node | None, model: str | None, changes: Changes) -> None:
    """Write each `table.column` or bare `column` entry of a view's column scope as a `ref()`.

    An entry already a template call is the reference pass's or validation's. A bare column needs
    the view's one model; that, and any other entry, is reported and left alone.
    """
    for item in sequence.items if sequence is not None and sequence.is_seq else ():
        text = item.text
        if text is not None and "{{" in text:
            continue
        name = bare(text)
        found = _COLUMN.fullmatch(name) if name is not None else None
        table = (found["table"] or model) if found is not None else None
        if found is not None and table is not None:
            changes.replace(item, quoted_call("ref", table, found["column"]), "reference")
            continue
        reason = "a bare column in a view whose tables: name no single model" if found else "not a column name"
        changes.leave(item.line, item.col, text or "", f"{reason}; write {_COLUMN_CALL} by hand")


def _dimensions_as_columns(lines: Sequence[str], view: Node, changes: Changes) -> None:
    """Rename a view's `dimensions:` to `columns:`, or report it when the view has both."""
    entry = view.entry(_DIMENSIONS)
    if entry is None:
        return
    if view.get("columns") is not None:
        changes.leave(entry.line, entry.col, _DIMENSIONS, "the view has columns: too; merge them by hand")
        return
    end = entry.col + len(_DIMENSIONS)
    if lines[entry.line][entry.col : end] != _DIMENSIONS:
        changes.leave(entry.line, entry.col, _DIMENSIONS, "a quoted key; rename it to columns: by hand")
        return
    changes.add(Edit(entry.line, entry.col, entry.line, end, "columns"), _DIMENSIONS, "columns", "setting")


def _non_additive(lines: Sequence[str], metric: Node, entry: Node, changes: Changes) -> None:
    """Write one `{dimension, table?}` entry as `ref:`, keeping its sort keys as written."""
    dimension = entry.entry("dimension")
    if dimension is None:
        return
    table = entry.entry("table")
    column = bare(dimension.value.text)
    model = _model_of(table.value if table is not None else None, metric)
    if column is None or model is None:
        reason = "the metric names no single model" if column is not None else "the dimension is not a bare column"
        changes.leave(dimension.line, dimension.col, dimension.value.text or "", f"{reason}; write ref: by hand")
        return
    after = "ref: " + quoted_call("ref", model, column)
    if entry.flow:
        _flow_entry(lines, entry, dimension, after, changes)
        return
    written = sorted((link for link in (dimension, table) if link is not None), key=lambda link: link.line)
    first = written[0]
    changes.add(
        Edit(first.line, first.col, first.value.end_line, first.value.end_col, after), first.key, after, "non_additive"
    )
    for other in written[1:]:
        start = comments_above(lines, other.line, other.col)
        changes.edits.append(lines_edit(start, entry_span(lines, other)[1], ""))


def _flow_entry(lines: Sequence[str], entry: Node, dimension: Entry, after: str, changes: Changes) -> None:
    """Rewrite a one-line flow entry `{dimension: x, ...}` as `{ref: ..., ...}`."""
    if entry.line != entry.end_line:
        changes.leave(entry.line, entry.col, "non_additive_dimensions", "a flow entry over several lines")
        return
    line = lines[entry.line]
    kept = [line[item.col : item.value.end_col] for item in entry.entries if item.key not in ("dimension", "table")]
    text = "{" + ", ".join([after, *kept]) + "}"
    changes.add(Edit(entry.line, entry.col, entry.end_line, entry.end_col, text), dimension.key, after, "non_additive")
