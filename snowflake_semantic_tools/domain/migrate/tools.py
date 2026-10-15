"""`sst migrate refs` for tool files: columns as refs, `body_file`, stage paths and settings.

A tool file holds tool groups (`tools: [{group, define: [...], reference: [...]}]`, as SST read
them before groups went) or members (`tool:`, or a `tools:` list). Either way each member is
rewritten in place; `sst migrate split` moves group members into files of their own, in either
order. A member is defined when it sits under `define:`, or, outside a group, writes no
`relations:`.

For a defined member, every column it names -- `search_column`, `attribute_columns`,
`on.search_column`, `on.attributes`, `id_column`, `title_column`, `relative_path_column` --
becomes `{{ ref('<on model>', '<column>') }}`; a referenced member's stay bare. Either kind's
`columns_and_descriptions` mapping becomes a `columns:` list of `column:` entries. `body_file:`,
read from the project root, becomes `body: "{{ file('<path>') }}"` relative to the tool file.
An `imports:` entry on a stage the project declares opens with that stage's `tool()` call. The
settings `warehouse`, `target_lag`, `embedding_model` and `execute_as` move under `config:`, and
`owner` under `meta:`.
"""

from __future__ import annotations

import posixpath
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.migrate.calls import bare, call, quoted_call, relative_to
from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import Edit, double_quoted, entry_span, shift
from snowflake_semantic_tools.domain.migrate.outline import Entry, Node
from snowflake_semantic_tools.domain.migrate.refs import Rewrite
from snowflake_semantic_tools.domain.migrate.settings import Move, move_settings
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls

TOOL_MOVES = tuple(Move((key,), key) for key in ("warehouse", "target_lag", "embedding_model", "execute_as"))
_OWNER = (Move(("owner",), "owner"),)
_COLUMN_KEYS = ("search_column", "id_column", "title_column", "relative_path_column")


@dataclass(frozen=True, slots=True)
class ToolInfo:
    """What the migrations need to know of one tool member, wherever it is declared.

    Attributes:
        on_model: The model its `on:` names with `ref()`; None when it names none.
        relations: Its relation per target, as written; empty for a defined member.
    """

    name: str
    type: str
    defined: bool
    on_model: str | None = None
    relations: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ToolSite:
    """One member as a tool file writes it: its mapping, and whether SST defines it."""

    node: Node
    defined: bool


def tool_sites(root: Node) -> Iterator[ToolSite]:
    """Yield each member a tool file declares, in a group or not, in file order."""
    single = root.get("tool")
    if single is not None and single.is_map:
        yield ToolSite(single, single.get("relations") is None)
    listed = root.get("tools")
    for entry in listed.maps() if listed is not None and listed.is_seq else ():
        if entry.get("group") is None:
            yield ToolSite(entry, entry.get("relations") is None)
            continue
        for ownership, defined in (("define", True), ("reference", False)):
            members = entry.get(ownership)
            for member in members.maps() if members is not None and members.is_seq else ():
                yield ToolSite(member, defined)


def on_model(member: Node) -> str | None:
    """Return the model a member's `on:` names, as a `ref()` string or its `table:`; else None."""
    on = member.get("on")
    target = on.get("table") if on is not None and on.is_map else on
    text = target.text if target is not None else None
    try:
        calls = scan_template_calls(text) if text else ()
    except TemplateSyntaxError:
        return None
    return calls[0].args[0] if len(calls) == 1 and calls[0].function == "ref" and len(calls[0].args) == 1 else None


def tool_index(roots: Mapping[str, Node]) -> dict[str, ToolInfo]:
    """Index every member of every tool file by its name, casefolded; a repeated name keeps the first."""
    index: dict[str, ToolInfo] = {}
    for _, root in sorted(roots.items()):
        for site in tool_sites(root):
            name, kind = site.node.get("name"), site.node.get("type")
            relations = site.node.get("relations")
            if name is None or not name.text:
                continue
            written = tuple(entry.value.text or "" for entry in relations.entries) if relations is not None else ()
            info = ToolInfo(
                name.text,
                kind.text if kind is not None and kind.text else "",
                site.defined,
                on_model(site.node),
                written,
            )
            index.setdefault(name.text.casefold(), info)
    return index


def stage_path(value: str | None, index: Mapping[str, ToolInfo]) -> str | None:
    """Return a literal `@<stage>/<path>` on a declared stage member as its `tool()` path; else None.

    A referenced stage is matched by any of its relations, a defined one by its name as the
    path's last part names the stage.
    """
    if value is None or "{{" in value or not value.startswith("@"):
        return None
    stage, slash, rest = value[1:].partition("/")
    for info in index.values():
        if info.type != "stage":
            continue
        names = {relation.casefold() for relation in info.relations}
        if (stage.casefold() in names) if not info.defined else stage.split(".")[-1].casefold() == info.name.casefold():
            return call("tool", info.name) + slash + rest
    return None


def tool_references(lines: Sequence[str], root: Node, path: str, index: Mapping[str, ToolInfo]) -> Changes:
    """Record the reference pass's rewrites for one tool file at project-relative `path`."""
    changes = Changes()
    for site in tool_sites(root):
        member = site.node
        model = on_model(member) if site.defined else None
        if site.defined:
            _member_columns(member, model, changes)
        columns = member.entry("columns_and_descriptions")
        if columns is not None:
            columns_list(lines, columns, model if site.defined else None, changes, site.defined)
        _body_file(member.entry("body_file"), path, changes)
        imports = member.get("imports")
        for item in imports.items if imports is not None and imports.is_seq else ():
            converted = stage_path(item.text, index)
            if converted is not None:
                changes.replace(item, double_quoted(converted), "reference")
    return changes


def tool_structure(lines: Sequence[str], root: Node) -> Changes:
    """Record the structure pass's rewrites for one tool file: settings and owner."""
    changes = Changes()
    for site in tool_sites(root):
        move_settings(lines, site.node, TOOL_MOVES, changes, block="config")
        move_settings(lines, site.node, _OWNER, changes, block="meta")
    return changes


def _member_columns(member: Node, model: str | None, changes: Changes) -> None:
    """Write each column a defined member names as a ref of its `on:` model."""
    on = member.get("on")
    scalars = [member.get(key) for key in _COLUMN_KEYS]
    lists = [member.get("attribute_columns")]
    if on is not None and on.is_map:
        scalars.append(on.get("search_column"))
        lists.append(on.get("attributes"))
    nodes = [node for node in scalars if node is not None]
    nodes += [item for node in lists if node is not None and node.is_seq for item in node.items]
    for node in nodes:
        column = bare(node.text)
        if column is None:
            continue
        if model is None:
            changes.leave(node.line, node.col, column, "the member's on: names no model to ref the column of")
        else:
            changes.replace(node, quoted_call("ref", model, column), "tool_column")


def columns_list(lines: Sequence[str], entry: Entry, model: str | None, changes: Changes, as_ref: bool) -> None:
    """Rewrite a `columns_and_descriptions` mapping as a `columns:` list, each column a `column:`.

    Each column is a `ref()` of `model` when `as_ref`, else bare. A flow mapping, or a column whose
    entry is neither a mapping nor empty, is reported and left alone, as is every column when a
    ref is asked for and `model` is None.
    """
    value = entry.value
    if value.flow or not value.is_map or (as_ref and model is None):
        reason = "a flow mapping" if value.flow or not value.is_map else "the member's on: names no model"
        changes.leave(entry.line, entry.col, entry.key, f"{reason}: rewrite it as columns: by hand")
        return
    edits: list[Edit] = [Edit(entry.line, entry.col, entry.line, entry.col + len(entry.key), "columns")]
    for column in value.entries:
        written = quoted_call("ref", model, column.key) if as_ref and model is not None else column.key
        text = _column_entry(lines, column, written)
        if text is None:
            changes.leave(column.line, column.col, column.key, "the column's entry is not a mapping")
            return
        edits.append(Edit(column.line, 0, entry_span(lines, column)[1], 0, text))
    changes.edits.extend(edits)
    changes.rewrites.append(Rewrite(entry.line + 1, entry.col + 1, entry.key, "columns", "tool_column"))


def _column_entry(lines: Sequence[str], column: Entry, written: str) -> str | None:
    """Return one `columns_and_descriptions` entry's lines as a `- column:` list entry."""
    value = column.value
    lead = " " * column.col + "- column: " + written
    if value.null:
        return lead + "\n"
    if not value.is_map:
        return None
    if value.flow:
        line = lines[column.line]
        inner = line[value.col + 1 : value.end_col - 1].strip() if value.line == value.end_line else None
        if inner is None:
            return None
        return " " * column.col + "- {column: " + written + (", " + inner if inner else "") + "}\n"
    end = entry_span(lines, column)[1]
    rest = lines[column.line][column.col + len(column.key) + 1 :]
    body = shift("".join(lines[column.line + 1 : end]), column.col + 2 - value.entries[0].col)
    return lead + (rest if rest.strip() else "\n") + body


def _body_file(entry: Entry | None, path: str, changes: Changes) -> None:
    """Write `body_file: <project path>` as `body: "{{ file('<path from the tool file>') }}"`."""
    if entry is None or entry.value.text is None:
        return
    written = entry.value.text.strip()
    target = posixpath.normpath(written)
    if posixpath.isabs(target) or target == ".." or target.startswith("../") or "{{" in written:
        changes.leave(entry.line, entry.col, written, "body_file names a path outside the project")
        return
    after = "body: " + quoted_call("file", relative_to(target, posixpath.dirname(path)))
    edit = Edit(entry.line, entry.col, entry.value.end_line, entry.value.end_col, after)
    changes.add(edit, f"body_file: {written}", after, "file")
