"""Render the authored-file reference page, `docs/reference/files.md`, from the authored-file tables.

One section per file kind: where its files live, then a row per key, nested keys as dotted
paths with `[]` for a list's entries. A key's row says its type, whether it is required, and
what it does; a key SST reads without checking its kind, or one of an older dialect, says so.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain.model.authored_schema import (
    FILE_KINDS,
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.resolve.ref_fields import REFERENCE_FIELDS

# How the type column names each status other than current; each field's summary says why.
_STATUS: Mapping[FieldStatus, str] = {
    FieldStatus.DEPRECATED: "deprecated",
    FieldStatus.INERT: "ignored",
    FieldStatus.REFUSED: "refused",
    FieldStatus.RENAMED: "renamed",
    FieldStatus.REMOVED: "removed",
}


def render_files(kinds: Mapping[str, FileKind] = FILE_KINDS, *, generated: str) -> str:
    """Render the page: an introduction, a contents list, then each kind's section, in table order."""
    lines = [
        "# Authored files reference",
        "",
        generated,
        "",
        "Every key SST reads in each file it reads, generated from the tables it reads them by.",
        "`sst schema` writes the same tables as JSON Schemas, for an editor to check files as SST",
        "would; the Python API returns them from `schemas()`.",
        "",
        "- A key is required when its row says so; any other may be left out, or written as `null`.",
        "- A key SST does not read in a file is reported where it is written: as `SST-PRS004`",
        "  (`SST-PRS022` when it is a near miss of one it reads), or as `SST-PRS132` in a skill's",
        "  front matter, an eval dataset or an eval config.",
        "- A key marked *not kind-checked* is read without checking its kind; a value of another",
        "  kind there is a warning, `SST-PRS131`.",
        "- A key of an older dialect is listed with what replaced it, or why it is not read.",
        "",
        "## Contents",
        "",
    ]
    lines.extend(f"- [`{name}`](#{name}): {kind.summary}" for name, kind in kinds.items())
    for name, kind in kinds.items():
        lines.extend(_section(name, kind))
    return "\n".join(lines) + "\n"


def _section(name: str, kind: FileKind) -> list[str]:
    lines = ["", f"## {name}", "", kind.summary, "", f"Where: {kind.location}."]
    if kind.roots:
        one, many = kind.roots
        lines.extend(("", f"A file declares one under `{one}:`, or a list of them under `{many}:`."))
    if kind.node.open:
        lines.extend(("", "Keys the table does not list are kept, and sent on as written."))
    lines.extend(("", "| Key | Type | Required | Description |", "|---|---|---|---|"))
    for key, item, condition in _rows(kind.node, "", name):
        required = "yes" if item.required and item.read else ""
        lines.append(f"| `{key}` | {_type(item)} | {required} | {_cell(_description(item, condition, key, name))} |")
    return lines


def _rows(node: Node, prefix: str, kind: str) -> Iterator[tuple[str, Field, str]]:
    """Each key below `node`, with what makes it apply, its own fields and then each variant's."""
    entries: list[tuple[Field, str]] = [(item, "") for item in node.fields]
    taken: dict[int, list[str]] = {}
    variant_fields: dict[int, Field] = {}
    for value, fields in node.variants.items():
        for item in fields:
            taken.setdefault(id(item), []).append(value)
            variant_fields[id(item)] = item
    for identity, item in variant_fields.items():
        values = ", ".join(f"`{value}`" for value in taken[identity])
        entries.append((item, f"When `{node.discriminator}` is {values}."))
    for item, condition in entries:
        key = f"{prefix}{item.name}"
        yield key, item, condition
        nested = item.nested
        if nested is not None and item.config_block is None:
            below = f"{key}[]." if isinstance(item.items, Node) else f"{key}."
            yield from _rows(nested, below, kind)


def _type(item: Field) -> str:
    """The value's type; for a key of an older dialect, its status, with its type when SST still reads it."""
    if item.status is FieldStatus.CURRENT:
        return _value_type(item)
    status = f"*{_STATUS[item.status]}*"
    return f"{_value_type(item)}, {status}" if item.status is FieldStatus.DEPRECATED else status


def _value_type(item: Field) -> str:
    """The kind of value the field takes, its choices for an enum, and a shorthand's kind."""
    if item.config_block is not None:
        return "settings"
    if item.kind is ValueKind.ENUM:
        if item.open_choices or not item.choices:
            return "text"
        return "one of " + ", ".join(f"`{choice}`" for choice in item.choices)
    if item.kind is ValueKind.LIST:
        inner = item.items
        if isinstance(inner, Node):
            return "list of mappings"
        return f"list of {_KIND_NAMES[inner]}s" if inner is not None and inner in _KIND_NAMES else "list"
    named = [_KIND_NAMES[kind] for kind in (item.kind, *item.also) if kind in _KIND_NAMES]
    if item.node is not None and item.node.shorthand is not None:
        named.append(_KIND_NAMES[item.node.shorthand])
    return " or ".join(named) or "any"


_KIND_NAMES: Mapping[ValueKind, str] = {
    ValueKind.STRING: "text",
    ValueKind.TEMPLATE: "template",
    ValueKind.BOOLEAN: "boolean",
    ValueKind.INTEGER: "integer",
    ValueKind.NUMBER: "number",
    ValueKind.MAPPING: "mapping",
    ValueKind.LIST: "list",
}


def _description(item: Field, condition: str, key: str, kind: str) -> str:
    """The field's summary, with what applies it, what it takes, and how SST reads or reports it."""
    parts = [condition] if condition else []
    parts.append(item.summary)
    calls = REFERENCE_FIELDS.get(f"{kind}.{key.replace('[]', '')}")
    if calls:
        parts.append("Takes " + ", ".join(f"`{call}()`" for call in calls) + ".")
    if item.config_block is not None:
        parts.append(f"See [`{item.config_block}:`](config.md#{item.config_block}).")
    if item.loose:
        parts.append("*Not kind-checked.*")
    if item.status is not FieldStatus.CURRENT and item.code is not None:
        parts.append(f"`{item.code}`.")
    return " ".join(part.strip() for part in parts)


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")
