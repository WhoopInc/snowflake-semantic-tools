"""Warn where a field SST reads without checking its kind holds a value of another kind.

Such a field (a description, a search column's flags, an agent's display block) is read as
text, as a flag, or as unset whatever it holds, so a mapping written there is published as its
Python form, or read as true, or dropped without a word. The authored-file tables mark each
one `loose`; this walks a declaration with its kind's table and names every such value.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.authored import NodePath
from snowflake_semantic_tools.domain.model.authored_schema import Field, Node, ValueKind

# How a value of each kind is named, written or found.
_EXPECTED: Mapping[ValueKind, str] = {
    ValueKind.STRING: "text",
    ValueKind.TEMPLATE: "text",
    ValueKind.ENUM: "text",
    ValueKind.BOOLEAN: "true or false",
    ValueKind.INTEGER: "an integer",
    ValueKind.NUMBER: "a number",
    ValueKind.LIST: "a list",
    ValueKind.MAPPING: "a mapping",
}


def _found(value: object) -> str:
    if isinstance(value, bool):
        return "true or false"
    if isinstance(value, int):
        return "an integer"
    if isinstance(value, float):
        return "a number"
    if isinstance(value, str):
        return "text"
    return "a list" if isinstance(value, list) else "a mapping" if isinstance(value, Mapping) else type(value).__name__


def _is(kind: ValueKind, value: object) -> bool:
    if kind in (ValueKind.STRING, ValueKind.TEMPLATE, ValueKind.ENUM):
        return isinstance(value, str)
    if kind is ValueKind.BOOLEAN:
        return isinstance(value, bool)
    if kind is ValueKind.INTEGER:
        return isinstance(value, int) and not isinstance(value, bool)
    if kind is ValueKind.NUMBER:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind is ValueKind.LIST:
        return isinstance(value, list)
    return kind is ValueKind.ANY or isinstance(value, Mapping)


def _matches(field: Field, value: object) -> bool:
    return any(_is(kind, value) for kind in (field.kind, *field.also))


def _label(path: NodePath) -> str:
    """Write a path inside a declaration as a diagnostic names a field: `columns[0].type`."""
    text = ""
    for part in path:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else part)
    return text


def loose_value_diagnostics(
    node: Node,
    value: object,
    path: NodePath,
    *,
    artifact: str,
    subject: str,
    origin_of: Callable[[NodePath], Origin],
    start: int | None = None,
) -> list[Diagnostic]:
    """Report each loose field below `value`, a declaration of `node` at `path`, that holds another kind.

    A field is labeled from `start` on, the declaration's own path by default; an absent or
    null value is unset, not of another kind. Fields are checked in table order, each then
    the nodes nested in it, list items in order.

    Diagnostics:
        SST-PRS131: a loose field's value is of a kind SST does not read it as.
    """
    begin = len(path) if start is None else start
    found: list[Diagnostic] = []
    if not isinstance(value, Mapping):
        return found
    fields = list(node.fields)
    if node.discriminator is not None:
        fields += node.variants.get(str(value.get(node.discriminator)), ())
    for item in fields:
        held = value.get(item.name)
        if not item.read or held is None:
            continue
        where = (*path, item.name)
        if item.loose and not _matches(item, held):
            found.append(
                D(
                    "SST-PRS131",
                    artifact=artifact,
                    field=_label(where[begin:]),
                    found=_found(held),
                    expected=_EXPECTED[item.kind],
                    subject=subject,
                    origin=origin_of(where),
                )
            )
            continue
        nested = item.nested
        if nested is None:
            continue
        entries = enumerate(held) if isinstance(item.items, Node) and isinstance(held, list) else ()
        for index, entry in entries:
            found.extend(
                loose_value_diagnostics(
                    nested, entry, (*where, index), artifact=artifact, subject=subject, origin_of=origin_of, start=begin
                )
            )
        if item.node is not None:
            found.extend(
                loose_value_diagnostics(
                    nested, held, where, artifact=artifact, subject=subject, origin_of=origin_of, start=begin
                )
            )
    return found
