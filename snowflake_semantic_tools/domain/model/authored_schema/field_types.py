"""The types of the authored-file key tables: a file kind, the nodes it nests, and each field.

The tables themselves are data, one module per family of kinds, built out of these. The
loaders and validators read the name sets derived here, so the keys they accept are the
table's; `config:` blocks are not described twice: a field naming its block points at the
`sst_config.yml` settings `config_schema` already types and describes.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import TypeAlias

from snowflake_semantic_tools.domain.model.authored import NodePath

# A node's place in its file, as the agent loaders match it: keys, with `*` for every list index.
NodePattern: TypeAlias = NodePath


class ValueKind(Enum):
    """The kind of value a field takes, as the loader reads it.

    `TEMPLATE` is text carrying template calls, such as `{{ ref('orders') }}`; which calls a
    field takes is `domain.resolve.ref_fields.REFERENCE_FIELDS`'s, by the field's path. `ENUM`
    is text from the field's `choices`; `MAPPING` is a mapping whose keys the field's `node`
    declares, or free-form when it has none; `ANY` is any value.
    """

    STRING = "string"
    TEMPLATE = "template"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    ENUM = "enum"
    LIST = "list"
    MAPPING = "mapping"
    ANY = "any"


class FieldStatus(Enum):
    """Whether SST reads a key, and what it reports when one is written.

    `DEPRECATED`, `INERT` and `REFUSED` keys are read, each to be reported: a 0.3 spelling
    honored under its 1.0 name, a key that renders nothing, and a key a file may not set.
    `RENAMED` and `REMOVED` keys are not read: each is an error naming what replaced it.
    """

    CURRENT = "current"
    DEPRECATED = "deprecated"
    INERT = "inert"
    REFUSED = "refused"
    RENAMED = "renamed"
    REMOVED = "removed"


# The statuses whose keys a loader reads; any other key of a node is reported as unread.
READ_STATUSES = frozenset((FieldStatus.CURRENT, FieldStatus.DEPRECATED, FieldStatus.INERT, FieldStatus.REFUSED))


@dataclass(frozen=True, slots=True)
class Field:
    """One key of a node.

    Attributes:
        name: The key as it is written.
        kind: The kind of value it takes; `also` names any other kind SST accepts for it.
        summary: What the key does, as the reference page and the JSON Schema describe it;
            for a renamed or removed key, what it was.
        required: Whether a node without the key is reported.
        choices: An `ENUM` key's values, taken from the constant its check uses.
        open_choices: The project may extend `choices`, so a value outside them is not an
            error the table can decide.
        items: A `LIST` key's item: a value kind, or the node each item is.
        node: A `MAPPING` key's declared keys; None for a free-form mapping.
        status: Whether the key is read, as `FieldStatus` says.
        replacement: The key a deprecated or renamed key is read as or became; why a removed
            or refused key does nothing, as its diagnostic says.
        code: The code a removed or refused key reports instead of the walker's own.
        config_block: For a `config:` key, the `sst_config.yml` block whose settings it takes.
        loose: SST reads the value without checking its kind, as text, a flag, or unset when it is
            of another kind, so a value of another kind is a warning (SST-PRS131), not an error.
    """

    name: str
    kind: ValueKind
    summary: str
    required: bool = False
    also: tuple[ValueKind, ...] = ()
    choices: tuple[str, ...] = ()
    open_choices: bool = False
    items: ValueKind | Node | None = None
    node: Node | None = None
    status: FieldStatus = FieldStatus.CURRENT
    replacement: str | None = None
    code: str | None = None
    config_block: str | None = None
    loose: bool = False

    @property
    def read(self) -> bool:
        """Return whether the loader reads the key, even if only to report it."""
        return self.status in READ_STATUSES

    @property
    def nested(self) -> Node | None:
        """Return the node the key's value is, or each item of it is; None for a scalar or open value."""
        return self.items if isinstance(self.items, Node) else self.node


@dataclass(frozen=True, slots=True)
class Node:
    """A mapping's declared keys.

    Attributes:
        fields: The keys, in the order a reference lists them and nested checks run.
        open: Keys beyond `fields` are accepted unchecked, as SST does not key-check the node.
        checked_elsewhere: The node's own keys are a `config:` block's, which its block's
            settings check; only the nodes nested in it are checked with the table.
        shorthand: A value of this kind may stand for the whole node, as a bare reference
            stands for `{ref: ...}` in a window's `order_by`.
        discriminator: The key whose value picks one of `variants`.
        variants: The keys each value of `discriminator` adds to `fields`.
    """

    fields: tuple[Field, ...]
    open: bool = False
    checked_elsewhere: bool = False
    shorthand: ValueKind | None = None
    discriminator: str | None = None
    variants: Mapping[str, tuple[Field, ...]] = field(default_factory=lambda: MappingProxyType({}))

    def all_fields(self) -> tuple[Field, ...]:
        """Return the node's own fields, then each variant's not already listed, in order."""
        seen = {item.name for item in self.fields}
        found = list(self.fields)
        for fields in self.variants.values():
            for item in fields:
                if item.name not in seen:
                    seen.add(item.name)
                    found.append(item)
        return tuple(found)

    def read_keys(self) -> frozenset[str]:
        """Return every key the loader reads in the node, across its variants."""
        return frozenset(item.name for item in self.all_fields() if item.read)

    def variant_keys(self) -> Mapping[str, frozenset[str]]:
        """Return the keys each discriminator value takes: the node's own, then its variant's."""
        common = frozenset(item.name for item in self.fields if item.read)
        return MappingProxyType(
            {value: common | {item.name for item in fields if item.read} for value, fields in self.variants.items()}
        )


@dataclass(frozen=True, slots=True)
class FileKind:
    """One kind of authored file, or of authored block inside another tool's file.

    Attributes:
        name: The kind's name, as `sst schema --kind` and the reference page name it.
        summary: What the file declares.
        location: Where such a file lives, as the reference page says it.
        node: The node one declaration is.
        roots: The top-level keys a file holds declarations under: the key one declaration is
            written under, then the key a list of them is; empty when the file is the node.
    """

    name: str
    summary: str
    location: str
    node: Node
    roots: tuple[str, ...] = ()


def walk(node: Node, pattern: NodePattern = ()) -> Iterator[tuple[NodePattern, Node, Field]]:
    """Yield each read field below `node` with the node holding it and that node's pattern, depth-first.

    A node's fields come in order, each followed by the fields nested in it; a list item's
    pattern ends in `*`.
    """
    for item in node.all_fields():
        if not item.read:
            continue
        yield pattern, node, item
        nested = item.nested
        if nested is not None:
            below = (*pattern, item.name, "*") if isinstance(item.items, Node) else (*pattern, item.name)
            yield from walk(nested, below)


def node_patterns(node: Node) -> Mapping[NodePattern, frozenset[str]]:
    """Return the keys each checked node declares, by its pattern, root first, then depth-first.

    An open node is not listed, nor is a checked-elsewhere node itself, though the nodes
    nested in it are.
    """
    found: dict[NodePattern, frozenset[str]] = {}

    def visit(current: Node, pattern: NodePattern) -> None:
        if not current.open and not current.checked_elsewhere:
            found[pattern] = current.read_keys()
        for item in current.all_fields():
            nested = item.nested if item.read else None
            if nested is not None:
                visit(nested, (*pattern, item.name, "*") if isinstance(item.items, Node) else (*pattern, item.name))

    visit(node, ())
    return MappingProxyType(found)


def unread_fields(node: Node, status: FieldStatus) -> Iterator[tuple[NodePattern, Field]]:
    """Yield each field with `status` at or below `node`, with the pattern of the node that holds it."""
    for item in node.all_fields():
        if item.status is status:
            yield (), item
    for pattern, _, item in walk(node):
        nested = item.nested
        if nested is None:
            continue
        below = (*pattern, item.name, "*") if isinstance(item.items, Node) else (*pattern, item.name)
        for field_item in nested.all_fields():
            if field_item.status is status:
                yield below, field_item
