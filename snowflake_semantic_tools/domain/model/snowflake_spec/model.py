"""The parts of a Snowflake spec table: keys, the nodes holding them, their sources and evidence.

A `SpecKey` is one key of what SST writes to Snowflake. Its `sources` say what fills it: a key
an authored file writes, a configuration setting, a constant, or a value SST works out. Its
`evidence` says why SST writes it there: a documentation page, or a measurement recorded in
`docs/design/snowflake-behavior.md` for what Snowflake does not document.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from snowflake_semantic_tools.domain.model.authored_schema import ValueKind
from snowflake_semantic_tools.domain.model.snowflake_spec.docs import DocPage


@dataclass(frozen=True, slots=True)
class Authored:
    """A key an authored file writes: the file kind, and the key's dotted path, list entries left out.

    Example:
        `Authored("agent", "spec.tools.semantic_view")` is each `spec.tools:` entry's `semantic_view:`.
    """

    kind: str
    path: str


@dataclass(frozen=True, slots=True)
class Setting:
    """A configuration setting: the `sst_config.yml` block, and the key without its `+`."""

    block: str
    key: str


@dataclass(frozen=True, slots=True)
class Fixed:
    """A value SST always writes."""

    value: str


@dataclass(frozen=True, slots=True)
class Derived:
    """A value SST works out, with how."""

    note: str


Source = Authored | Setting | Fixed | Derived


@dataclass(frozen=True, slots=True)
class Documented:
    """Snowflake documents the key on `page`."""

    page: DocPage


@dataclass(frozen=True, slots=True)
class Measured:
    """An undocumented key SST measured; `section` is the `snowflake-behavior.md` heading that records it."""

    section: str


Evidence = Documented | Measured


@dataclass(frozen=True, slots=True)
class SpecKey:
    """One key of what SST writes to Snowflake.

    Attributes:
        name: The key, as Snowflake reads it.
        kind: The kind of value it holds.
        summary: What it does, in one line.
        sources: What fills it, in the order SST tries them; the first that is set wins.
        node: The keys nested in a mapping value.
        items: A list's entries: a kind, or a node of keys.
        evidence: Why SST writes it here; None takes its node's.
        named: A mapping whose keys are names, such as a tool's name, each holding `node`.
    """

    name: str
    kind: ValueKind
    summary: str
    sources: tuple[Source, ...] = ()
    node: SpecNode | None = None
    items: ValueKind | SpecNode | None = None
    evidence: Evidence | None = None
    named: bool = False

    @property
    def nested(self) -> SpecNode | None:
        """The node of keys below this one: its mapping's, or its list entries'."""
        return self.node if self.node is not None else self.items if isinstance(self.items, SpecNode) else None


@dataclass(frozen=True, slots=True)
class SpecNode:
    """The keys of one mapping SST writes, in the order it writes them.

    Attributes:
        keys: Its keys.
        evidence: Why SST writes them; a key's own evidence wins.
    """

    keys: tuple[SpecKey, ...]
    evidence: Evidence | None = None

    def key(self, name: str) -> SpecKey | None:
        """The key named `name`; None when the node has none."""
        return next((item for item in self.keys if item.name == name), None)

    @property
    def names(self) -> frozenset[str]:
        """The names of its keys."""
        return frozenset(item.name for item in self.keys)


SpecPath = tuple[str, ...]
# The segment a named mapping's or a list's entry takes in a `SpecPath`.
ANY = "*"


def walk(
    node: SpecNode, path: SpecPath = (), evidence: Evidence | None = None
) -> Iterator[tuple[SpecPath, SpecKey, Evidence | None]]:
    """Yield each key below `node`, depth-first, with its path and the evidence that covers it.

    A list's entries and a named mapping's values add `*` to the path: `tools.*.tool_spec.name`.
    """
    covering = node.evidence or evidence
    for item in node.keys:
        where = (*path, item.name)
        yield where, item, item.evidence or covering
        nested = item.nested
        if nested is not None:
            below = (*where, ANY) if item.named or isinstance(item.items, SpecNode) else where
            yield from walk(nested, below, item.evidence or covering)


def sources(node: SpecNode) -> Iterator[tuple[SpecPath, Source]]:
    """Yield every source of every key below `node`, with the key's path."""
    for path, item, _ in walk(node):
        for source in item.sources:
            yield path, source


def settings(node: SpecNode) -> tuple[tuple[str, SpecPath], ...]:
    """Each setting a key below `node` comes from, with the key's path, in the order SST writes them."""
    return tuple((source.key, path) for path, source in sources(node) if isinstance(source, Setting))


def merges_into(item: SpecKey) -> bool:
    """Whether a passthrough mapping under `item` merges key by key into what SST writes there.

    A mapping SST writes from its own keys does; a list, a named mapping and a plain value
    are replaced whole, so passthrough there would overwrite what SST writes.
    """
    return item.node is not None and not item.named


def passthrough_split(node: SpecNode, block: Mapping[Any, object]) -> tuple[list[str], int]:
    """Split a passthrough block over `node` into the dotted paths it overwrites and how many keys it adds.

    A key the node does not declare is added, unvalidated. A mapping under a key SST writes as
    a mapping merges into it, key by key; any other value at a key SST writes overwrites it.

    Example:
        Over an agent's specification, `{"models": {"fallback": "x"}, "tools": []}` overwrites
        `tools` and adds one key, `models.fallback`.
    """
    overwritten: list[str] = []
    added = 0
    for key, value in block.items():
        declared = node.key(str(key))
        if declared is None:
            added += 1
        elif merges_into(declared) and isinstance(value, Mapping) and declared.node is not None:
            inner, count = passthrough_split(declared.node, value)
            overwritten += [f"{key}.{path}" for path in inner]
            added += count
        else:
            overwritten.append(str(key))
    return overwritten, added


def unmodelled(node: SpecNode, value: Mapping[Any, object]) -> dict[str, object]:
    """Return what `value` holds that `node` does not declare, at any depth, as a passthrough block.

    The inverse of `passthrough_split`: copied over what SST writes from `node`, the result
    restores `value`'s undeclared keys and overwrites none SST writes.
    """
    left: dict[str, object] = {}
    for key, item in value.items():
        declared = node.key(str(key))
        if declared is None:
            left[str(key)] = item
        elif merges_into(declared) and isinstance(item, Mapping) and declared.node is not None:
            inner = unmodelled(declared.node, item)
            if inner:
                left[str(key)] = inner
    return left
