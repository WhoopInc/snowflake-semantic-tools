"""A YAML document located in its own text, for the migrations that edit a file in place.

The YAML adapter builds it; nothing here reads YAML. Lines and columns are 0-based and count
characters. A block collection ends where the token after it starts, as YAML's marks say, so
the last line it occupies is worked out from the text (`domain.migrate.edits.value_end`).
"""

from __future__ import annotations

from dataclasses import dataclass

MAP = "map"
SEQ = "seq"
SCALAR = "scalar"


@dataclass(frozen=True, slots=True)
class Entry:
    """One key of a mapping, where the key is written, and its value."""

    key: str
    line: int
    col: int
    value: Node


@dataclass(frozen=True, slots=True)
class Node:
    """One mapping, sequence or scalar, where it starts and ends in the text.

    Attributes:
        kind: `MAP`, `SEQ` or `SCALAR`.
        end_line, end_col: Where the node ends: just past a scalar or a flow collection, and at
            the next token for a block collection.
        value: A scalar's text, with every template call as written; None for a collection and
            for a null scalar (`x:`, `x: null`, `x: ~`).
        style: A scalar's quote or block indicator (`'`, `"`, `|`, `>`); "" when plain.
        flow: The collection is written in flow style, `[...]` or `{...}`.
    """

    kind: str
    line: int
    col: int
    end_line: int
    end_col: int
    value: str | None = None
    style: str = ""
    flow: bool = False
    entries: tuple[Entry, ...] = ()
    items: tuple[Node, ...] = ()

    @property
    def is_map(self) -> bool:
        """Whether the node is a mapping."""
        return self.kind == MAP

    @property
    def is_seq(self) -> bool:
        """Whether the node is a sequence."""
        return self.kind == SEQ

    @property
    def text(self) -> str | None:
        """Return a non-null scalar's text; None for anything else."""
        return self.value if self.kind == SCALAR else None

    @property
    def null(self) -> bool:
        """Whether the node is a null scalar."""
        return self.kind == SCALAR and self.value is None

    def entry(self, key: str) -> Entry | None:
        """Return the mapping's entry for `key`; None when absent or the node is no mapping."""
        return next((entry for entry in self.entries if entry.key == key), None)

    def get(self, key: str) -> Node | None:
        """Return the value at `key` of a mapping; None when absent."""
        found = self.entry(key)
        return found.value if found is not None else None

    def find(self, *path: str) -> Node | None:
        """Return the node at `path` through nested mappings; None when any step is missing."""
        node: Node | None = self
        for key in path:
            node = node.get(key) if node is not None else None
        return node

    def maps(self) -> tuple[Node, ...]:
        """Return a sequence's mapping items, in order; () for anything else."""
        return tuple(item for item in self.items if item.is_map)
