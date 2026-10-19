"""Draw a tree with connectors: each row under its parent, the last of its rows marked `└── `.

Every other row is marked `├── `. `lineage` draws its traces this way and `test` its eval
results; a terminal without unicode gets `|-- ` and `` `-- `` instead. A row's text may run
onto more lines, which keep the connectors of the rows around it, so a long answer stays
inside its branch.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Connectors:
    """The four pieces a tree is drawn with: a branch, the last branch, a line past a row, and none."""

    branch: str
    last: str
    through: str
    blank: str


UNICODE_TREE = Connectors("├── ", "└── ", "│   ", "    ")
ASCII_TREE = Connectors("|-- ", "`-- ", "|   ", "    ")


def connectors(unicode: bool) -> Connectors:
    """Return the connectors a terminal can show."""
    return UNICODE_TREE if unicode else ASCII_TREE


@dataclass(frozen=True, slots=True)
class TreeNode:
    """One row of a tree, the lines its text runs onto, and the rows under it."""

    text: str
    more: tuple[str, ...] = ()
    children: tuple[TreeNode, ...] = ()


def tree_prefixes(rows: Sequence[tuple[int, bool]], drawn: Connectors, base: str = "") -> list[str]:
    """Return each row's connectors, the rows given depth first as `(level, last)`.

    Args:
        rows: Each row's level, 1 under the root, and whether it is its parent's last child.
        base: What every prefix starts with, such as the root's own continuation.
    """
    prefixes = []
    open_levels: list[bool] = []
    for level, last in rows:
        del open_levels[level - 1 :]
        prefixes.append(base + "".join(drawn.through if more else drawn.blank for more in open_levels))
        prefixes[-1] += drawn.last if last else drawn.branch
        open_levels.append(not last)
    return prefixes


def tree_lines(nodes: Sequence[TreeNode], drawn: Connectors, base: str = "") -> list[tuple[str, str]]:
    """Lay out `nodes` and everything under them as `(connectors, text)` lines, depth first.

    A node's further lines continue the lines of the rows still open around it, so their text
    starts where the node's does; a node with children also runs its own line down to them,
    over the first column of that text, which should be blank.
    """
    lines: list[tuple[str, str]] = []
    for position, node in enumerate(nodes):
        last = position == len(nodes) - 1
        lines.append((base + (drawn.last if last else drawn.branch), node.text))
        inner = base + (drawn.blank if last else drawn.through)
        down = drawn.through.rstrip() if node.children else ""
        lines.extend((inner + down, text[len(down) :]) for text in node.more)
        lines.extend(tree_lines(node.children, drawn, inner))
    return lines
