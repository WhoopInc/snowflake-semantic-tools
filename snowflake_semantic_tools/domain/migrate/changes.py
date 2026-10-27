"""What one migration pass changes in a file: its edits, and what it reports.

A pass reads one outline of a file, records every edit against that text, and `merged` applies
them all at once, so no edit sees another's result. Rewrites from earlier passes keep the lines
of the text those passes read.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.migrate.edits import Edit, apply_edits, node_edit
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.refs import MigrationResult, Rewrite, Untouched


@dataclass(slots=True)
class Changes:
    """The edits a pass makes to one text, each rewrite it reports, and what it left alone."""

    edits: list[Edit] = field(default_factory=list)
    rewrites: list[Rewrite] = field(default_factory=list)
    untouched: list[Untouched] = field(default_factory=list)

    def add(self, edit: Edit, before: str, after: str, kind: str) -> None:
        """Record an edit and the rewrite it reports, located where the edit starts."""
        self.edits.append(edit)
        self.rewrites.append(Rewrite(edit.line + 1, edit.col + 1, before, after, kind))

    def replace(self, node: Node, after: str, kind: str) -> None:
        """Replace a scalar, quotes included, and report it."""
        self.add(node_edit(node, after), node.value or "", after, kind)

    def leave(self, line: int, col: int, text: str, reason: str) -> None:
        """Report something the pass will not rewrite, at a 0-based position."""
        self.untouched.append(Untouched(line + 1, col + 1, text, reason))


def merged(previous: MigrationResult, changes: Changes) -> MigrationResult:
    """Apply a pass's edits to the text `previous` holds, adding what the pass reported.

    Raises:
        ValueError: Two of the pass's edits overlap, which is a fault in the pass.
    """
    if not changes.edits and not changes.untouched:
        return previous
    return MigrationResult(
        apply_edits(previous.text, changes.edits),
        tuple(sorted((*previous.rewrites, *changes.rewrites), key=lambda item: (item.line, item.col))),
        (*previous.untouched, *changes.untouched),
    )
