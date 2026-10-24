"""Text edits for the migrations: replace spans, move whole lines, and find where a block ends.

Every edit addresses the original text by 0-based line and column, so a migration computes all
its edits against one reading of a file and applies them together; `apply_edits` refuses two
that overlap rather than guess which wins. Line helpers work on `text.splitlines(keepends=True)`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.migrate.outline import SCALAR, Entry, Node


@dataclass(frozen=True, slots=True)
class Edit:
    """Replace the text from `(line, col)` up to, not including, `(end_line, end_col)`.

    Attributes:
        text: The replacement; "" deletes the span. An insertion is a span with no width.
    """

    line: int
    col: int
    end_line: int
    end_col: int
    text: str


def lines_edit(start: int, end: int, text: str) -> Edit:
    """Replace whole lines `start` up to, not including, `end` with `text`."""
    return Edit(start, 0, end, 0, text)


def node_edit(node: Node, text: str) -> Edit:
    """Replace a scalar or a flow collection, quotes and brackets included, with `text`."""
    return Edit(node.line, node.col, node.end_line, node.end_col, text)


def apply_edits(text: str, edits: Iterable[Edit]) -> str:
    """Apply every edit to the text it was computed against and return the result.

    A position past the last line is the end of the text. Edits that only touch are applied in
    position order: an insertion where a replacement starts lands before it.

    Raises:
        ValueError: Two edits overlap.
    """
    starts = [0]
    for index, character in enumerate(text):
        if character == "\n":
            starts.append(index + 1)

    def offset(line: int, col: int) -> int:
        return len(text) if line >= len(starts) else min(starts[line] + col, len(text))

    spans = sorted(((offset(e.line, e.col), offset(e.end_line, e.end_col), e.text) for e in edits), reverse=True)
    out = text
    limit = len(text)
    for start, end, replacement in spans:
        if end > limit:
            raise ValueError(f"overlapping edits at offset {start}")
        out = out[:start] + replacement + out[end:]
        limit = start
    return out


def indent(line: str) -> int:
    """Return how many spaces open the line."""
    return len(line) - len(line.lstrip(" "))


def is_blank(line: str) -> bool:
    """Whether the line holds only whitespace."""
    return not line.strip()


def is_comment(line: str) -> bool:
    """Whether the line holds only a comment."""
    return line.lstrip().startswith("#")


def value_end(lines: Sequence[str], node: Node, floor: int) -> int:
    """Return the line after the last one a value occupies, trailing blank lines left out.

    A block collection ends where the next token starts, so its content stops on the line
    before -- unless the text ends without a newline, when it ends just past the last line's
    text, on that line; a scalar or a flow collection ends just past its text. A trailing
    comment indented no deeper than `floor`, the column of the key or dash that holds the
    value, belongs to what follows, and is left out too.
    """
    block = node.kind != SCALAR and not node.flow
    at_eof = bool(lines) and node.end_line == len(lines) - 1 and node.end_col >= len(lines[-1].rstrip("\r\n"))
    last = node.end_line - 1 if (block and not at_eof) or node.end_col == 0 else node.end_line
    last = min(last, len(lines) - 1)
    while last > node.line and (is_blank(lines[last]) or (is_comment(lines[last]) and indent(lines[last]) <= floor)):
        last -= 1
    return max(last, node.line) + 1


def entry_span(lines: Sequence[str], entry: Entry) -> tuple[int, int]:
    """Return the lines an entry occupies, its key's line first, as `(start, end)`."""
    return entry.line, max(entry.line + 1, value_end(lines, entry.value, entry.col))


def comments_above(lines: Sequence[str], line: int, col: int) -> int:
    """Return the first of the comment lines directly above `line` indented exactly `col`."""
    first = line
    while first > 0 and is_comment(lines[first - 1]) and indent(lines[first - 1]) == col:
        first -= 1
    return first


def dash_line(lines: Sequence[str], item: Node, dash_col: int) -> int:
    """Return the line holding the `-` that opens a block sequence item."""
    line = item.line
    while line > 0 and lines[line][dash_col : dash_col + 1] != "-":
        line -= 1
    return line


def shift(text: str, delta: int) -> str:
    """Indent every non-blank line of `text` by `delta` spaces; a negative delta removes them.

    A line holding fewer spaces than a negative delta asks loses only the spaces it has.
    """
    out: list[str] = []
    for line in text.splitlines(keepends=True):
        if is_blank(line):
            out.append(line)
        elif delta >= 0:
            out.append(" " * delta + line)
        else:
            out.append(line[min(-delta, indent(line)) :])
    return "".join(out)


def double_quoted(value: str) -> str:
    """Write `value` as a YAML double-quoted scalar, escaping only a backslash and a quote."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
