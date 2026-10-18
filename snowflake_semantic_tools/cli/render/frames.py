"""The one source line a finding points at, laid out for a terminal with a caret under its column.

A finding knows its file, line and column, not its extent, so the caret covers the token that
starts at the column: a `{{ ... }}` call, a quoted string, or a run of characters up to the
next separator. Tabs are expanded, control characters are replaced so a project file cannot
drive the terminal, and a line wider than the terminal is cut to a window around the column.
"""

from __future__ import annotations

import dataclasses
import unicodedata
from collections.abc import Sequence
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin

TAB_WIDTH = 4
# A file larger than this is not read for an excerpt; the finding still prints its location.
_MAX_FILE_BYTES = 4 * 1024 * 1024
# Characters that end a bare token, so the caret covers one YAML scalar or one name.
_TOKEN_STOPS = frozenset(" \t:,[]{}()#\"'")
_ELLIPSIS = "..."
_CACHE: dict[tuple[str, int, int], tuple[str, ...]] = {}
# The template values that name the text a finding is about, in the order they are searched for.
HINT_KEYS = ("column", "name", "field", "value", "found", "key", "identifier", "var", "selector")
# A block longer than this is not searched for a hint; the origin stands as it is.
_BLOCK_LINES = 400


@dataclasses.dataclass(frozen=True)
class Excerpt:
    """One source line as displayed, and the span the caret marks on it.

    Attributes:
        line: The line number, 1-based, as the finding gives it.
        text: The line, tabs expanded, control characters replaced, cut to fit when wide.
        start: The display column, 0-based, the caret starts at.
        length: How many display columns the caret covers, at least 1.
    """

    line: int
    text: str
    start: int
    length: int


def excerpt(project_dir: Path, origin: Origin, width: int) -> Excerpt | None:
    """Return the excerpt for `origin`, fitted into `width` columns; None when there is none.

    There is none when the origin names no line, or its file cannot be read, is too large, or has
    fewer lines. Nothing raises: a finding without an excerpt still prints.

    Example:
        An origin at line 3, column 3 of `  expr: {{ ref('orders') }}` gives the excerpt
        `  expr: {{ ref('orders') }}`, start 2, length 4 (the token `expr`).
    """
    if origin.line is None or origin.line < 1:
        return None
    lines = _lines(project_dir / origin.file)
    if lines is None or origin.line > len(lines):
        return None
    raw = lines[origin.line - 1].rstrip("\r\n")
    column = max((origin.col or 1) - 1, 0)
    if column > len(raw):
        column = len(raw)
    token = _token(raw, column) if origin.col is not None else raw.lstrip()
    if origin.col is None:
        column = len(raw) - len(raw.lstrip())
    text = _printable(raw)
    start = display_width(_printable(raw[:column]))
    length = max(display_width(_printable(token)), 1)
    return _fitted(Excerpt(origin.line, text.rstrip(), start, length), width)


def pinpoint(project_dir: Path, origin: Origin, hints: Sequence[str]) -> Origin:
    """Return `origin` moved onto the text a finding names, when its YAML block holds it.

    Many findings point at the line that opens their member (`name: sales_partner`) while the
    problem is a value further down. The first hint found -- on the origin's line, then on the
    lines indented under it -- moves the origin there; a dotted hint (`orders.tax_payed`) is also
    looked for by its last part. The origin stands when no hint is found, and the JSON envelope
    always reports the origin unmoved.

    Example:
        An origin at `  name: customer_count` (line 3) with the hint `customer_uuid` moves to
        line 5, `  expr: "COUNT(DISTINCT {{ ref('customers', 'customer_uuid') }})"`, column 46.
    """
    if origin.line is None or not hints:
        return origin
    lines = _lines(project_dir / origin.file)
    if lines is None or origin.line > len(lines):
        return origin
    block = _block(lines, origin.line)
    # Hints go first, then lines: the most specific value wins wherever in the block it stands.
    for word in (word for hint in hints for word in _forms(hint)):
        for number in block:
            start = (origin.col or 1) - 1 if number == origin.line else 0
            found = _word_at(lines[number - 1], word, start)
            if found >= 0:
                return dataclasses.replace(origin, line=number, col=found + 1)
    return origin


def display_width(text: str) -> int:
    """Return how many terminal columns `text` takes: two for a wide character, one otherwise."""
    return sum(2 if unicodedata.east_asian_width(char) in ("W", "F") else 1 for char in text)


def _lines(path: Path) -> tuple[str, ...] | None:
    """Return the file's lines, read once per version of it; None when it cannot be read."""
    try:
        stat = path.stat()
        if not path.is_file() or stat.st_size > _MAX_FILE_BYTES:
            return None
        key = (str(path), stat.st_mtime_ns, stat.st_size)
        if key not in _CACHE:
            _CACHE[key] = tuple(path.read_text(encoding="utf-8", errors="replace").splitlines())
        return _CACHE[key]
    except OSError:
        return None


def _forms(hint: str) -> list[str]:
    """Return the spellings to look for: the hint, then a dotted hint's last part."""
    hint = hint.strip()
    if len(hint) < 2:
        return []
    tail = hint.rsplit(".", 1)[-1]
    return [hint, tail] if tail != hint and len(tail) >= 2 else [hint]


def _block(lines: tuple[str, ...], first: int) -> range:
    """Return the line numbers of the origin's line and the lines indented under it."""
    indent = _indent(lines[first - 1])
    last = first
    for number in range(first + 1, min(len(lines), first + _BLOCK_LINES) + 1):
        text = lines[number - 1]
        if text.strip() and not text.lstrip().startswith("#") and _indent(text) < indent:
            break
        last = number
    return range(first, last + 1)


def _indent(text: str) -> int:
    return len(text) - len(text.lstrip(" \t-"))


def _word_at(text: str, word: str, start: int) -> int:
    """Return where `word` stands alone in `text` from `start`, not inside a longer name; else -1."""
    position = text.find(word, start)
    while position >= 0:
        before = text[position - 1] if position else " "
        after = text[position + len(word)] if position + len(word) < len(text) else " "
        if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
            return position
        position = text.find(word, position + 1)
    return -1


def _token(line: str, column: int) -> str:
    """Return the token that starts at `column`: a template call, a quoted string, or a bare run."""
    rest = line[column:]
    if rest.startswith("{{"):
        end = rest.find("}}")
        return rest[: end + 2] if end >= 0 else rest
    if rest[:1] in ("'", '"'):
        end = rest.find(rest[0], 1)
        return rest[: end + 1] if end > 0 else rest
    for index, char in enumerate(rest):
        if char in _TOKEN_STOPS:
            return rest[:index]
    return rest


def _printable(text: str) -> str:
    """Expand tabs, and replace every other control character, so the line cannot drive a terminal."""
    expanded = text.replace("\t", " " * TAB_WIDTH)
    return "".join("?" if unicodedata.category(char) in ("Cc", "Cf") else char for char in expanded)


def _fitted(found: Excerpt, width: int) -> Excerpt:
    """Cut a line wider than `width` to a window around the caret, marking each cut with `...`."""
    if display_width(found.text) <= width:
        return found
    before = max(found.start - width // 3, 0)
    cut = found.text[before:]
    lead = _ELLIPSIS if before else ""
    room = width - len(lead) - len(_ELLIPSIS)
    kept = ""
    for char in cut:
        if display_width(kept + char) > room:
            break
        kept += char
    tail = _ELLIPSIS if len(kept) < len(cut) else ""
    # The window opens at or before the caret, so the caret stays inside it.
    start = len(lead) + display_width(found.text[before : found.start])
    length = max(min(found.length, display_width(kept) - (start - len(lead))), 1)
    return Excerpt(found.line, lead + kept + tail, start, length)
