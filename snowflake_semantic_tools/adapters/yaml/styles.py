"""Choose a string's YAML style from what it holds: folded, literal, or as written.

A value with no line break that runs past the wrap width is written as a folded block, `>-`,
broken at single spaces. Loading folds each break back into that space, so the value -- and what
Snowflake or dbt `persist_docs` receives -- is the same one line. A value with a line break
keeps its literal block, `|-`, because the break is content.

With `unwrap`, a prose value (`PROSE_KEYS`: descriptions, comments, eval text) first has its soft
wraps joined: a line break inside a paragraph becomes a space, a run of blank lines between
paragraphs becomes one line break, and a break before a list item, heading, table row, code fence
or indented line is kept. A value left with only paragraph breaks is folded, each break written
as one blank line; one that keeps a hard break stays literal. That repair changes the value, so
the formatter compares against the joined text. A structural value (`STRUCTURAL_KEYS`: SQL,
expressions, instructions, prompts) is never joined or folded: its line breaks are code or markup.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ruamel.yaml.scalarstring import FoldedScalarString, LiteralScalarString

from snowflake_semantic_tools.domain.model.text_fields import PROSE_KEYS, STRUCTURAL_KEYS

__all__ = ("PROSE_KEYS", "STRUCTURAL_KEYS", "YamlStyle", "fold_positions", "foldable", "styled", "unwrap_prose")


@dataclass(frozen=True, slots=True)
class YamlStyle:
    """The `format:` settings every YAML writer applies: `sst format`, `sst enrich` and `sst import`.

    Attributes:
        spaced: `format.blank_line_between_items`.
        wrap_width: `format.wrap_width`; None folds nothing.
        unwrap_prose: `format.unwrap_prose`.
    """

    spaced: bool = False
    wrap_width: int | None = None
    unwrap_prose: bool = False


# A line that starts something a line break must stay in front of.
_HARD_START = re.compile(r"^(?:\s|[-*+]\s|\d+[.)]\s|#|\||```|~~~)")
_TEMPLATE = ("{{", "{%")
_CONTROL = re.compile("[\x00-\x08\x0b-\x1f\x7f]")


def foldable(text: str) -> bool:
    """Report whether `text` survives folding exactly: every break would load back as one space.

    It must have no leading or trailing whitespace, no tab, no run of two spaces, no control
    character, and no template, which project files keep double-quoted.
    """
    if not text or text != text.strip() or "\t" in text or "  " in text or _CONTROL.search(text):
        return False
    if any(marker in text for marker in _TEMPLATE):
        return False
    # A paragraph that starts or ends in a space would fold differently around its break.
    return all(line == line.strip() and line for line in text.split("\n"))


def fold_positions(text: str, width: int, indent: int) -> list[int]:
    """Return the offsets of the spaces to break `text` at, so no line passes `width` columns.

    Lines are filled greedily, counting `indent` columns before each; a word longer than a line
    is never split. Each paragraph (a line of `text`) is filled on its own.
    """
    room = max(width - indent, 1)
    positions: list[int] = []
    start = 0
    for paragraph in text.split("\n"):
        line_start = 0
        last_space = -1
        for index, char in enumerate(paragraph):
            if char == " ":
                if index - line_start > room and last_space > line_start:
                    positions.append(start + last_space)
                    line_start = last_space + 1
                last_space = index
        if len(paragraph) - line_start > room and last_space > line_start:
            positions.append(start + last_space)
        start += len(paragraph) + 1
    return positions


def unwrap_prose(text: str) -> tuple[list[str], bool]:
    """Join `text`'s soft wraps; return its paragraphs and whether a hard line break remains.

    Paragraphs are split at blank-line runs. A paragraph with a line `_HARD_START` matches after
    its first, or that starts indented, is hard and kept as written.
    """
    paragraphs: list[str] = []
    hard = False
    for block in re.split(r"\n[ \t]*\n+", text.strip("\n")):
        lines = block.split("\n")
        if any(_HARD_START.match(line) for line in lines[1:]) or lines[0][:1].isspace():
            hard = True
            paragraphs.append(block)
        else:
            paragraphs.append(" ".join(line.strip() for line in lines))
    return paragraphs, hard


def _quoted_length(text: str) -> int:
    """Return the most columns `text` takes on its key's line: quoted, with each quote escaped.

    The dumper may quote a plain value, so room for quotes is always kept; a second pass that
    then counted them would fold what the first left alone.
    """
    return len(text) + 2 + sum(text.count(char) for char in "'\"\\")


def _folded(text: str, width: int | None, indent: int) -> FoldedScalarString:
    folded = FoldedScalarString(text)
    folded.fold_pos = fold_positions(text, width, indent) if width is not None else []  # type: ignore[attr-defined]
    return folded


def styled(
    value: object, key: str | None, *, width: int | None, unwrap: bool, indent: int, inline: int | None = None
) -> object:
    """Return `value` in the style its text calls for, or as it is when no rule applies.

    Args:
        key: The mapping key the value is under (a list item's list key); None at the root.
        width: The wrap width; None leaves single-line values as written.
        unwrap: Join soft wraps in a prose value first (see the module docstring).
        indent: The columns before the value's text once written as a block.
        inline: The columns before the value when it stays on its key's line (the key, its colon
            and a space, or a list dash); `indent` when unknown.
    """
    if not isinstance(value, str) or key in STRUCTURAL_KEYS:
        return value
    text = str(value)
    if isinstance(value, FoldedScalarString) and "\n" in text and foldable(text):
        # Folding already joined its soft wraps: each line break left is a paragraph break.
        return _folded(text, width, indent)
    if "\n" in text.strip("\n"):
        if not (unwrap and key in PROSE_KEYS):
            return value
        paragraphs, hard = unwrap_prose(text)
        if hard:
            kept = "\n\n".join(paragraphs)
            return value if kept == text else LiteralScalarString(kept)
        joined = "\n".join(paragraphs)
        if "\n" in joined:
            if foldable(joined):
                return _folded(joined, width, indent)
            kept = "\n\n".join(paragraphs)
            return value if kept == text else LiteralScalarString(kept)
        text = joined
    if width is None:
        return value if text == str(value) else text
    if (indent if inline is None else inline) + _quoted_length(text) <= width or not foldable(text):
        # A fold SST wrote is undone once the value fits; any other style is the author's.
        return text if text != str(value) or isinstance(value, FoldedScalarString) else value
    return _folded(text, width, indent)
