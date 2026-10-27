"""Find the file paths a skill's Markdown names: inline links, reference definitions, and bare paths.

`scan_references` reports every candidate in source order, located by character offset,
line, and column, so a rewrite can splice a published name in place. Links and reference
definitions claim their spans first, and a bare path inside a claimed span is not reported
again. Deciding what a candidate refers to is the caller's job.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Marks a line whose paths are illustrations, so a missing one is not reported as SST-VAL808.
IGNORE_MARKER = "sst: ignore SST-VAL808"

_LINK = re.compile(r"!?\[[^\]\n]*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"\n]*\")?\s*\)")
_DEFINITION = re.compile(r"^[ ]{0,3}\[[^\]\n]+\]:[ \t]*<?([^\s>]+)>?", re.MULTILINE)
_SEGMENT = r"(?:\.{1,2}|\.?[A-Za-z0-9_][A-Za-z0-9_.-]*)"
# A path's last segment never ends in punctuation, so a sentence-final period or
# comma stays outside the token.
_LAST = r"[A-Za-z0-9_](?:[A-Za-z0-9_.-]*[A-Za-z0-9_-])?"
_BARE = re.compile(
    rf"(?<![\w./:@~$-])((?:{_SEGMENT}/)+{_LAST}|[A-Za-z0-9_][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9]{{0,5}})"
    # A token that runs into a glob or template marker is a pattern, not a path.
    r"(?![\w/*{<$-])"
)


@dataclass(frozen=True, slots=True)
class PathReference:
    """A path written in a Markdown or script file, located by character offset.

    Attributes:
        text: The path as written, without its `#anchor`; empty for a bare `#anchor` link.
        anchor: The `#anchor` the path carried, `#` included; empty when it carried none.
        line: 1-based line of the path's first character; `col` is 1-based within that line.
        start: Offset of the path's first character; `end` is one past its last, anchor excluded.
        form: `link`, `image`, `definition`, `fenced` (bare, inside a code fence), or `bare`.
        suppressed: Whether the path's line carries `IGNORE_MARKER`.
    """

    file: str
    text: str
    anchor: str
    line: int
    col: int
    start: int
    end: int
    form: str
    suppressed: bool = False


def scan_references(text: str, file: str) -> tuple[PathReference, ...]:
    """Find every candidate path in one file, in source order."""
    found: list[PathReference] = []
    claimed: list[tuple[int, int]] = []
    for pattern, form in ((_LINK, "link"), (_DEFINITION, "definition")):
        for match in pattern.finditer(text):
            start, end = match.span(1)
            form_name = "image" if form == "link" and match.group(0).startswith("!") else form
            claimed.append((start, end))
            found.append(_reference(text, file, match.group(1), start, form_name))
    fences = _fence_spans(text)
    for match in _BARE.finditer(text):
        start = match.start(1)
        if any(left <= start < right for left, right in claimed):
            continue
        form = "fenced" if any(left <= start < right for left, right in fences) else "bare"
        found.append(_reference(text, file, match.group(1), start, form))
    return tuple(sorted(found, key=lambda item: item.start))


def _reference(text: str, file: str, raw: str, start: int, form: str) -> PathReference:
    path, _, anchor = raw.partition("#")
    line_start = text.rfind("\n", 0, start) + 1
    line_end = text.find("\n", start)
    line_text = text[line_start : line_end if line_end >= 0 else len(text)]
    return PathReference(
        file=file,
        text=path,
        anchor=f"#{anchor}" if anchor else "",
        line=text.count("\n", 0, start) + 1,
        col=start - line_start + 1,
        start=start,
        end=start + len(path),
        form=form,
        suppressed=IGNORE_MARKER in line_text,
    )


def _fence_spans(text: str) -> tuple[tuple[int, int], ...]:
    """Locate each fenced code block as a (start, end) offset pair; an unclosed fence runs to the end."""
    spans: list[tuple[int, int]] = []
    opened: int | None = None
    offset = 0
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith(("```", "~~~")):
            if opened is None:
                opened = offset
            else:
                spans.append((opened, offset + len(line)))
                opened = None
        offset += len(line)
    if opened is not None:
        spans.append((opened, len(text)))
    return tuple(spans)
