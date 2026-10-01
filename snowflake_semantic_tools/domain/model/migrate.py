"""`sst migrate refs`: rewrite the legacy reference globals and nothing else.

The rewrite is textual and position-aware, so comments, quoting, spacing, and
key order survive byte for byte:

- `{{ table('x') }}` as an item of a `tables:` list becomes `{{ ref('x') }}`;
- `{{ table('x') }}` or `{{ ref('x') }}` as the whole value of `left_table:` or
  `right_table:` becomes the bare logical name `x`, which is what those keys take;
- `{{ column('x', 'y') }}` anywhere becomes `{{ ref('x', 'y') }}`.

A `table()` call anywhere else is reported and left alone, because rewriting it
would be a guess. A boolean standalone filter additionally gains
`labels: [filter]`, classified by the same predicate check validation uses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .expression import is_boolean_expression
from .reference import TemplateSyntaxError, scan_template_calls

_SPAN = re.compile(r"\{\{.*?\}\}")
_RELATION_KEY = re.compile(r"^(\s*)(left_table|right_table)(\s*:\s*)(\S.*?)\s*(#.*)?$")
_LIST_ITEM = re.compile(r"^(\s*)-\s+(\S.*?)\s*(#.*)?$")
_BLOCK_KEY = re.compile(r"^(\s*)(?:-\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(#.*)?$")
_FLOW_TABLES = re.compile(r"^\s*(?:-\s+)?tables\s*:\s*\[.*\]\s*(#.*)?$")
_CALL_NAME = re.compile(r"^\{\{(\s*)(table|column)(\s*)\(")


@dataclass(frozen=True, slots=True)
class Rewrite:
    """One change the migration made, located in the text it was given.

    Attributes:
        line: The 1-based line of the change.
        col: The 1-based column where the change starts.
        before: The text replaced; "" for an insertion.
        after: The replacement; for an inserted `labels:` block, a summary of the block.
        kind: `column`, `ref`, or `bare` for a rewritten call; `labels` for an added filter label.
    """

    line: int
    col: int
    before: str
    after: str
    kind: str


@dataclass(frozen=True, slots=True)
class Untouched:
    """A `table()` call the migration left alone, because rewriting it would be a guess.

    Attributes:
        line: The 1-based line of the call.
        col: The 1-based column where the call starts.
        text: The call as written, braces included.
        reason: Why it was left alone.
    """

    line: int
    col: int
    text: str
    reason: str


@dataclass(frozen=True, slots=True)
class FilterSite:
    """One `snowflake_filters` entry, located so `labels:` can be inserted after it."""

    name: str
    expr: str
    has_labels: bool
    last_line: int
    indent: int


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """One file's migrated text, every change made to it, and every call left alone.

    Attributes:
        text: The migrated text; byte for byte the input when nothing changed.
        rewrites: The changes, sorted by line and then column.
        untouched: The `table()` calls left alone, in source order.
    """

    text: str
    rewrites: tuple[Rewrite, ...] = ()
    untouched: tuple[Untouched, ...] = ()

    @property
    def changed(self) -> bool:
        """Whether anything was rewritten; a call left untouched does not count."""
        return bool(self.rewrites)


def _unquoted(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _parent_key(lines: list[str], index: int, indent: int) -> str | None:
    """The block key a list item at `indent` belongs to, walking back over siblings."""
    for previous in range(index - 1, -1, -1):
        text = lines[previous].rstrip("\n")
        stripped = text.strip()
        if not stripped or stripped.startswith("#"):
            continue
        current = len(text) - len(text.lstrip())
        if current > indent or (current == indent and stripped.startswith("- ")):
            continue
        match = _BLOCK_KEY.match(text)
        return match.group(2) if match is not None else None
    return None


def _renamed(span: str) -> str:
    return _CALL_NAME.sub(lambda match: f"{{{{{match.group(1)}ref{match.group(3)}(", span, count=1)


def migrate_refs(text: str) -> MigrationResult:
    """Rewrite one YAML file's legacy reference calls as the module describes, keeping every other byte.

    Each line is read on its own, so a template that spans lines is never seen. A template that
    does not parse, and a `table()` or `column()` call with the wrong number of arguments, is
    left as written and not reported. Filter labels are added separately, by `add_filter_labels`.
    """
    lines = text.splitlines(keepends=True)
    rewrites: list[Rewrite] = []
    untouched: list[Untouched] = []
    out: list[str] = []
    for index, line in enumerate(lines):
        if "{{" not in line:
            out.append(line)
            continue
        body = line.rstrip("\r\n")
        ending = line[len(body) :]
        relation = _RELATION_KEY.match(body)
        item = _LIST_ITEM.match(body)
        replacements: list[tuple[int, int, str, str]] = []
        for match in _SPAN.finditer(body):
            span = match.group(0)
            try:
                calls = scan_template_calls(span)
            except TemplateSyntaxError:
                continue
            call = calls[0]
            if call.function == "column" and len(call.args) == 2:
                replacements.append((match.start(), match.end(), _renamed(span), "column"))
                continue
            whole_relation = relation is not None and _unquoted(relation.group(4)) == span
            if call.function == "ref" and len(call.args) == 1 and relation is not None and whole_relation:
                replacements.append((relation.start(4), relation.end(4), call.args[0], "bare"))
                continue
            if call.function != "table" or len(call.args) != 1:
                continue
            if relation is not None and whole_relation:
                start = relation.start(4)
                replacements.append((start, relation.end(4), call.args[0], "bare"))
            elif (
                item is not None
                and _unquoted(item.group(2)) == span
                and _parent_key(lines, index, len(item.group(1))) == "tables"
            ):
                replacements.append((match.start(), match.end(), _renamed(span), "ref"))
            elif _FLOW_TABLES.match(body):
                replacements.append((match.start(), match.end(), _renamed(span), "ref"))
            else:
                untouched.append(
                    Untouched(
                        index + 1, match.start() + 1, span, "table() outside a tables: list or a relationship table key"
                    )
                )
        for start, end, after, kind in sorted(replacements, reverse=True):
            rewrites.append(Rewrite(index + 1, start + 1, body[start:end], after, kind))
            body = body[:start] + after + body[end:]
        out.append(body + ending)
    return MigrationResult(
        "".join(out),
        tuple(sorted(rewrites, key=lambda item: (item.line, item.col))),
        tuple(untouched),
    )


def add_filter_labels(result: MigrationResult, sites: tuple[FilterSite, ...]) -> MigrationResult:
    """Give every boolean filter without a `labels:` key the `labels: [filter]` it needs."""
    lines = result.text.splitlines(keepends=True)
    added: list[Rewrite] = []
    for site in sorted(sites, key=lambda item: item.last_line, reverse=True):
        if site.has_labels or not is_boolean_expression(site.expr):
            continue
        prefix = " " * site.indent
        block = f"{prefix}labels:\n{prefix}  - filter\n"
        position = min(site.last_line, len(lines))
        if position and not lines[position - 1].endswith("\n"):
            lines[position - 1] += "\n"
        lines.insert(position, block)
        added.append(Rewrite(site.last_line + 1, site.indent + 1, "", "labels: [filter]", "labels"))
    return MigrationResult(
        "".join(lines),
        tuple(sorted((*result.rewrites, *added), key=lambda item: (item.line, item.col))),
        result.untouched,
    )
