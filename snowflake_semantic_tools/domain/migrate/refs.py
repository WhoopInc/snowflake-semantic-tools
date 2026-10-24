"""`sst migrate refs`: rewrite legacy references and drop the keys SST now infers, and nothing else.

The rewrite is textual and position-aware, so comments, quoting, spacing, and
key order survive byte for byte:

- `{{ table('x') }}` as an item of a `tables:` list becomes `{{ ref('x') }}`;
- a bare name `x`, or `{{ table('x') }}`, as the whole value of `left_table:` or
  `right_table:` becomes `"{{ ref('x') }}"`, which is what those keys take;
- `{{ column('x', 'y') }}` anywhere becomes `{{ ref('x', 'y') }}`.

A `table()` call anywhere else is reported and left alone, because rewriting it
would be a guess. A boolean standalone filter additionally gains
`labels: [filter]`, classified by the same predicate check validation uses. A
metric's, filter's or verified query's `tables:` and `derived:` keys are dropped,
since SST infers both, and a verified query's `sql_file: x` becomes
`sql: "{{ file('x') }}"` (`rewrite_member_keys`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.validate.expression import is_boolean_expression

_SPAN = re.compile(r"\{\{.*?\}\}")
_RELATION_KEY = re.compile(r"^(\s*)(left_table|right_table)(\s*:\s*)(\S.*?)\s*(#.*)?$")
_LIST_ITEM = re.compile(r"^(\s*)-\s+(\S.*?)\s*(#.*)?$")
_BLOCK_KEY = re.compile(r"^(\s*)(?:-\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(#.*)?$")
_FLOW_TABLES = re.compile(r"^\s*(?:-\s+)?tables\s*:\s*\[.*\]\s*(#.*)?$")
_CALL_NAME = re.compile(r"^\{\{(\s*)(table|column)(\s*)\(")
_BARE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_ROOT_KEY = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:")
_INFERRED_KEY = re.compile(r"^(\s+)(tables|derived)\s*:")
_SQL_FILE_KEY = re.compile(r"^(\s+)sql_file(\s*:\s*)(\S.*?)\s*(#.*)?$")
# The member roots, list and singular, whose `tables:` and `derived:` keys SST now infers.
_INFERRING_ROOTS = frozenset(
    ("snowflake_metrics", "metric", "snowflake_filters", "filter", "snowflake_verified_queries", "verified_query")
)
_QUERY_ROOTS = frozenset(("snowflake_verified_queries", "verified_query"))


@dataclass(frozen=True, slots=True)
class Rewrite:
    """One change the migration made, located in the text it was given.

    Attributes:
        line: The 1-based line of the change.
        col: The 1-based column where the change starts.
        before: The text replaced; "" for an insertion.
        after: The replacement; for an inserted `labels:` block, a summary of the block; "" for
            a dropped key.
        kind: `column`, `ref`, or `endpoint` for a rewritten call or name; `labels` for an added
            filter label; `inferred` for a dropped `tables:` or `derived:` key; `file` for a
            `sql_file:` rewritten as a `file()` call.
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
        body = line.rstrip("\r\n")
        ending = line[len(body) :]
        relation = _RELATION_KEY.match(body)
        if relation is not None and _BARE_NAME.fullmatch(_unquoted(relation.group(4))):
            name = _unquoted(relation.group(4))
            after = f"\"{{{{ ref('{name}') }}}}\""
            rewrites.append(Rewrite(index + 1, relation.start(4) + 1, relation.group(4), after, "endpoint"))
            out.append(body[: relation.start(4)] + after + body[relation.end(4) :] + ending)
            continue
        if "{{" not in line:
            out.append(line)
            continue
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
            if call.function != "table" or len(call.args) != 1:
                continue
            whole_relation = relation is not None and _unquoted(relation.group(4)) == span
            if relation is not None and whole_relation:
                if relation.group(4) == span:
                    # Unquoted, the call would read as a YAML mapping, so it is quoted.
                    after = f"\"{{{{ ref('{call.args[0]}') }}}}\""
                    replacements.append((relation.start(4), relation.end(4), after, "endpoint"))
                else:
                    replacements.append((match.start(), match.end(), _renamed(span), "endpoint"))
            elif (
                item is not None
                and _unquoted(item.group(2)) == span
                and _parent_key(lines, index, len(item.group(1))) == "tables"
            ) or _FLOW_TABLES.match(body):
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


def rewrite_member_keys(result: MigrationResult, keep: frozenset[int] = frozenset()) -> MigrationResult:
    """Drop the member keys SST infers, and write a verified query's `sql_file:` as a `file()` call.

    Each metric's, filter's and verified query's `tables:` and `derived:` key is dropped with
    the lines indented below it, so a block `tables:` list goes too. A verified query's
    `sql_file: x` becomes `sql: "{{ file('x') }}"`, its comment kept. A key written as an
    entry's first line, after its `- `, is left alone: dropping it would leave the entry
    without its dash. A key on a 0-based line in `keep` stays: a `tables:` that names a table
    the member names nowhere else.
    """
    lines = result.text.splitlines(keepends=True)
    kept: list[str] = []
    changes: list[Rewrite] = []
    root: str | None = None
    skip_below: int | None = None
    for index, line in enumerate(lines):
        body = line.rstrip("\r\n")
        indent = len(body) - len(body.lstrip())
        if skip_below is not None:
            if not body.strip() or indent > skip_below:
                continue
            skip_below = None
        if (top := _ROOT_KEY.match(body)) is not None:
            root = top.group(1)
        key = _INFERRED_KEY.match(body)
        if key is not None and root in _INFERRING_ROOTS and index not in keep:
            changes.append(Rewrite(index + 1, indent + 1, body.strip(), "", "inferred"))
            skip_below = indent
            continue
        sql_file = _SQL_FILE_KEY.match(body) if root in _QUERY_ROOTS else None
        if sql_file is not None:
            after = f"sql{sql_file.group(2)}\"{{{{ file('{_unquoted(sql_file.group(3))}') }}}}\""
            comment = f"  {sql_file.group(4)}" if sql_file.group(4) else ""
            changes.append(Rewrite(index + 1, indent + 1, body.strip(), after, "file"))
            kept.append(f"{sql_file.group(1)}{after}{comment}{line[len(body) :]}")
            continue
        kept.append(line)
    if not changes:
        return result
    return MigrationResult(
        "".join(kept),
        tuple(sorted((*result.rewrites, *changes), key=lambda item: (item.line, item.col))),
        result.untouched,
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
