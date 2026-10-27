"""Rewrite a live semantic view's expressions as the project writes them: `ref()` and `metric()` calls.

Snowflake shows a member's expression with logical names: `SUM(ORDERS.ORDER_TOTAL)`,
`DIV0(GROSS_PROFIT, ORDERS.TOTAL_REVENUE)`. The project writes the same expression with the
calls that resolve to them, `SUM({{ ref('orders', 'order_total') }})`, so a rename in dbt
reaches every member. Each `TABLE.NAME` whose table is in the view becomes a `metric()` when
NAME is one of the view's metrics on that table, else a `ref()` to the table's model; a bare name
that is one of the view's metrics becomes a `metric()`, and anything else -- a function, a
keyword, a variable, a string -- is kept as written. A window metric's `OVER (...)` is read
into the parts of a `window:` block.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.sql.lexer import LexError, Token, TokenKind, tokenize


@dataclass(frozen=True, slots=True)
class ViewNames:
    """The names a view's expressions use, by how they are written live, each upper-cased.

    Attributes:
        tables: Each logical table's model, by the table's logical name.
        columns: Each model's columns, by name upper-cased, as the model's YAML spells them.
        table_metrics: Each table-scoped metric's name as written, by `(TABLE, METRIC)`.
        metrics: Each metric's name as written, by its bare name.
    """

    tables: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    columns: Mapping[str, Mapping[str, str]] = field(default_factory=lambda: MappingProxyType({}))
    table_metrics: Mapping[tuple[str, str], str] = field(default_factory=lambda: MappingProxyType({}))
    metrics: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def column(self, table: str, name: str) -> str:
        """Return a column as its model's YAML spells it; as written live, lowercased, when it has none."""
        model = self.tables.get(table.upper(), "")
        return self.columns.get(model, {}).get(name.upper(), name.casefold())


@dataclass(frozen=True, slots=True)
class Rewritten:
    """An expression as the project writes it, and the models its `ref()` calls name."""

    text: str
    models: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LiveWindow:
    """A window metric's `OVER (...)`, its entries rewritten as references.

    Attributes:
        partition_by, partition_excluding: The partition's entries; at most one is set.
        order_by: Each ordering entry: its reference, `sort_direction`, and `null_order` when stated.
        frame: The frame as written, such as `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`.
    """

    partition_by: tuple[str, ...] = ()
    partition_excluding: tuple[str, ...] = ()
    order_by: tuple[Mapping[str, str], ...] = ()
    frame: str = ""


def rewrite(expression: str, names: ViewNames) -> Rewritten:
    """Return `expression` with each name it references written as a `ref()` or `metric()` call.

    Raises:
        LexError: the expression does not lex, such as an unterminated string.
    """
    tokens = tokenize(expression)
    out: list[str] = []
    models: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.kind is TokenKind.WORD and not _called(tokens, index):
            pair = _dotted(tokens, index)
            if pair is not None:
                table, name = pair
                reference = _qualified(table, name, names, models)
                if reference is not None:
                    out.append(reference)
                    index += 3
                    continue
            elif not _after_dot(tokens, index):
                metric = names.metrics.get(token.text.upper())
                if metric is not None:
                    out.append("{{ metric('" + metric + "') }}")
                    index += 1
                    continue
        out.append(token.text)
        index += 1
    return Rewritten("".join(out), tuple(dict.fromkeys(models)))


def split_window(expression: str) -> tuple[str, str] | None:
    """Split a window metric's expression at its top-level `OVER`: the aggregate, and what OVER holds.

    None when the expression has no `OVER (...)` outside parentheses, or does not lex.
    """
    try:
        tokens = tokenize(expression)
    except LexError:
        return None
    depth = 0
    for index, token in enumerate(tokens):
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
        elif depth == 0 and token.kind is TokenKind.WORD and token.text.upper() == "OVER":
            rest = "".join(item.text for item in tokens[index + 1 :]).strip()
            if rest.startswith("(") and rest.endswith(")"):
                return "".join(item.text for item in tokens[:index]).strip(), rest[1:-1].strip()
    return None


def parse_window(inside: str, names: ViewNames) -> LiveWindow | None:
    """Read the inside of `OVER (...)` into a window's parts; None when it is not that grammar.

    `[PARTITION BY [EXCLUDING] <refs>] [ORDER BY <ref> [ASC|DESC] [NULLS FIRST|LAST], ...] [<frame>]`.
    """
    words = _top_level_words(inside)
    if words is None:
        return None
    marks = [(position, word) for position, word in words if word in ("PARTITION", "ORDER", "ROWS", "RANGE")]
    sections: dict[str, str] = {}
    for number, (position, word) in enumerate(marks):
        end = marks[number + 1][0] if number + 1 < len(marks) else len(inside)
        sections[word] = inside[position:end].strip()
    partition_by: tuple[str, ...] = ()
    partition_excluding: tuple[str, ...] = ()
    partition = sections.get("PARTITION")
    if partition is not None:
        body = _after_keywords(partition, ("PARTITION", "BY"))
        excluding = body.upper().startswith("EXCLUDING")
        entries = _references(_after_keywords(body, ("EXCLUDING",)) if excluding else body, names)
        if entries is None:
            return None
        if excluding:
            partition_excluding = entries
        else:
            partition_by = entries
    order_by: tuple[Mapping[str, str], ...] = ()
    ordering = sections.get("ORDER")
    if ordering is not None:
        ordered = _ordering(_after_keywords(ordering, ("ORDER", "BY")), names)
        if ordered is None:
            return None
        order_by = ordered
    if not sections and inside.strip():
        return None
    frame = sections.get("ROWS") or sections.get("RANGE") or ""
    return LiveWindow(partition_by, partition_excluding, order_by, " ".join(frame.split()))


def rewrite_tables(sql_text: str, names: ViewNames) -> Rewritten:
    """Return a verified query's SQL with each table it reads by logical name written as `{{ ref() }}`.

    Only a name right after `FROM` or `JOIN` counts; a table listed after a comma is kept, and
    SST-VAL414 then reports it. A column such as `orders.order_id` keeps its alias as written.

    Raises:
        LexError: the SQL does not lex.
    """
    tokens = tokenize(sql_text)
    out: list[str] = []
    models: list[str] = []
    expecting = False
    for token in tokens:
        if token.kind is TokenKind.WORD:
            upper = token.text.upper()
            model = names.tables.get(upper)
            if expecting and model is not None:
                out.append("{{ ref('" + model + "') }}")
                models.append(model)
                expecting = False
                continue
            expecting = upper in ("FROM", "JOIN")
        elif token.kind not in (TokenKind.SPACE, TokenKind.COMMENT):
            expecting = False
        out.append(token.text)
    return Rewritten("".join(out), tuple(dict.fromkeys(models)))


def _qualified(table: str, name: str, names: ViewNames, models: list[str]) -> str | None:
    model = names.tables.get(table.upper())
    if model is None:
        return None
    metric = names.table_metrics.get((table.upper(), name.upper()))
    if metric is not None:
        return "{{ metric('" + metric + "') }}"
    models.append(model)
    return "{{ ref('" + model + "', '" + names.column(table, name) + "') }}"


def _dotted(tokens: tuple[Token, ...], index: int) -> tuple[str, str] | None:
    """Return `A.B` starting at `index`, as two words; None when the words there are not one."""
    if index + 2 >= len(tokens) or tokens[index + 1].text != "." or tokens[index + 2].kind is not TokenKind.WORD:
        return None
    if index + 3 < len(tokens) and tokens[index + 3].text == ".":
        return None
    if _after_dot(tokens, index):
        return None
    return tokens[index].text, tokens[index + 2].text


def _after_dot(tokens: tuple[Token, ...], index: int) -> bool:
    return index > 0 and tokens[index - 1].text == "."


def _called(tokens: tuple[Token, ...], index: int) -> bool:
    """Report whether the word at `index` names a function: an opening parenthesis follows it."""
    following = next((token for token in tokens[index + 1 :] if token.kind is not TokenKind.SPACE), None)
    return following is not None and following.text == "("


def _top_level_words(text: str) -> list[tuple[int, str]] | None:
    """Return each word outside parentheses, upper-cased, with its offset; None when the text does not lex."""
    try:
        tokens = tokenize(text)
    except LexError:
        return None
    depth = 0
    words: list[tuple[int, str]] = []
    for token in tokens:
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
        elif depth == 0 and token.kind is TokenKind.WORD:
            words.append((token.offset, token.text.upper()))
    return words


def _after_keywords(text: str, keywords: tuple[str, ...]) -> str:
    rest = text.strip()
    for keyword in keywords:
        if rest.upper().startswith(keyword):
            rest = rest[len(keyword) :].strip()
    return rest


def _split_commas(text: str) -> list[str] | None:
    try:
        tokens = tokenize(text)
    except LexError:
        return None
    return _commas(tokens)


def _commas(tokens: Sequence[Token]) -> list[str]:
    """Return the text between the commas outside parentheses, each part stripped."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for token in tokens:
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
        if token.text == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(token.text)
    parts.append("".join(current).strip())
    return [part for part in parts if part]


def _references(text: str, names: ViewNames) -> tuple[str, ...] | None:
    """Rewrite each comma-separated entry, which must be one reference; None when one is not."""
    parts = _split_commas(text)
    if parts is None:
        return None
    rewritten = tuple(rewrite(part, names).text for part in parts)
    if not all(item.startswith("{{") and item.endswith("}}") and item.count("{{") == 1 for item in rewritten):
        return None
    return rewritten


def _ordering(text: str, names: ViewNames) -> tuple[Mapping[str, str], ...] | None:
    """Read `ORDER BY`'s entries: each a reference, `ASC` or `DESC`, and `NULLS FIRST` or `LAST`."""
    # `text` is a slice of SQL that already lexed, cut at a top-level word, so it lexes too.
    parts = _commas(tokenize(text))
    entries: list[Mapping[str, str]] = []
    for part in parts:
        words = part.split()
        modifiers = [word.upper() for word in words[1:]]
        reference = _references(words[0], names) if words else None
        if reference is None:
            return None
        entry: dict[str, str] = {"ref": reference[0]}
        if "DESC" in modifiers:
            entry["sort_direction"] = "descending"
        elif "ASC" in modifiers:
            entry["sort_direction"] = "ascending"
        if "NULLS" in modifiers and modifiers.index("NULLS") + 1 < len(modifiers):
            entry["null_order"] = modifiers[modifiers.index("NULLS") + 1].casefold()
        if set(modifiers) - {"ASC", "DESC", "NULLS", "FIRST", "LAST"}:
            return None
        entries.append(entry)
    return tuple(entries)
