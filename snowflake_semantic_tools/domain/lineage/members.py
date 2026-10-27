"""What each member of a resolved view reads: the dbt models under its tables, and the members it uses.

A member is a node of lineage, between the models its view reads and the view itself, so a change
to a model reaches only the members that read it, and a change to a metric only what uses that
metric. Tables are read from where a member is placed: a fact's or dimension's table, a metric's
table and the tables its expression names, a relationship's two ends, and the tables a verified
query's SQL names. Uses are the metrics an expression or query names, and the relationships a
metric joins through. Names are matched as identifiers, outside the strings and comments of the
text, so the edges are what the text says rather than what Snowflake would resolve.

Columns are `<table>.<column>`, keyed by the table's model or source: a fact's or dimension's
own, a relationship's join keys, and each `TABLE.COLUMN` an expression names, which is what a
two-argument `ref()` resolves to. A verified query names a column through the alias its `FROM`
or `JOIN` gives a table. A bare column is recorded only where one of the member's tables, and
only one, has a column of that name; one that several of them have, or a column qualified by an
alias no table is given (a CTE's or a subquery's), is unresolved. The text is lexed, not parsed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.sql.lexer import Token, TokenKind, tokenize

# Words after a table in a FROM or JOIN clause that start the next clause rather than alias it.
_NOT_ALIASES = frozenset(
    (
        "WHERE",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "CROSS",
        "NATURAL",
        "ON",
        "USING",
        "GROUP",
        "ORDER",
        "LIMIT",
        "UNION",
        "HAVING",
        "QUALIFY",
        "WINDOW",
        "LATERAL",
        "SELECT",
        "AND",
        "OR",
        "EXCEPT",
        "INTERSECT",
        "MINUS",
        "AS",
        "PIVOT",
        "UNPIVOT",
        "SAMPLE",
        "TABLESAMPLE",
    )
)
_NAMES = (TokenKind.WORD, TokenKind.QUOTED_IDENTIFIER)


@dataclass(frozen=True, slots=True)
class MemberEdges:
    """What one member reads.

    Attributes:
        tables: The keys of the dbt models and sources under the tables it reads, as the manifest's
            `dbt_models` records them: a model's name or a source's `<source>.<table>`, casefolded.
        uses: The keys of the members it uses, such as the metrics a derived metric names.
        columns: The columns it reads, as `<table key>.<column>`, casefolded, sorted.
        unresolved: The columns it names that no one table could be found for, as written
            (`cte.amount`, or a bare `amount` two of its tables have), casefolded, sorted.
    """

    tables: tuple[str, ...] = ()
    uses: tuple[str, ...] = ()
    columns: tuple[str, ...] = ()
    unresolved: tuple[str, ...] = ()


def member_edges(view: SemanticView) -> dict[str, MemberEdges]:
    """Return what each member of `view` reads, by member key; a custom instruction reads nothing."""
    tables = _table_keys(view)
    known = _known_columns(view)
    metrics = {metric.name.upper(): artifact_key("metric", metric.name.casefold()) for metric in view.metrics}
    edges: dict[str, MemberEdges] = {}
    # What each fact and dimension reads, by its table and name: an expression naming one reads that.
    derived: dict[tuple[str, str], tuple[str, ...]] = {}
    for column in view.columns:
        kind = column.kind.value if column.kind.value in ("fact", "filter") else "dimension"
        name = column.name if kind == "filter" else column.qualified_name
        found = _Columns(tables, known, (column.table,)).read(column.expr.text)
        if kind != "filter":
            derived[(column.table.upper(), column.name.upper())] = found[0]
        edges[artifact_key(kind, name.casefold())] = MemberEdges(_keys(tables, (column.table,)), (), *found)
    for relationship in view.relationships:
        ends = (relationship.from_table, relationship.to_table)
        keys = _Columns(tables, known, ends)
        for table, names in (
            (relationship.from_table, relationship.from_columns),
            (relationship.to_table, relationship.to_columns),
        ):
            keys.add_all(table, names)
        edges[artifact_key("relationship", relationship.name.casefold())] = MemberEdges(
            _keys(tables, ends), (), *keys.result()
        )
    for metric in view.metrics:
        words = _words(metric.expr.text)
        named = (*(name for name in tables if name in words), *((metric.table,) if metric.table else ()))
        key = artifact_key("metric", metric.name.casefold())
        used = (
            *_named(metrics, words, key),
            *(artifact_key("relationship", name.casefold()) for name in metric.using_relationships),
        )
        found = _Columns(tables, known, named, skip=frozenset(metrics), derived=derived).read(metric.expr.text)
        edges[key] = MemberEdges(_keys(tables, named), tuple(sorted(set(used))), *found)
    for query in view.verified_queries:
        words = _words(query.sql.statement)
        key = artifact_key("verified_query", query.name.casefold())
        named = tuple(name for name in tables if name in words)
        found = _Columns(tables, known, named, skip=frozenset(metrics), derived=derived, aliased=True).read(
            query.sql.statement
        )
        edges[key] = MemberEdges(_keys(tables, named), _named(metrics, words, key), *found)
    return edges


def member_expressions(view: SemanticView) -> dict[str, str]:
    """Return the resolved expression of each fact, dimension, filter and metric of `view`, by member key."""
    found: dict[str, str] = {}
    for column in view.columns:
        kind = column.kind.value if column.kind.value in ("fact", "filter") else "dimension"
        name = column.name if kind == "filter" else column.qualified_name
        found[artifact_key(kind, name.casefold())] = column.expr.text
    for metric in view.metrics:
        found[artifact_key("metric", metric.name.casefold())] = metric.expr.text
    return found


class _Columns:
    """Collect the columns one member's text names, by the table each belongs to.

    Args:
        tables: Each table's model or source key, by its upper-cased logical name.
        known: Each table's columns, upper-cased, by its upper-cased logical name; a table whose
            columns are not known (a source's) has none, so no bare column is found on it.
        named: The member's tables, which a bare column is looked for on.
        skip: Words that are never a column, such as the view's metric names.
        derived: What each fact and dimension reads, by its table and name: a name of one, as an
            expression may write it, records the columns it reads instead of itself.
        aliased: Read the aliases `FROM` and `JOIN` give tables, as a verified query's are.
    """

    def __init__(
        self,
        tables: Mapping[str, str],
        known: Mapping[str, frozenset[str]],
        named: Iterable[str],
        *,
        skip: frozenset[str] = frozenset(),
        derived: Mapping[tuple[str, str], tuple[str, ...]] | None = None,
        aliased: bool = False,
    ) -> None:
        self._tables = tables
        self._known = known
        self._named = tuple(sorted({name.upper() for name in named if name.upper() in tables}))
        self._skip = skip
        self._derived = derived or {}
        self._aliased = aliased
        self._columns: set[str] = set()
        self._unresolved: set[str] = set()

    def add(self, table: str, column: str) -> None:
        """Record `column` of the table logically named `table`; a table with no key records nothing."""
        key = self._tables.get(table.upper())
        if key is not None:
            self._columns.add(f"{key}.{column.casefold()}")

    def add_all(self, table: str, columns: Iterable[str]) -> None:
        """Record each of `columns` of the table logically named `table`."""
        for column in columns:
            self.add(table, column)

    def result(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return the columns found and those left unresolved, each sorted."""
        return tuple(sorted(self._columns)), tuple(sorted(self._unresolved))

    def read(self, text: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Record what `text` names: each qualified column, then each bare one, and return `result`."""
        names = [token for token in tokenize(text) if token.kind not in (TokenKind.SPACE, TokenKind.COMMENT)]
        aliases = _aliases(names, self._tables) if self._aliased else {}
        qualified: set[int] = set()
        for index in range(len(names) - 2):
            qualifier, dot, column = names[index : index + 3]
            if not (qualifier.kind in _NAMES and dot.text == "." and column.kind in _NAMES):
                continue
            if (index > 0 and names[index - 1].text == ".") or (
                index + 3 < len(names) and names[index + 3].text == "."
            ):
                continue
            qualified.update((index, index + 2))
            owner = aliases.get(_name(qualifier)) or _name(qualifier)
            if _name(column) in self._skip:
                continue
            if (owner, _name(column)) in self._derived:
                self._columns.update(self._derived[(owner, _name(column))])
            elif owner in self._tables:
                self.add(owner, _name(column))
            elif self._aliased:
                self._unresolved.add(f"{_name(qualifier)}.{_name(column)}".casefold())
        for index, token in enumerate(names):
            if index in qualified or token.kind is not TokenKind.WORD or _name(token) in self._skip:
                continue
            if index + 1 < len(names) and names[index + 1].text in (".", "("):
                continue
            self._bare(_name(token))
        return self.result()

    def _bare(self, word: str) -> None:
        """Record a bare word that is a column of exactly one of the member's tables; unresolved for several."""
        owners = [table for table in self._named if word in self._known.get(table, frozenset())]
        if len(owners) == 1:
            self.add(owners[0], word)
        elif len(owners) > 1:
            self._unresolved.add(word.casefold())


def _aliases(names: list[Token], tables: Mapping[str, str]) -> dict[str, str]:
    """Map each alias a `FROM` or `JOIN` gives a table to the table's logical name, upper-cased."""
    aliases: dict[str, str] = {}
    for index, token in enumerate(names[:-1]):
        if token.kind not in _NAMES or _name(token) not in tables:
            continue
        if index > 0 and names[index - 1].text == ".":
            continue
        following = index + 1
        if names[following].kind is TokenKind.WORD and _name(names[following]) == "AS":
            following += 1
        if following < len(names) and names[following].kind in _NAMES:
            alias = _name(names[following])
            if alias not in _NOT_ALIASES and (following + 1 >= len(names) or names[following + 1].text != "."):
                aliases[alias] = _name(token)
    return aliases


def _name(token: Token) -> str:
    """Return an identifier token's name, upper-cased; a quoted one unquoted."""
    if token.kind is TokenKind.QUOTED_IDENTIFIER:
        return token.text[1:-1].replace('""', '"').upper()
    return token.text.upper()


def _known_columns(view: SemanticView) -> Mapping[str, frozenset[str]]:
    """Map each table with a dbt model to that model's columns, upper-cased, by logical name."""
    return {
        logical.upper(): frozenset(column.name.upper() for column in model.columns)
        for logical, model in view.table_models.items()
    }


def _table_keys(view: SemanticView) -> Mapping[str, str]:
    """Map each table's upper-cased logical name to its model's or source's key.

    A table a `source()` names has no model; its key is the `<source>.<table>` the view records
    among its referenced models, matched by the table's name.
    """
    keys: dict[str, str] = {}
    sources = [name for name in view.referenced_models if "." in name]
    for table in view.tables:
        logical = table.logical_name.upper()
        model = view.table_models.get(table.logical_name)
        if model is not None:
            keys[logical] = model.name.casefold()
            continue
        pair = next((name for name in sources if name.split(".", 1)[1].upper() == logical), None)
        if pair is not None:
            keys[logical] = pair
    return keys


def _keys(tables: Mapping[str, str], logical: Iterable[str]) -> tuple[str, ...]:
    """Return the sorted keys of the named tables, by logical name; a name no table has is dropped."""
    return tuple(sorted({tables[name.upper()] for name in logical if name.upper() in tables}))


def _named(metrics: Mapping[str, str], words: frozenset[str], own: str) -> tuple[str, ...]:
    """Return the sorted keys of the metrics `words` names, besides the member's own."""
    return tuple(sorted(key for name, key in metrics.items() if name in words and key != own))


def _words(text: str) -> frozenset[str]:
    """Return the upper-cased identifiers of `text` outside its strings and comments, quoted ones unquoted.

    The text is an expression or query the SQL guard accepted, so it lexes.
    """
    words: set[str] = set()
    for token in tokenize(text):
        if token.kind is TokenKind.WORD:
            words.add(token.text.upper())
        elif token.kind is TokenKind.QUOTED_IDENTIFIER:
            words.add(token.text[1:-1].replace('""', '"').upper())
    return frozenset(words)
