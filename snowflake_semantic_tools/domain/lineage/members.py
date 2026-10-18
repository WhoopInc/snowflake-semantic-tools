"""What each member of a resolved view reads: the dbt models under its tables, and the members it uses.

A member is a node of lineage, between the models its view reads and the view itself, so a change
to a model reaches only the members that read it, and a change to a metric only what uses that
metric. Tables are read from where a member is placed: a fact's or dimension's table, a metric's
table and the tables its expression names, a relationship's two ends, and the tables a verified
query's SQL names. Uses are the metrics an expression or query names, and the relationships a
metric joins through. Names are matched as identifiers, outside the strings and comments of the
text, so the edges are what the text says rather than what Snowflake would resolve.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.sql.lexer import TokenKind, tokenize


@dataclass(frozen=True, slots=True)
class MemberEdges:
    """What one member reads.

    Attributes:
        tables: The keys of the dbt models and sources under the tables it reads, as the manifest's
            `dbt_models` records them: a model's name or a source's `<source>.<table>`, casefolded.
        uses: The keys of the members it uses, such as the metrics a derived metric names.
    """

    tables: tuple[str, ...] = ()
    uses: tuple[str, ...] = ()


def member_edges(view: SemanticView) -> dict[str, MemberEdges]:
    """Return what each member of `view` reads, by member key; a custom instruction reads nothing."""
    tables = _table_keys(view)
    metrics = {metric.name.upper(): artifact_key("metric", metric.name.casefold()) for metric in view.metrics}
    edges: dict[str, MemberEdges] = {}
    for column in view.columns:
        kind = column.kind.value if column.kind.value in ("fact", "filter") else "dimension"
        name = column.name if kind == "filter" else column.qualified_name
        edges[artifact_key(kind, name.casefold())] = MemberEdges(_keys(tables, (column.table,)))
    for relationship in view.relationships:
        ends = (relationship.from_table, relationship.to_table)
        edges[artifact_key("relationship", relationship.name.casefold())] = MemberEdges(_keys(tables, ends))
    for metric in view.metrics:
        words = _words(metric.expr.text)
        named = (*(name for name in tables if name in words), *((metric.table,) if metric.table else ()))
        key = artifact_key("metric", metric.name.casefold())
        used = (
            *_named(metrics, words, key),
            *(artifact_key("relationship", name.casefold()) for name in metric.using_relationships),
        )
        edges[key] = MemberEdges(_keys(tables, named), tuple(sorted(set(used))))
    for query in view.verified_queries:
        words = _words(query.sql.statement)
        key = artifact_key("verified_query", query.name.casefold())
        edges[key] = MemberEdges(_keys(tables, (name for name in tables if name in words)), _named(metrics, words, key))
    return edges


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
