"""Properties of the SQL builder, checked against reference readers and an injection corpus.

Identifiers and literals round-trip through what Snowflake would read back; a rendered
statement built from arbitrary names and prose stays one statement with no comment; the
expression guard refuses every injection payload and accepts every expression the fixtures
author.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    SemanticView,
    Table,
    VerifiedQuery,
)
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.render.tool import render_tool, search_service_statement
from snowflake_semantic_tools.domain.sql import (
    UnsafeSqlError,
    expr,
    guard_expression,
    guard_query,
    ident,
    literal,
    qname,
    sql,
)
from snowflake_semantic_tools.domain.sql.lexer import TokenKind, tokenize
from tests.helpers.sql_reference import identifier_names, snowflake_reads
from tests.helpers.sql_values import authored, authored_query

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
# Text Snowflake can hold: no NUL, and no lone surrogate.
_CHARACTERS = st.characters(blacklist_characters="\x00", blacklist_categories=["Cs"])
TEXT = st.text(alphabet=_CHARACTERS, max_size=60)
NAMES = st.text(alphabet=_CHARACTERS, min_size=1, max_size=40)
UNQUOTED = st.from_regex(r"[A-Za-z_][A-Za-z0-9_$]{0,20}", fullmatch=True)


def _one_statement(text: str) -> None:
    """Assert `text` lexes as one statement: no comment, no `$$` body, no `;` outside strings."""
    tokens = tokenize(text)
    assert not [token for token in tokens if token.kind in (TokenKind.COMMENT, TokenKind.DOLLAR_STRING)]
    assert not [token for token in tokens if token.kind is TokenKind.PUNCTUATION and token.text == ";"]


@given(NAMES)
def test_any_quoted_identifier_names_exactly_its_text(value: str) -> None:
    rendered = str(ident(Identifier(value, quoted=True)))
    assert identifier_names(rendered) == value
    [token] = tokenize(rendered)
    assert token.kind is TokenKind.QUOTED_IDENTIFIER


@given(UNQUOTED)
def test_an_unquoted_identifier_names_its_upper_cased_text(value: str) -> None:
    assert identifier_names(str(ident(Identifier.parse(value)))) == value.upper()


@given(NAMES, NAMES, NAMES)
def test_a_qualified_name_lexes_as_three_identifiers_and_two_dots(database: str, schema: str, name: str) -> None:
    parts = [Identifier(value, quoted=True) for value in (database, schema, name)]
    tokens = list(tokenize(str(qname(QualifiedName(*parts)))))
    assert [token.kind for token in tokens] == [
        TokenKind.QUOTED_IDENTIFIER,
        TokenKind.PUNCTUATION,
        TokenKind.QUOTED_IDENTIFIER,
        TokenKind.PUNCTUATION,
        TokenKind.QUOTED_IDENTIFIER,
    ]
    assert [identifier_names(token.text) for token in tokens[::2]] == [database, schema, name]


@given(TEXT)
def test_a_literal_lexes_as_one_string_snowflake_reads_back_exactly(value: str) -> None:
    rendered = str(literal(value))
    [token] = tokenize(rendered)
    assert token.kind is TokenKind.STRING
    assert snowflake_reads(rendered) == value


@settings(max_examples=60, deadline=None)
@given(NAMES, TEXT, st.lists(TEXT, max_size=3), st.lists(TEXT, max_size=3), TEXT)
def test_ddl_built_from_any_names_and_prose_is_one_statement(
    name: str, comment: str, synonyms: list[str], samples: list[str], question: str
) -> None:
    quoted = Identifier(name, quoted=True).sql
    view = SemanticView(
        fqn=f"DB.S.{quoted}",
        tables=(Table(logical_name=quoted, fqn=f"DB.S.{quoted}", synonyms=tuple(synonyms), comment=comment),),
        columns=(
            Column(
                table=quoted,
                name=quoted,
                kind=ColumnKind.DIMENSION,
                expr=authored(f"{quoted}.{quoted}"),
                comment=comment,
                synonyms=tuple(synonyms),
                sample_values=tuple(samples),
            ),
        ),
        metrics=(Metric(name=quoted, expr=authored(f"COUNT({quoted}.{quoted})"), table=quoted, comment=comment),),
        comment=comment,
        ai_sql_generation=comment,
        verified_queries=(VerifiedQuery(name=quoted, question=question, sql=authored_query("SELECT 1")),),
        ownership_marker=comment,
    )
    _one_statement(str(render(view)))


@settings(max_examples=60, deadline=None)
@given(NAMES, TEXT, TEXT)
def test_tool_ddl_built_from_any_names_and_prose_is_one_statement(name: str, description: str, comment: str) -> None:
    target = QualifiedName.parse(f"DB.S.{Identifier(name, quoted=True).sql}")
    search = ToolMember(
        group="g",
        name="search",
        type="cortex_search_service",
        ownership=ToolOwnership.DEFINE,
        origin=Origin("tools.yml"),
        source_file="tools.yml",
        description=description,
        search_column=Identifier(name, quoted=True).sql,
        warehouse="WH",
        target_lag=comment or "1 hour",
    )
    _one_statement(str(search_service_statement(search, target, target, comment=comment)))
    for statement in render_tool(replace(search, type="stage"), target, None).statements:
        _one_statement(str(statement))


INJECTIONS = [
    "1); DROP TABLE x; --",
    "' OR 1=1 --",
    "x' OR '1'='1' --",
    "$$",
    "a $$ b",
    "/*",
    "x /* hidden */",
    "x\\'; DROP TABLE t; --",
    "x -- trailing",
    "x // trailing",
    "1; SELECT 1",
    "1 UNION SELECT password FROM users",
    "(SELECT secret FROM vault)",
    "x)) AS y FROM t; DELETE FROM t WHERE (1",
    "CALL SYSTEM$CANCEL_ALL_QUERIES()",
    "GRANT OWNERSHIP ON DATABASE D TO ROLE PUBLIC",
    "a\x00",
    '"unterminated',
    "COALESCE(a, b",
    "x] ",
    "ALTER SESSION SET QUERY_TAG = 'x'",
    "1 \n; DROP TABLE x",
]


@pytest.mark.parametrize("payload", INJECTIONS)
def test_the_expression_guard_refuses_every_injection_payload(payload: str) -> None:
    with pytest.raises(UnsafeSqlError):
        guard_expression(payload)


@pytest.mark.parametrize("payload", ["‘ OR 1=1", "a ’ b", "x ＂ y", "x ； y"])
def test_unicode_quote_and_semicolon_lookalikes_are_inert(payload: str) -> None:
    """Snowflake reads none of these as a quote or a separator, so they stay inside one expression."""
    statement = str(sql("SELECT {value} FROM T", value=expr(guard_expression(payload))))
    _one_statement(statement)


@given(TEXT)
def test_whatever_the_guard_accepts_stays_one_expression_in_a_statement(text: str) -> None:
    try:
        guarded = guard_expression(text)
    except UnsafeSqlError:
        return
    statement = str(sql("SELECT {value} FROM T", value=expr(guarded)))
    _one_statement(statement)
    with pytest.raises(UnsafeSqlError):
        guard_expression(f"{text}; DROP TABLE T")


def _authored(kind: str) -> list[str]:
    """Every authored value of one kind in the fixture projects: `expr`/`where`/`frame`, or `sql`."""
    keys = {"expression": ("expr", "where", "frame"), "query": ("sql",)}[kind]
    found: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if key in keys and isinstance(value, str):
                    found.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    # `target/` holds what compile wrote during other tests, not what a project authors, and
    # `negative_overlays/` is invalid on purpose.
    skipped = {"target", "negative_overlays"}
    for path in sorted(path for path in FIXTURES.rglob("*.yml") if not skipped & set(path.relative_to(FIXTURES).parts)):
        # The loader's own parser, which reads the 0.3 dialect's unquoted templates too.
        walk(parse_yaml_bytes(path.read_bytes(), str(path)).tree)
    if kind == "query":
        found.extend(path.read_text(encoding="utf-8") for path in sorted(FIXTURES.rglob("verified_queries/**/*.sql")))
    return found


def test_the_fixtures_author_expressions_and_queries_to_check() -> None:
    assert len(_authored("expression")) > 20
    assert len(_authored("query")) >= 4


@pytest.mark.parametrize("text", _authored("expression"))
def test_every_fixture_expression_is_accepted(text: str) -> None:
    assert guard_expression(text).text == text


@pytest.mark.parametrize("text", _authored("query"))
def test_every_fixture_query_is_accepted(text: str) -> None:
    assert guard_query(text).text == text
