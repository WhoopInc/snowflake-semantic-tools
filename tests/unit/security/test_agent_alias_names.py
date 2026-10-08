"""An agent's version alias is a plain, unreserved identifier, or it is refused before any SQL is built.

`ALTER AGENT ... SET ALIAS = <alias>` writes the alias unquoted. A reserved word there is a
syntax error, and quoting it to rescue it would make the alias case-sensitive for every later
reference, so validation refuses it (SST-PRS025) and so does the renderer. A name that is not
a plain identifier -- written quoted, or valid only once quoted -- is refused too (SST-PRS005).
A tag name names an existing object, so a reserved word in one is quoted as the upper-cased
name it already resolves to.
"""

from __future__ import annotations

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import RESERVED_AGENT_ALIASES, AgentModel, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.parse.names import RESERVED_WORDS
from snowflake_semantic_tools.domain.validate.agent import agent_rules, plain_alias
from tests.helpers.sql_values import texts

_PLAIN = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
RESERVED = sorted(RESERVED_WORDS | RESERVED_AGENT_ALIASES)
SPECIAL = (
    "pro-moted",
    "pro moted",
    '"promoted"',
    '"Prod"',
    '"select"',
    "a.b.c",
    'x"; DROP AGENT y; --',
    "x'; DROP",
    "a$$b",
    "1st",
    "prodé",
    "line\nbreak",
    "semi;colon",
    "",
    " ",
)


def _alias_codes(alias: str) -> list[str]:
    model = AgentModel("agent", Origin("agent.yml"), ("agent.yml",), alias=alias)
    return [item.code for item in agent_rules(model, frozenset(("auto",)), ()) if item.code in _ALIAS_CODES]


_ALIAS_CODES = frozenset(("SST-PRS025", "SST-PRS005"))


def _statements(alias: str | None, tags: tuple[tuple[str, str], ...] = ()) -> tuple[str, ...]:
    model = AgentModel("agent", Origin("agent.yml"), ("agent.yml",), alias=alias, tags=tags)
    agent = CompiledAgent(ResolvedAgent(model, ()), QualifiedName.parse("DB.S.AGENT"), "{}\n", "a" * 64)
    published = for_publication(agent, stage=QualifiedName.parse("DB.S.SPECS"), git_sha="abc1234")
    return texts(published.rendered_artifact.statements)


@pytest.mark.parametrize("word", RESERVED)
@pytest.mark.parametrize("spelling", [str.lower, str.upper, str.title])
def test_every_reserved_word_is_refused_as_an_alias(word: str, spelling: object) -> None:
    alias = spelling(word)  # type: ignore[operator]
    assert _alias_codes(alias) == ["SST-PRS025"]
    assert not plain_alias(alias)
    with pytest.raises(ValueError, match="not a plain, unreserved identifier"):
        _statements(alias)


@pytest.mark.parametrize("alias", [item for item in SPECIAL if item.strip()])
def test_an_alias_that_is_not_a_plain_identifier_is_refused_and_never_rendered(alias: str) -> None:
    assert _alias_codes(alias) in (["SST-PRS005"], ["SST-PRS025"])
    with pytest.raises(ValueError):
        _statements(alias)


def test_the_reserved_message_names_the_word() -> None:
    model = AgentModel("agent", Origin("agent.yml"), ("agent.yml",), alias="select")
    [diagnostic] = [item for item in agent_rules(model, frozenset(("auto",)), ()) if item.code == "SST-PRS025"]
    assert diagnostic.message == "agent: alias 'select' is reserved"


@settings(max_examples=300, deadline=None)
@given(st.from_regex(_PLAIN, fullmatch=True).filter(lambda value: len(value) <= 255))
def test_a_plain_unreserved_alias_renders_unquoted_and_upper_cased(alias: str) -> None:
    reserved = alias.upper() in RESERVED_WORDS | RESERVED_AGENT_ALIASES
    if reserved or "$$" in alias:
        assert _alias_codes(alias) != []
        return
    assert _alias_codes(alias) == []
    assert f'MODIFY VERSION "LAST" SET ALIAS = {alias.upper()}' in "\n".join(_statements(alias))


@settings(max_examples=300, deadline=None)
@given(st.text(min_size=1, max_size=40))
def test_any_alias_is_either_rendered_plain_or_refused_before_rendering(alias: str) -> None:
    codes = _alias_codes(alias)
    if codes:
        with pytest.raises(ValueError):
            _statements(alias)
        return
    [statement] = [text for text in _statements(alias) if "SET ALIAS" in text]
    rendered = statement.rsplit("= ", 1)[1]
    assert _PLAIN.fullmatch(rendered) is not None
    assert rendered == alias.strip().upper()
    assert rendered not in RESERVED_WORDS | RESERVED_AGENT_ALIASES


@pytest.mark.parametrize(
    ("tag", "rendered"),
    [
        ("select", '"SELECT"'),
        ("Order", '"ORDER"'),
        ("DB.S.ORDER", 'DB.S."ORDER"'),
        ("table.schema.domain", '"TABLE"."SCHEMA".DOMAIN'),
        ('"select"', '"select"'),
        ("domain", "DOMAIN"),
    ],
)
def test_a_reserved_word_in_a_tag_name_is_quoted_as_the_name_it_resolves_to(tag: str, rendered: str) -> None:
    [statement] = [text for text in _statements(None, ((tag, "v"),)) if "SET TAG" in text]
    assert statement.endswith(f"SET TAG {rendered} = 'v'")
