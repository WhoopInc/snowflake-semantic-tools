"""The Snowflake spec tables agree with what SST reads, and say why each key is written.

Every source a key names exists: an authored field declared current, a configuration setting
SST reads. Every current authored field and setting of an agent or CoWork profile is the source
of some key, a passthrough block, or listed as not in the specification with where it goes.
Every key has evidence: a documentation page from `docs`, or a measured section of
`docs/design/snowflake-behavior.md`; and no package module but `docs` writes a docs link.
"""

from __future__ import annotations

import re

import pytest

from snowflake_semantic_tools.domain import snowflake_docs as docs
from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS, Field, FieldStatus, Node
from snowflake_semantic_tools.domain.model.config_schema import CHILDREN, KeyStatus
from snowflake_semantic_tools.domain.model.snowflake_spec import (
    AGENT_NOT_IN_SPEC,
    AGENT_PASSTHROUGH,
    AGENT_SPEC,
    COWORK_NOT_IN_SPEC,
    COWORK_PASSTHROUGH,
    COWORK_SPEC,
    TOOL_RESOURCES,
    Authored,
    Documented,
    Measured,
    Setting,
    SpecNode,
    sources,
    walk,
)
from tests.helpers.reference_project import REPO_ROOT

PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
BEHAVIOR = REPO_ROOT / "docs" / "design" / "snowflake-behavior.md"
# Each table, with the authored kind and configuration block it is filled from.
TABLES: dict[str, tuple[SpecNode, ...]] = {
    "agent": (AGENT_SPEC, *TOOL_RESOURCES.values()),
    "cowork_profile": (COWORK_SPEC,),
}
_BLOCKS = {"agent": "agents", "cowork_profile": "cowork_profiles"}
_PASSTHROUGH = {"agent": AGENT_PASSTHROUGH, "cowork_profile": COWORK_PASSTHROUGH}
_NOT_IN_SPEC = {"agent": AGENT_NOT_IN_SPEC, "cowork_profile": COWORK_NOT_IN_SPEC}


def _field(kind: str, path: str) -> Field | None:
    """The authored field at `path` in `kind`'s table, list entries left out; None when there is none."""
    node: Node | None = FILE_KINDS[kind].node
    found: Field | None = None
    for part in path.split("."):
        if node is None:
            return None
        found = next((item for item in node.all_fields() if item.name == part), None)
        if found is None:
            return None
        node = found.nested if found.config_block is None else None
    return found


def _leaves(node: Node, prefix: str = "") -> list[str]:
    """The current fields below `node` that hold a value rather than nested keys, as dotted paths."""
    found: list[str] = []
    for item in node.all_fields():
        if item.status is not FieldStatus.CURRENT or item.config_block is not None:
            continue
        path = f"{prefix}{item.name}"
        nested = item.nested
        if nested is None:
            found.append(path)
        else:
            found += _leaves(nested, f"{path}.")
    return found


@pytest.mark.parametrize("kind", sorted(TABLES))
def test_every_source_names_a_current_field_or_setting(kind: str) -> None:
    for table in TABLES[kind]:
        for path, source in sources(table):
            if isinstance(source, Authored):
                found = _field(source.kind, source.path)
                assert found is not None and found.status is FieldStatus.CURRENT, (path, source)
            elif isinstance(source, Setting):
                key = CHILDREN.get(source.block, {}).get(f"+{source.key}")
                assert key is not None and key.status is KeyStatus.CURRENT, (path, source)


@pytest.mark.parametrize("kind", sorted(TABLES))
def test_every_current_field_and_setting_is_in_the_spec_or_says_where_it_goes(kind: str) -> None:
    named = {
        f"{source.kind}:{source.path}" if isinstance(source, Authored) else f"{source.block}.{source.key}"
        for table in TABLES[kind]
        for _, source in sources(table)
        if isinstance(source, (Authored, Setting))
    }
    passthrough = {f"{kind}:{path}" for path in _PASSTHROUGH[kind]}
    authored = {f"{kind}:{path}" for path in _leaves(FILE_KINDS[kind].node)}
    block = _BLOCKS[kind]
    settings = {
        f"{block}.{name[1:]}"
        for name, key in CHILDREN[block].items()
        if name.startswith("+") and key.status is KeyStatus.CURRENT
    }
    unplaced = (authored | settings) - named - passthrough - set(_NOT_IN_SPEC[kind])
    assert sorted(unplaced) == []
    # A reason for something that is in the spec, or not read at all, is stale.
    assert sorted(set(_NOT_IN_SPEC[kind]) - (authored | settings)) == []
    assert sorted(set(_NOT_IN_SPEC[kind]) & (named | passthrough)) == []


@pytest.mark.parametrize("kind", sorted(TABLES))
def test_every_key_has_evidence(kind: str) -> None:
    for table in TABLES[kind]:
        for path, _, evidence in walk(table):
            assert evidence is not None, path


def test_every_cited_page_is_a_docs_constant_and_every_measured_section_exists() -> None:
    headings = set(re.findall(r"^#+ (.+?)\s*$", BEHAVIOR.read_text(encoding="utf-8"), re.MULTILINE))
    for table in (node for tables in TABLES.values() for node in tables):
        for path, _, evidence in walk(table):
            if isinstance(evidence, Documented):
                assert evidence.page in docs.PAGES, path
            elif isinstance(evidence, Measured):
                assert evidence.section in headings, (path, evidence.section)


def test_every_docs_page_is_a_snowflake_docs_address() -> None:
    assert len({page.url for page in docs.PAGES}) == len(docs.PAGES)
    for page in docs.PAGES:
        assert page.url.startswith("https://docs.snowflake.com/en/"), page
        assert page.preview == ("/LIMITEDACCESS/" in page.url), page


def test_no_package_module_but_docs_pastes_a_docs_link() -> None:
    pasting = {
        path.relative_to(PACKAGE).as_posix()
        for path in PACKAGE.rglob("*.py")
        if "docs.snowflake.com" in path.read_text(encoding="utf-8")
    }
    # A docstring names a page by its constant; a message builds the link from its `url`.
    assert sorted(pasting) == ["domain/snowflake_docs.py"]


# Pages cited for a statement or another table rather than a key here: CREATE AGENT and ALTER
# AGENT by the renderer's statements, and the semantic view pages by the READ_YAML reader.
_NOT_KEY_PAGES = frozenset(
    (docs.CREATE_AGENT, docs.ALTER_AGENT, docs.READ_YAML_FROM_SEMANTIC_VIEW, docs.SEMANTIC_VIEW_YAML)
)


def test_every_docs_page_is_cited() -> None:
    """A page nothing cites is a gap in the tables, or a page to remove."""
    cited = {
        evidence.page
        for table in (node for tables in TABLES.values() for node in tables)
        for _, _, evidence in walk(table)
        if isinstance(evidence, Documented)
    }
    assert sorted(page.title for page in set(docs.PAGES) - cited - _NOT_KEY_PAGES) == []
