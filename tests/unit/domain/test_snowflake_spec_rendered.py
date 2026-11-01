"""What SST renders, and what the importer reads back, stays inside the spec tables.

The agent and CoWork profile goldens are the renderer's bytes for the reference project
(`test_golden_agents.py` compares them), so each key path they hold must be one the
tables declare: a key SST writes that the table leaves out is a gap in the contract.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

from snowflake_semantic_tools.domain import snowflake_docs
from snowflake_semantic_tools.domain.importing import agent as importer
from snowflake_semantic_tools.domain.model.snowflake_spec import (
    AGENT_SPEC,
    COWORK_SPEC,
    ROUTER_NAME,
    ROUTER_RESOURCES,
    TOOL_RESOURCES,
    SpecNode,
    walk,
)
from snowflake_semantic_tools.domain.render.spec_reference import render_snowflake_spec

GOLDEN = Path(__file__).resolve().parents[2] / "golden" / "expected"


def _undeclared(node: SpecNode, value: Mapping[str, object], path: str) -> Iterator[str]:
    """Yield each dotted path below `value` that `node` does not declare; resources by tool type."""
    for key, item in value.items():
        where = f"{path}{key}"
        declared = node.key(key)
        if declared is None:
            yield where
        elif isinstance(declared.items, SpecNode) and isinstance(item, list):
            for entry in item:
                if isinstance(entry, Mapping):
                    yield from _undeclared(declared.items, entry, f"{where}.*.")
        elif declared.node is not None and not declared.named and isinstance(item, Mapping):
            yield from _undeclared(declared.node, item, f"{where}.")


def _resources(spec: Mapping[str, object]) -> Iterator[str]:
    """Yield each undeclared path of each tool's resources, read against its type's table."""
    types = {entry["tool_spec"]["name"]: entry["tool_spec"]["type"] for entry in spec.get("tools", [])}  # type: ignore[attr-defined]
    for name, resource in dict(spec.get("tool_resources", {})).items():  # type: ignore[call-overload]
        table = ROUTER_RESOURCES if name == ROUTER_NAME else TOOL_RESOURCES[types[name]]
        yield from _undeclared(table, resource, f"tool_resources.{name}.")


@pytest.mark.parametrize(
    ("golden", "table"),
    [
        *[(path, AGENT_SPEC) for path in sorted((GOLDEN / "agent").glob("*.json"))],
        *[(path, COWORK_SPEC) for path in sorted((GOLDEN / "cowork_profile").glob("*.json"))],
    ],
    ids=lambda value: value.stem if isinstance(value, Path) else "",
)
def test_the_renderer_writes_only_paths_the_tables_declare(golden: Path, table: SpecNode) -> None:
    spec = json.loads(golden.read_text(encoding="utf-8"))
    assert [*_undeclared(table, spec, ""), *_resources(spec)] == []


def test_the_goldens_cover_every_tool_type_the_tables_declare() -> None:
    """So the check above reads every resource table, not only those a golden happens to use."""
    used = {
        entry["tool_spec"]["type"]
        for path in (GOLDEN / "agent").glob("*.json")
        for entry in json.loads(path.read_text(encoding="utf-8")).get("tools", [])
    }
    assert sorted(set(TOOL_RESOURCES) - used) == []


def test_the_importer_splits_a_search_tools_resources_as_the_table_declares_them() -> None:
    """A search tool's own keys and its member's are kept apart; together they are the table's, less the service."""
    own, member = set(importer._SEARCH_KEYS), set(importer._MEMBER_KEYS)
    assert not own & member
    declared = TOOL_RESOURCES["cortex_search"].names - {"search_service", "execution_environment"}
    assert own | member == declared


def test_the_spec_reference_page_lists_every_key_and_every_page_cited() -> None:
    page = render_snowflake_spec(generated="")
    for table in (AGENT_SPEC, COWORK_SPEC, *TOOL_RESOURCES.values()):
        for path, _, _ in walk(table):
            # A list's entries are `[]` and a named mapping's `<name>` on the page, `*` in a path.
            assert any(_spelled(path, spelling) in page for spelling in ("[]", ".<name>")), path
    for doc_page in snowflake_docs.PAGES:
        assert doc_page.url in page, doc_page.title


def _spelled(path: tuple[str, ...], spelling: str) -> str:
    return "`" + ".".join(path).replace(".*", spelling) + "`"
