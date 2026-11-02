"""Every authored file kind is declared once, and the tables the loaders check are derived from it."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.model.agent import KNOWN_AGENT_TOOL_TYPES
from snowflake_semantic_tools.domain.model.authored_schema import (
    AGENT,
    AGENT_TOOL,
    FILE_KINDS,
    SEMANTIC_KINDS,
    TOOL_MEMBER,
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
    node_patterns,
)
from snowflake_semantic_tools.domain.model.config_schema import block_settings
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.resolve.ref_fields import NESTED_AT, REFERENCE_FIELDS
from snowflake_semantic_tools.domain.validate.agent_tool import TOOL_KEYS
from snowflake_semantic_tools.domain.validate.semantic.authored_keys import AUTHORED_KEYS, NESTED_KEYS


def _nodes(node: Node) -> Iterator[Node]:
    yield node
    for item in node.all_fields():
        if item.nested is not None:
            yield from _nodes(item.nested)


def _all_fields() -> Iterator[tuple[FileKind, Node, Field]]:
    for kind in FILE_KINDS.values():
        for node in _nodes(kind.node):
            for item in node.all_fields():
                yield kind, node, item
            for fields in node.variants.values():
                for item in fields:
                    yield kind, node, item


def test_every_kind_is_registered_once_under_its_own_name() -> None:
    assert all(name == kind.name for name, kind in FILE_KINDS.items())
    assert [kind.name for kind in SEMANTIC_KINDS] == list(AUTHORED_KEYS)


@pytest.mark.parametrize("kind", SEMANTIC_KINDS, ids=lambda kind: kind.name)
def test_a_semantic_kind_takes_its_registry_roots(kind: FileKind) -> None:
    entry = ARTIFACT_REGISTRY.artifacts.get(kind.name) or ARTIFACT_REGISTRY.members[kind.name]
    assert kind.roots == (entry.singular_root, entry.root_key)


def test_tool_members_take_the_tool_artifact_roots() -> None:
    tool = ARTIFACT_REGISTRY.artifacts["tool"]
    assert TOOL_MEMBER.roots == (tool.singular_root, tool.root_key)


def test_every_node_declares_each_key_once() -> None:
    for kind in FILE_KINDS.values():
        for node in _nodes(kind.node):
            names = [item.name for item in node.fields]
            assert len(names) == len(set(names)), (kind.name, names)
            for value, fields in node.variants.items():
                own = [item.name for item in fields]
                assert len(own) == len(set(own)) and not set(own) & set(names), (kind.name, value)


def test_every_field_describes_itself_and_its_value() -> None:
    for kind, _, item in _all_fields():
        where = (kind.name, item.name)
        assert item.summary.strip() and item.summary.endswith("."), where
        assert (item.kind is ValueKind.ENUM) == bool(item.choices), where
        assert (item.kind is ValueKind.LIST) == (item.items is not None), where
        assert item.node is None or item.kind is ValueKind.MAPPING, where
        assert item.config_block is None or item.name == "config", where


def test_every_field_not_read_as_written_names_what_replaced_it() -> None:
    for kind, node, item in _all_fields():
        where = (kind.name, item.name)
        if item.status in (FieldStatus.DEPRECATED, FieldStatus.RENAMED):
            assert item.replacement in node.read_keys(), where
        elif item.status is FieldStatus.REMOVED:
            assert item.replacement, where
        else:
            assert item.replacement is None, where
        assert item.code is None or item.code in ERROR_REGISTRY, where


def test_every_config_field_points_at_a_settings_block() -> None:
    blocks = [item.config_block for _, _, item in _all_fields() if item.config_block]
    assert blocks
    assert all(block_settings(block) for block in blocks)


def test_each_agent_tool_type_has_its_keys() -> None:
    assert set(AGENT_TOOL.variants) == KNOWN_AGENT_TOOL_TYPES
    assert set(TOOL_KEYS) == KNOWN_AGENT_TOOL_TYPES
    assert node_patterns(AGENT.node)[("spec", "tools", "*")] == frozenset().union(*TOOL_KEYS.values())


def test_the_nested_semantic_scopes_are_the_tables_lists_and_blocks() -> None:
    assert (("semantic_view", "config") in NESTED_KEYS) and NESTED_KEYS[("semantic_view", "config")] == frozenset()
    assert NESTED_KEYS[("metric.window", "order_by")] == frozenset(("ref", "sort_direction", "null_order"))


def _resolved(path: str) -> Field:
    """The field a dotted `REFERENCE_FIELDS` path names, a list's items being implied."""
    kind, *keys = path.split(".")
    node: Node | None = FILE_KINDS[kind].node
    found: Field | None = None
    for key in keys:
        assert node is not None, path
        found = next(item for item in node.all_fields() if item.name == key)
        node = found.nested
    assert found is not None, path
    return found


@pytest.mark.parametrize("path", sorted(REFERENCE_FIELDS))
def test_every_reference_field_is_a_declared_key(path: str) -> None:
    found = _resolved(NESTED_AT.get(path, path))
    takes = found.items if found.kind is ValueKind.LIST else (found.node or found.kind)
    # A block, such as an agent's instructions, whose keys take the calls, or a node a bare call stands for.
    assert takes is ValueKind.TEMPLATE or isinstance(takes, Node), path
