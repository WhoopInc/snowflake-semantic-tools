"""What an editor offers in a field is what SST will read there.

Each field's functions are the reference and inline tables', each closed set is the one SST
reports a value outside of, and the project's `snowflake:` block closes the lists it configures.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.agent import KNOWN_AGENT_TOOL_TYPES
from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS, Field, Node, ValueKind
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_SCHEMA, KeyKind, KeyStatus
from snowflake_semantic_tools.domain.model.registry import NON_ARTIFACT_FUNCTIONS, SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.render.field_vocabulary import (
    DATA_TYPE_FIELDS,
    FieldVocabulary,
    field_vocabularies,
    template_functions,
)
from snowflake_semantic_tools.domain.resolve.calls import KNOWN_FUNCTIONS, LEGACY_FUNCTIONS
from snowflake_semantic_tools.domain.resolve.ref_fields import (
    INLINE_FIELDS,
    REF_FIELDS,
    REF_POLICIES,
    REFERENCE_FIELDS_AT,
)
from snowflake_semantic_tools.domain.sql.datatype import SUGGESTED_TYPES, is_datatype


def _fields() -> dict[str, Field]:
    found: dict[str, Field] = {}

    def walk(node: Node, path: str) -> None:
        for item in node.all_fields():
            if item.read:
                found[f"{path}.{item.name}"] = item
                if item.nested is not None:
                    walk(item.nested, f"{path}.{item.name}")

    for name, kind in FILE_KINDS.items():
        walk(kind.node, name)
    return found


def _by_path(config: dict[str, object] | None = None) -> dict[str, FieldVocabulary]:
    return {item.path: item for item in field_vocabularies(config or {})}


def test_every_function_but_the_legacy_ones_is_offered_with_what_it_names() -> None:
    functions = {item.name: item for item in template_functions()}

    assert set(functions) == KNOWN_FUNCTIONS - LEGACY_FUNCTIONS
    types = {*SEMANTIC_REGISTRY.artifacts, *SEMANTIC_REGISTRY.members}
    for name, item in functions.items():
        assert (item.names in types) is (name not in NON_ARTIFACT_FUNCTIONS), name
    assert (functions["ref"].arity, functions["ref"].names) == ((1, 2), "model")
    assert (functions["custom_instructions"].names, functions["source"].arity) == ("custom_instruction", (2,))


@pytest.mark.parametrize("path", sorted(REFERENCE_FIELDS_AT))
def test_a_reference_field_offers_its_functions_with_the_suggested_one_first(path: str) -> None:
    offered = _by_path()[path]

    assert offered.functions[: len(REFERENCE_FIELDS_AT[path])] == REFERENCE_FIELDS_AT[path]


@pytest.mark.parametrize("path", sorted(INLINE_FIELDS))
def test_an_inline_field_is_template_text_offering_what_its_policy_allows(path: str) -> None:
    field = _fields()[path]
    offered = _by_path()[path]

    takes = field.items if field.kind is ValueKind.LIST else field.kind
    assert takes is ValueKind.TEMPLATE, path
    assert offered.inline and set(INLINE_FIELDS[path].allowed) <= set(offered.functions)


@pytest.mark.parametrize(
    ("inline", "named"),
    [
        ("metric.expr", "metric.expression"),
        ("metric.window.partition_by", "metric.window"),
        ("filter.expr", "filter.expression"),
        ("tool.where", "tool.where"),
        ("verified_query.sql", "verified_query.sql"),
    ],
)
def test_the_inline_table_binds_what_the_resolver_binds(inline: str, named: str) -> None:
    assert INLINE_FIELDS[inline] is REF_POLICIES[REF_FIELDS[named]]


def test_a_closed_enum_is_offered_closed_and_an_open_one_as_suggestions() -> None:
    offered = _by_path()

    for path, field in _fields().items():
        if field.kind is ValueKind.ENUM and path != "agent.spec.tools.type":
            assert offered[path].choices == field.choices, path
            assert offered[path].closed is not field.open_choices, path
            assert offered[path].folded is field.folded, path
    assert (offered["tool.language"].folded, offered["tool.language"].closed) == (True, True)


def test_a_data_type_field_suggests_types_and_takes_any_the_grammar_does() -> None:
    offered = _by_path()

    assert set(_fields()) >= DATA_TYPE_FIELDS
    assert all(offered[path].choices == SUGGESTED_TYPES and not offered[path].closed for path in DATA_TYPE_FIELDS)
    assert all(is_datatype(name) for name in SUGGESTED_TYPES)


def test_every_enum_setting_is_offered_closed_at_its_key_and_in_an_artifact_config() -> None:
    offered = _by_path()

    for key in CONFIG_SCHEMA:
        if key.kind is KeyKind.ENUM and key.status is KeyStatus.CURRENT:
            assert (offered[key.path].choices, offered[key.path].closed) == (key.choices, True), key.path
    assert offered["tool.config.execute_as"] == FieldVocabulary(
        "tool.config.execute_as", choices=offered["tools.+execute_as"].choices, closed=True, folded=True
    )
    assert offered["agent.config.tags.name"].functions == ("tag",)


def test_the_project_closes_the_tool_types_models_and_avatars_it_configures() -> None:
    snowflake = {
        "orchestration_models": ["auto", "claude-4-sonnet"],
        "tool_types": ["future_tool"],
        "profile": {"avatar_allowlist": ["robot", "bolt"]},
    }
    offered = _by_path({"snowflake": snowflake})

    tool_types = offered["agent.spec.tools.type"]
    assert tool_types.closed and set(tool_types.choices) == {*KNOWN_AGENT_TOOL_TYPES, "future_tool"}
    for path in ("agent.config.orchestration_model", "agents.+orchestration_model", "coco_profile.model"):
        assert (offered[path].choices, offered[path].closed) == (("auto", "claude-4-sonnet"), True), path
    assert offered["evals.+candidate_models"].choices == ("claude-4-sonnet",)
    assert (
        offered["agent.profile.avatar"].choices == offered["cowork_profile.display.avatar"].choices == ("bolt", "robot")
    )


def test_an_unconfigured_project_leaves_avatars_and_candidates_open_and_allows_auto() -> None:
    offered = _by_path()

    assert "agent.profile.avatar" not in offered and "evals.+candidate_models" not in offered
    assert offered["agents.+orchestration_model"].choices == ("auto",)
    assert offered["agent.spec.tools.type"].choices == tuple(sorted(KNOWN_AGENT_TOOL_TYPES))
