"""Per-target values read as structure, written back, and resolved exactly as a run resolves them."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.resolve.config import render_config
from snowflake_semantic_tools.domain.resolve.target_value import (
    Arm,
    ArmTest,
    Resolved,
    TargetValue,
    ValueForm,
    format_target_value,
    parse_target_value,
    resolve_for,
    target_groups,
    target_list,
)

_VARS: dict[str, object] = {
    "production_targets": ["prod"],
    "dev_targets": ["scratch", "studio"],
    "db": "ANALYTICS",
    "empty": "",
    "nested": {"a": 1},
    "flag": True,
}
_TARGETS = ("dev", "prod", "scratch", "studio")
# Every form the grammar reads, and some it does not.
_VALUES = (
    "SEMANTIC_VIEWS",
    "{{ target.schema }}",
    "{{ var('db') }}",
    "{{ var('flag') }}",
    "PRE_{{ var('db') }}",
    "{{ 'A' if target.name == 'prod' else 'B' }}",
    "{{ 'A' if target.name != 'prod' else 'B' }}",
    "{{ 'A' if target.name in ('prod',) else 'B' }}",
    "{{ 'A' if target.name not in ['prod', 'dev'] else 'B' }}",
    "{{ '' if target.name in var('dev_targets') else 'STATE' }}",
    "{{ 'P' if target.name in var('production_targets') else 'D' if target.name == 'dev' }}",
    "{{ 'P' if target.name in var('db') else 'D' }}",
    "{{ 'A' if target.name == 'prod' }}",
    "{{ 'A' if target.name == 'prod' else B }}",
    "x {{ 'A' if target.name == 'prod' else 'B' }}",
    "{{ var('missing') }}",
    "{{ var('empty') }}",
    "{{ var('nested') }}",
)


@pytest.mark.parametrize("text", _VALUES)
@pytest.mark.parametrize("target", _TARGETS)
def test_a_value_resolves_for_each_target_as_a_run_for_it_resolves_it(text: str, target: str) -> None:
    rendered, diagnostics = render_config({"agents": {"+schema": text}, "vars": _VARS}, target_name=target, file="f")
    resolved = resolve_for(text, target, _VARS)
    if diagnostics:
        assert resolved.value is None and resolved.problem
    else:
        assert resolved.value == str(rendered["agents"]["+schema"])


def test_a_value_reads_as_its_form() -> None:
    assert parse_target_value("SALES").form is ValueForm.PLAIN
    assert parse_target_value(" {{ var('db') }} ") == TargetValue(ValueForm.VAR, " {{ var('db') }} ", variable="db")
    assert parse_target_value("x {{ 'A' if target.name == 'p' else 'B' }}").form is ValueForm.UNREADABLE
    assert parse_target_value("{{ 'A' if target.name == 'p' else B }}").form is ValueForm.UNREADABLE
    assert parse_target_value(
        "{{ 'P' if target.name in var('production_targets') else 'D' if target.name == 'dev' }}"
    ) == (
        TargetValue(
            ValueForm.CONDITIONAL,
            "{{ 'P' if target.name in var('production_targets') else 'D' if target.name == 'dev' }}",
            arms=(
                Arm("P", ArmTest("in", variable="production_targets")),
                Arm("D", ArmTest("==", names=("dev",))),
            ),
        )
    )
    listed = parse_target_value("{{ 'A' if target.name not  in ['a', 'b'] else 'B' }}")
    assert listed.arms[0].test == ArmTest("not in", names=("a", "b"), bracket="[")
    assert listed.otherwise == "B"


@pytest.mark.parametrize("text", [value for value in _VALUES if "if" in value or "var" in value])
def test_writing_back_what_was_read_reads_the_same(text: str) -> None:
    value = parse_target_value(text)
    assert parse_target_value(format_target_value(value)).arms == value.arms


def test_a_value_is_written_in_the_grammar_the_resolver_reads() -> None:
    value = TargetValue(
        ValueForm.CONDITIONAL,
        "",
        arms=(
            Arm("A", ArmTest("in", names=("prod",), bracket="(")),
            Arm("B", ArmTest("not in", names=("x", "y"), bracket="[")),
            Arm("C", ArmTest("!=", names=("dev",))),
        ),
        otherwise="D",
    )
    assert format_target_value(value) == (
        "{{ 'A' if target.name in ('prod',) else 'B' if target.name not in ['x', 'y'] "
        "else 'C' if target.name != 'dev' else 'D' }}"
    )
    assert format_target_value(TargetValue(ValueForm.VAR, "", variable="db")) == "{{ var('db') }}"
    assert format_target_value(TargetValue(ValueForm.PLAIN, "SALES")) == "SALES"


def test_targets_group_by_what_they_get_named_by_the_list_that_names_them() -> None:
    lists = {"production_targets": ("prod",), "dev_targets": ("scratch", "studio")}
    groups = target_groups("{{ '' if target.name in var('dev_targets') else 'STATE' }}", _TARGETS, lists, _VARS)
    assert [(group.label, group.targets, group.resolved) for group in groups] == [
        ("dev, prod", ("dev", "prod"), Resolved("STATE", chosen=True)),
        ("dev_targets", ("scratch", "studio"), Resolved("", chosen=True)),
    ]


def test_a_list_of_targets_is_a_non_empty_list_of_names() -> None:
    assert target_list("dev_targets", _VARS) == ("scratch", "studio")
    assert target_list("db", _VARS) is None
    assert target_list("missing", _VARS) is None
