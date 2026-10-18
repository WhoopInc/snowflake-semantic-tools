"""An agent's two definitions: the specification SST renders, and the one the version it serves holds."""

from __future__ import annotations

import json

import pytest

from snowflake_semantic_tools.domain.plan.definitions.agent import spec_definition
from snowflake_semantic_tools.domain.plan.member_diff import ADDED, CHANGED, diff_definitions

_SPEC = {
    "models": {"orchestration": "auto"},
    "orchestration": {"budget": {"seconds": 60, "tokens": 16000}},
    "instructions": {
        "response": "Answer in one paragraph.",
        "orchestration": "Prefer the semantic views.",
        "sample_questions": [{"question": "What sold best?"}, {"question": "Which store is busiest?"}],
    },
    "tools": [
        {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "SALES", "description": "Sales."}},
        {"tool_spec": {"type": "cortex_search", "name": "menu_search", "description": "Menu."}},
    ],
    "tool_resources": {
        "SALES": {"semantic_view": "SALES_DB.SV.SALES", "execution_environment": {"type": "warehouse"}},
        "menu_search": {"search_service": "MENU_DB.TOOLS.MENU_SEARCH", "max_results": 5},
        "orphan": {"search_service": "X.Y.Z"},
    },
    "skills": [{"name": "jaffle-analytics", "source": {"stage": "@S/skills/a"}}, {"source": {}}],
}


def _text(spec: dict[str, object]) -> str:
    return json.dumps(spec)


def test_a_specification_and_one_with_its_keys_reordered_agree() -> None:
    reordered = json.dumps(dict(reversed(list(_SPEC.items()))), indent=2, sort_keys=True)
    assert diff_definitions(spec_definition(_text(_SPEC)), spec_definition(reordered)) == ()


def test_members_are_the_parts_an_author_edits_apart() -> None:
    labels = [f"{member.kind} {member.name}" for member in spec_definition(_text(_SPEC)).members]
    assert labels == [
        "setting models",
        "setting orchestration",
        "instruction response",
        "instruction orchestration",
        "instruction sample_questions",
        "tool SALES",
        "tool menu_search",
        "skill jaffle-analytics",
        "skill #2",
        "tool orphan",
    ]
    sales = next(member for member in spec_definition(_text(_SPEC)).members if member.name == "SALES")
    assert dict(sales.properties) == {
        "type": "cortex_analyst_text_to_sql",
        "description": "Sales.",
        "resource.semantic_view": "SALES_DB.SV.SALES",
        "resource.execution_environment": '{"type": "warehouse"}',
    }


def test_an_instruction_edit_and_a_new_sample_question_show_as_what_they_change() -> None:
    edited = json.loads(_text(_SPEC))
    edited["instructions"]["response"] = "Answer in two sentences."
    edited["instructions"]["sample_questions"].append({"question": "What is new?"})
    edited["tools"].append({"tool_spec": {"type": "generic", "name": "order_status"}})
    diffs = diff_definitions(spec_definition(_text(_SPEC)), spec_definition(_text(edited)))
    assert [(item.op, item.label, [p.name for p in item.properties]) for item in diffs] == [
        (CHANGED, "instruction response", ["text"]),
        (CHANGED, "instruction sample_questions", ["questions"]),
        (ADDED, "tool order_status", ["type"]),
    ]


def test_a_shape_the_spec_does_not_expect_is_still_compared() -> None:
    odd = {"instructions": {"sample_questions": ["plain"]}, "tools": ["bare", "bare"], "skills": ["s"], "x": [1]}
    labels = [f"{member.kind} {member.name}" for member in spec_definition(json.dumps(odd)).members]
    assert labels == ["instruction sample_questions", "tool #1", "tool #2", "skill #1", "setting x"]
    twice = {"tools": [{"tool_spec": {"name": "a"}}, {"tool_spec": {"name": "A"}}]}
    assert [member.name for member in spec_definition(json.dumps(twice)).members] == ["a", "A#2"]
    assert spec_definition(json.dumps({"tool_resources": []})).members == ()


def test_a_specification_that_is_not_an_object_is_refused() -> None:
    with pytest.raises(ValueError, match="is a JSON object"):
        spec_definition("[]")
