"""Scoring one reply as an unpublished eval run does: exact tool selection, and an LLM judge's prompt and score."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from snowflake_semantic_tools.domain.evaluate.judge import (
    ANSWER_CORRECTNESS_SCALE,
    JudgedReply,
    answer_correctness_prompt,
    custom_judge_prompt,
    fill_judge_prompt,
    judged_reason,
    judged_score,
    unfillable_placeholders,
)
from snowflake_semantic_tools.domain.evaluate.scores import average_text, score_text, threshold_phrase
from snowflake_semantic_tools.domain.evaluate.tool_selection import (
    counted_calls,
    declared_tools,
    expected_tools,
    selected_tools,
    service_tools,
    tool_selection_score,
)
from snowflake_semantic_tools.domain.model.eval.model import ThresholdRange

SPEC = {
    "tools": [
        {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "SALES"}},
        {"tool_spec": {"type": "cortex_search", "name": "docs"}},
        {"tool_spec": {"type": "data_to_chart", "name": "chart"}},
        {"tool_spec": {"name": "untyped"}},
        {"tool_spec": {"type": "generic"}},
        {"tool_spec": "not a spec"},
        "not an entry",
    ]
}
DECLARED = declared_tools(SPEC)


def test_the_specification_declares_each_named_tool_with_its_type() -> None:
    assert DECLARED == {
        "SALES": "cortex_analyst_text_to_sql",
        "docs": "cortex_search",
        "chart": "data_to_chart",
        "untyped": "",
    }
    assert declared_tools({"tools": "none"}) == {} and declared_tools([]) == {}


def test_expected_tools_read_the_invocations_and_tell_none_from_no_expectation() -> None:
    invocations = [{"tool_name": "SALES"}, {"tool_input": "no name"}, "x", {"tool_name": "docs"}]
    assert expected_tools({"ground_truth_invocations": invocations}) == ("SALES", "docs")
    assert expected_tools({"ground_truth_invocations": []}) == ()
    assert expected_tools({"ground_truth_output": "x"}) is None
    assert expected_tools("not a mapping") is None


def test_only_declared_non_chart_calls_count_each_time_and_without_case() -> None:
    called = ["SALES", "server_skill", "system_execute_sql", "chart", "SALES", "sales"]
    assert selected_tools(called, DECLARED) == Counter({"sales": 3})


@pytest.mark.parametrize(
    ("called", "expected", "score"),
    [
        (["SALES"], ["SALES"], 1.0),
        # Snowflake's own system tools are never declared, so they never count.
        (["server_skill", "SALES", "system_execute_sql"], ["SALES"], 1.0),
        # An extra chart is tolerated, as the evaluation service tolerates it.
        (["SALES", "chart"], ["SALES"], 1.0),
        (["SALES"], ["SALES", "chart"], 1.0),
        (["docs", "SALES"], ["SALES", "docs"], 1.0),
        (["sales"], ["SALES"], 1.0),
        # The measured case: expected once, called twice.
        (["SALES", "SALES"], ["SALES"], 0.5),
        (["SALES"], ["SALES", "docs"], 0.5),
        (["SALES", "docs"], ["SALES"], 0.5),
        ([], [], 1.0),
        (["server_skill"], [], 1.0),
        (["SALES"], [], 0.0),
        ([], ["SALES"], 0.0),
        (["SALES"], ["MISSING"], 0.0),
        (["SALES"], None, None),
    ],
)
def test_tool_selection_scores_matched_over_matched_missing_and_extra(
    called: list[str], expected: list[str] | None, score: float | None
) -> None:
    assert tool_selection_score(called, DECLARED, expected) == score


REPLY = JudgedReply(
    "How many orders?",
    "12 orders.",
    {"ground_truth_output": "Reports a count of orders."},
    '[{"name": "SALES"}]',
    1500,
)


def test_a_reply_fills_each_placeholder_it_has_a_value_for() -> None:
    values = REPLY.values()
    assert values["ground_truth"] == json.dumps({"ground_truth_output": "Reports a count of orders."})
    assert (values["duration"], values["error"], values["status"]) == ("1500", "", "success")
    failed = JudgedReply("q", "", error="boom")
    assert (failed.values()["error"], failed.values()["status"]) == ("boom", "error")
    prompt = "Q {{input}} A {{ output }} T {{ tool_info }} S {{ span_id }}"
    assert fill_judge_prompt(prompt, REPLY) == 'Q How many orders? A 12 orders. T [{"name": "SALES"}] S {{ span_id }}'


def test_the_placeholders_a_reply_cannot_fill_are_named_once() -> None:
    prompt = "{{ span_id }} {{ llm_model }} {{ span_id }} {{ start_timestamp }} {{ input }}"
    assert unfillable_placeholders(prompt) == ("llm_model", "span_id", "start_timestamp")
    assert unfillable_placeholders("Score {{ output }}.") == ()


def test_a_custom_prompt_is_followed_by_how_to_answer_on_its_scale() -> None:
    prompt = custom_judge_prompt("Score {{ output }}.", REPLY, 0, 5)
    assert prompt.startswith("Score 12 orders..\n\n")
    assert "Question:" not in prompt
    assert prompt.endswith('"score", a number from 0 to 5, and "reason", one sentence saying why.')


def test_a_custom_prompt_with_no_question_or_answer_placeholder_is_given_both() -> None:
    prompt = custom_judge_prompt("You will be given the question and the answer.", REPLY, 0, 5)
    assert prompt.startswith(
        "You will be given the question and the answer.\n\nQuestion:\nHow many orders?\n\nAnswer:\n12 orders.\n\n"
    )
    assert "Question:" not in custom_judge_prompt("Was {{ input }} answered?", REPLY, 0, 5)


def test_answer_correctness_is_judged_against_the_ground_truth_output_alone() -> None:
    prompt = answer_correctness_prompt(REPLY)
    assert prompt is not None
    assert "Reference:\nReports a count of orders." in prompt and "Answer:\n12 orders." in prompt
    assert "a number from 0 to 1" in prompt
    assert ANSWER_CORRECTNESS_SCALE == (0.0, 1.0)
    assert answer_correctness_prompt(JudgedReply("q", "a", {"ground_truth_output": "  "})) is None
    assert answer_correctness_prompt(JudgedReply("q", "a")) is None


@pytest.mark.parametrize(
    ("answer", "score"),
    [
        ({"score": 0.75, "reason": "partly"}, 0.75),
        ('{"score": 4, "reason": "ok"}', 4.0),
        ({"score": 0}, 0.0),
        ({"score": 5.5}, None),
        ({"score": -1}, None),
        ({"score": True}, None),
        ({"score": "4"}, None),
        ({"score": float("nan")}, None),
        ({"reason": "no score"}, None),
        ("not json", None),
        ([4], None),
        (None, None),
    ],
)
def test_a_judges_score_is_read_only_when_it_is_on_the_scale(answer: object, score: float | None) -> None:
    assert judged_score(answer, 0, 5) == score


def test_the_counted_calls_keep_their_order_and_spelling_and_drop_system_and_chart_tools() -> None:
    called = ["sales", "server_skill", "chart", "docs", "SALES"]

    assert counted_calls(called, DECLARED) == ("sales", "docs", "SALES")


@pytest.mark.parametrize(
    ("criteria", "side", "tools"),
    [
        (
            "**Expected:** `[menu_search]` (1 tools)\n\n**Invoked:** `[menu_search, menu_search]` (2 tools)",
            "Invoked",
            ("menu_search", "menu_search"),
        ),
        ("**Expected:** `[menu_search]` (1 tools)\n\n**Invoked:** `[]` (0 tools)", "Expected", ("menu_search",)),
        ("**Expected:** `[]` (0 tools)\n\n**Invoked:** `[]` (0 tools)", "Invoked", ()),
        ("**Expected:** `[menu_search]` (1 tools)", "Invoked", None),
        ("a judge's prose criteria", "Invoked", None),
        (None, "Invoked", None),
    ],
)
def test_the_services_criteria_name_each_side_as_a_list(
    criteria: object, side: str, tools: tuple[str, ...] | None
) -> None:
    assert service_tools(criteria, side) == tools


@pytest.mark.parametrize(
    ("threshold", "phrase"),
    [
        (ThresholdRange(min=0.8), "needs at least 0.8"),
        (ThresholdRange(max=0.25), "needs at most 0.25"),
        (ThresholdRange(min=0.5, max=1.0), "needs 0.5 to 1"),
        (ThresholdRange(), None),
        (None, None),
    ],
)
def test_a_threshold_reads_as_the_score_it_needs(threshold: ThresholdRange | None, phrase: str | None) -> None:
    assert threshold_phrase(threshold) == phrase


def test_a_score_has_two_places_and_an_average_three() -> None:
    assert (score_text(0.5), score_text(1), score_text(None)) == ("0.50", "1.00", "no score")
    assert (average_text(0.9285714285714286), average_text(1), average_text(None), average_text(True)) == (
        "0.929",
        "1.000",
        "no score",
        "no score",
    )


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        ({"score": 0.5, "reason": " Half right. "}, "Half right."),
        ('{"score": 1, "reason": "ok"}', "ok"),
        ({"score": 1, "reason": "  "}, None),
        ({"score": 1, "reason": 3}, None),
        ({"score": 1}, None),
        ("not json", None),
        ([1], None),
    ],
)
def test_a_judges_reason_is_read_stripped_when_it_gave_one(answer: object, reason: str | None) -> None:
    assert judged_reason(answer) == reason
