"""How `sst eval` lays out eval results: each attempt's tree of metrics, cost and questions."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.cli.render.evals import EvalMarks, EvalShown, eval_lines
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import Terminal
from snowflake_semantic_tools.cli.render.tree import ASCII_TREE, TreeNode, connectors, tree_lines, tree_prefixes
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
    ThresholdRange,
)

NEVER = ("completion_tokens", "llm_call_count", "prompt_tokens", "total_tokens")
APPROXIMATE = EvalMarks(approximate=frozenset({"answer_correctness"}), unscored=frozenset({"logical_consistency"}))


def _row(
    index: int,
    tool_score: float,
    *,
    called: tuple[str, ...] | None = ("menu_search",),
    judged: float = 1.0,
    reason: str | None = "Names the veggie jaffle.",
    error: str | None = None,
) -> EvalResultRow:
    key = f"q{index}"
    metrics = (
        EvalMetricResult(key, "answer_correctness", judged, judged >= 0.5, reason),
        EvalMetricResult(key, "logical_consistency", None, False),
        EvalMetricResult(
            key, "tool_selection_accuracy", tool_score, tool_score >= 0.8, "**Score:**", ThresholdRange(0.8)
        ),
    )
    cost = EvalCostSummary(4100, total_input_tokens=38862, total_output_tokens=715, unmeasured=NEVER)
    return EvalResultRow(
        key,
        f"Question {index}?",
        metrics,
        index=index,
        answer="The veggie jaffle.",
        error=error,
        expected_tools=("menu_search",),
        called_tools=called,
        cost=cost,
    )


def _attempt(*rows: EvalResultRow, **fields: object) -> EvalRunAttempt:
    cost = EvalCostSummary(102600, total_input_tokens=250640, total_output_tokens=1651, unmeasured=NEVER)
    values: dict[str, object] = {"rows": rows, "cost": cost, "wall_ms": 37200, **fields}
    return EvalRunAttempt("unpublished", 1, "COMPLETED", **values)  # type: ignore[arg-type]


def _lines(*attempts: EvalRunAttempt, width: int = 100, unicode: bool = True, **options: object) -> list[str]:
    style = Style(Terminal(width=width, unicode=unicode))
    return eval_lines([EvalShown("eval:concierge", attempts)], style, **options)  # type: ignore[arg-type]


def test_by_default_only_the_questions_that_fell_short_are_shown_with_their_tools() -> None:
    lines = _lines(_attempt(_row(1, 1.0), _row(6, 0.5, called=("menu_search", "menu_search"))), marks=APPROXIMATE)
    assert lines == [
        "eval:concierge  unpublished  COMPLETED  1/2 questions passed",
        "├── answer_correctness       2/2 passed  average 1.000  approximate, never gates",
        "├── logical_consistency      not scored by this run",
        "├── tool_selection_accuracy  1/2 passed  average 0.750  needs at least 0.8",
        "├── cost  37.2s wall · 102.6s agent time · agent 250,640 in / 1,651 out",
        "│         not measured: LLM calls, judge tokens",
        "└── 1 of 2 questions fell short  (-v lists every question)",
        "    └── ✗ 6  Question 6?",
        "        ├── answer_correctness       1.00",
        "        └── tool_selection_accuracy  0.50  needs at least 0.8",
        "            ├── expected  menu_search",
        "            └── called    menu_search, menu_search",
    ]


def test_verbose_shows_every_question_with_its_cost_reasons_and_answer_cut_to_two_lines() -> None:
    long = "word " * 80
    row = _row(1, 1.0, reason=long)
    lines = _lines(_attempt(row), width=80, verbose=True, marks=APPROXIMATE)
    assert lines[6:8] == ["└── questions  1 of 1 passed", "    └── ✓ 1  Question 1?  4.1s · 38,862 in / 715 out"]
    reason = lines.index("        ├── answer_correctness       1.00") + 1
    assert lines[reason].startswith("        │   └── reason  word word")
    assert lines[reason + 1].startswith("        │               word") and lines[reason + 1].endswith("…")
    assert lines[reason + 2 :] == [
        "        ├── tool_selection_accuracy  1.00",
        "        │   ├── expected  menu_search",
        "        │   └── called    menu_search",
        "        └── answer  The veggie jaffle.",
    ]
    assert max(map(len, lines)) <= 80


def test_a_question_that_missed_only_an_approximate_metric_did_not_fall_short() -> None:
    lines = _lines(_attempt(_row(1, 1.0, judged=0.0), _row(2, 1.0)), marks=APPROXIMATE)
    assert lines[0].endswith("2/2 questions passed")
    assert lines[1] == "├── answer_correctness       1/2 passed  average 0.500  approximate, never gates"
    assert lines[-1] == "└── all 2 questions passed  (-v lists them)"
    # Without the mark, the judged miss fails the question, and its reason says why.
    unmarked = _lines(_attempt(_row(1, 1.0, judged=0.0)))
    assert unmarked[-4:] == [
        "        ├── answer_correctness       0.00",
        "        │   └── reason  Names the veggie jaffle.",
        "        ├── logical_consistency      no score",
        "        └── tool_selection_accuracy  1.00",
    ]


def test_an_error_a_regression_and_unparsed_tools_are_shown_on_their_question() -> None:
    errored = _row(1, 0.0, called=None, error="the agent is down")
    regressed = _row(2, 0.5, called=None, reason=None)
    marks = EvalMarks(regressions=frozenset({("q2", "tool_selection_accuracy")}))
    lines = _lines(_attempt(errored, regressed), marks=marks)
    text = "\n".join(lines)
    assert "├── tool_selection_accuracy  0/2 passed  average 0.250  needs at least 0.8  1 regressed" in text
    assert "    ├── ✗ 1  Question 1?\n    │   ├── error  the agent is down\n" in text
    # With the called tools unknown, the service's explanation stands in for them.
    assert (
        "        └── tool_selection_accuracy  0.50  needs at least 0.8  regressed\n            └── reason  **Score:**"
        in text
    )


def test_quiet_shows_metrics_and_cost_and_plain_draws_no_connectors_and_cuts_nothing() -> None:
    attempt = _attempt(_row(6, 0.5, called=("menu_search", "menu_search"), reason="x " * 200))
    quiet = _lines(attempt, summary_only=True, marks=APPROXIMATE)
    assert quiet[-2:] == [
        "└── cost  37.2s wall · 102.6s agent time · agent 250,640 in / 1,651 out",
        "          not measured: LLM calls, judge tokens",
    ]
    plain = _lines(attempt, plain=True, unicode=False, verbose=True, marks=APPROXIMATE)
    assert (
        "  cost  37.2s wall - 102.6s agent time - agent 250,640 in / 1,651 out - not measured: LLM calls, judge tokens"
        in plain
    )
    assert "    x 6  Question 6?  4.1s - 38,862 in / 715 out" in plain
    assert any(line.startswith("        reason  x x") and line.endswith("x") for line in plain)
    assert not any(symbol in line for line in plain for symbol in ("├", "|--", "`--"))


def test_a_published_attempt_counts_every_cost_and_several_attempts_are_numbered() -> None:
    measured = EvalCostSummary(43600, 3000, 1846, 4846, 250640, 1651, 13)
    failed = EvalRunAttempt(
        "RUN", 1, "FAILED", (), measured, retrieval_error="no rows", status_details=("partial", "timeout")
    )
    marks = EvalMarks(unscored=frozenset({"logical_consistency"}))
    lines = _lines(failed, EvalRunAttempt("RUN_R2", 2, "COMPLETED", (_row(1, 1.0),), measured), marks=marks)
    assert lines[:5] == [
        "eval:concierge  attempt 1  FAILED",
        "├── run  RUN",
        "├── cost  43.6s agent time · agent 250,640 in / 1,651 out · 13 LLM calls · judge 4,846 tokens",
        "├── status details  partial; timeout",
        "└── retrieval error  no rows",
    ]
    assert lines[5:7] == ["eval:concierge  attempt 2  COMPLETED  1/1 question passed", "├── run  RUN_R2"]
    assert lines[-1] == "└── the question passed  (-v shows it)"
    one_call = EvalRunAttempt("R", 1, "COMPLETED", (), EvalCostSummary(llm_call_count=1))
    assert "1 LLM call ·" in _lines(one_call)[2]


def test_an_eval_with_no_attempt_is_named_alone() -> None:
    assert eval_lines([EvalShown("eval:concierge", ())], Style(Terminal())) == ["eval:concierge"]


@pytest.mark.parametrize("unicode", [True, False])
def test_the_tree_continues_a_rows_text_under_it_and_down_to_its_children(unicode: bool) -> None:
    drawn = connectors(unicode)
    nodes = [TreeNode("a", ("  more",), (TreeNode("b"),)), TreeNode("c", ("more",))]
    lines = [prefix + text for prefix, text in tree_lines(nodes, drawn)]
    if unicode:
        assert lines == ["├── a", "│   │ more", "│   └── b", "└── c", "    more"]
    else:
        assert lines == ["|-- a", "|   | more", "|   `-- b", "`-- c", "    more"]
    assert tree_prefixes([(1, False), (2, True), (1, True)], ASCII_TREE, "> ") == ["> |-- ", "> |   `-- ", "> `-- "]


def _routed_row(
    index: int, score: float | None, expected: tuple[str, ...] | None, routed: tuple[str, ...] | None
) -> EvalResultRow:
    """A CoWork profile's question (a Snowflake private preview), scored on its routing."""
    key = f"q{index}"
    metrics = (
        EvalMetricResult(key, "answer_correctness", 1.0, True),
        EvalMetricResult(key, "routing_accuracy", score, score == 1.0, "why", ThresholdRange(0.9)),
    )
    return EvalResultRow(
        key, f"Question {index}?", metrics, index=index, expected_subagents=expected, routed_subagents=routed
    )


def test_a_profile_question_shows_the_subagents_it_accepts_and_the_router_chose() -> None:
    sales, partner = "DB.AGENTS.SALES", "PARTNER.AGENTS.DELIVERY"
    rows = (
        _routed_row(1, 1.0, (sales,), (sales,)),
        _routed_row(2, 0.0, (), (partner,)),
        _routed_row(3, 0.0, (sales, partner), ()),
        _routed_row(4, None, (sales,), None),
    )
    lines = _lines(_attempt(*rows))
    assert lines[5:] == [
        "└── 3 of 4 questions fell short  (-v lists every question)",
        "    ├── ✗ 2  Question 2?",
        "    │   ├── answer_correctness  1.00",
        "    │   └── routing_accuracy    0.00  needs at least 0.9",
        "    │       ├── expected  none (a decline)",
        "    │       └── routed    PARTNER.AGENTS.DELIVERY",
        "    ├── ✗ 3  Question 3?",
        "    │   ├── answer_correctness  1.00",
        "    │   └── routing_accuracy    0.00  needs at least 0.9",
        "    │       ├── expected  DB.AGENTS.SALES, PARTNER.AGENTS.DELIVERY",
        "    │       └── routed    none",
        "    └── ✗ 4  Question 4?",
        "        ├── answer_correctness  1.00",
        "        └── routing_accuracy    no score  needs at least 0.9",
        "            ├── expected  DB.AGENTS.SALES",
        "            └── routed    not known",
    ]


def test_a_profile_question_with_no_subagents_read_shows_its_reason() -> None:
    lines = _lines(_attempt(_routed_row(1, None, None, None)))
    assert lines[-2:] == ["        └── routing_accuracy    no score  needs at least 0.9", "            └── reason  why"]
