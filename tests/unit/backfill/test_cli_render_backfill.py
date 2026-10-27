"""Human renders at the edges the broader suites skip: excerpts, evals, comparisons, agent text, progress."""

from __future__ import annotations

import io
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.timings import RunTimings
from snowflake_semantic_tools.cli.render import progress
from snowflake_semantic_tools.cli.render.agent import agent_document
from snowflake_semantic_tools.cli.render.compare import comparison_lines
from snowflake_semantic_tools.cli.render.evals import EvalShown, eval_lines
from snowflake_semantic_tools.cli.render.frames import excerpt
from snowflake_semantic_tools.cli.render.progress import ProgressDisplay
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import PLAIN, Terminal
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.evaluate.compare import PINNED
from snowflake_semantic_tools.domain.model.eval import (
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
)
from tests.helpers.compare_reports import compared_report, model, run


def test_an_origin_in_a_directory_has_no_excerpt(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    assert excerpt(tmp_path, Origin("models", 1, 1), 80) is None


def test_a_wide_line_with_its_caret_near_the_end_is_cut_at_its_start_only(tmp_path: Path) -> None:
    (tmp_path / "views.yml").write_text("x" * 95 + " target\n", encoding="utf-8")
    found = excerpt(tmp_path, Origin("views.yml", 1, 97), 80)
    assert found is not None
    assert found.text.startswith("...") and found.text.endswith("target")
    assert found.text[found.start : found.start + found.length] == "target"


def _eval(*metrics: EvalMetricResult, **row: object) -> list[str]:
    values: dict[str, object] = {"index": 1, **row}
    shown = EvalResultRow("q1", "Which tools?", metrics, **values)  # type: ignore[arg-type]
    attempt = EvalRunAttempt("run", 1, "COMPLETED", (shown,), EvalCostSummary(1000))
    return eval_lines([EvalShown("eval:concierge", (attempt,))], Style(Terminal(width=100)), verbose=True)


def test_a_tool_metric_with_no_expected_tools_says_there_is_no_expectation() -> None:
    lines = _eval(
        EvalMetricResult("q1", "tool_selection_accuracy", 0.0, False),
        called_tools=("menu_search",),
        expected_tools=None,
    )
    assert any("expected  no expectation" in line for line in lines), lines
    assert any("called    menu_search" in line for line in lines), lines


def test_a_question_whose_tokens_were_not_measured_shows_its_time_alone() -> None:
    cost = EvalCostSummary(2500, unmeasured=("total_input_tokens", "total_output_tokens"))
    lines = _eval(EvalMetricResult("q1", "answer_correctness", 1.0, True), cost=cost)
    [question] = [line for line in lines if "Which tools?" in line]
    assert "2.5s" in question and " in / " not in question


def test_a_comparison_of_alike_models_lists_no_differing_questions_pin_or_files() -> None:
    report = compared_report(
        model("pinned", PINNED, run("p1", 1.0, 1.0)), model("opus", "candidate", run("o1", 1.0, 1.0))
    )
    [evaluation] = report.evals
    assert not evaluation.verdict.pin_suggestion
    lines = comparison_lines(report, Style(PLAIN))
    assert not [
        line for line in lines if "questions that differ" in line or line.lstrip().startswith(("pin", "report"))
    ]


def test_a_comparison_with_no_metrics_has_no_headline_column() -> None:
    report = compared_report()
    bare = replace(report, evals=tuple(replace(item, metrics=()) for item in report.evals))
    header = comparison_lines(bare, Style(PLAIN))[1]
    assert header.split() == ["#", "model", "role", "credits/q", "p95", "version"]


def test_an_agent_finding_without_a_suggestion_has_no_fix_line(tmp_path: Path) -> None:
    finding = D("SST-MEM107", member="m", count=1, subject="semantic_view:v")
    assert finding.suggestion is None
    document = agent_document(
        "validate",
        1,
        DiagnosticBag((replace(finding, severity=D("SST-CFG001", path="p").severity),)),
        baselined=frozenset(),
        fingerprint=lambda item: item.code,
        project_dir=tmp_path,
    )
    assert "SST-MEM107" in document and "fix:" not in document


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _wait_for(condition: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 5
    while not condition():
        assert time.monotonic() < deadline, "the progress loop did not run"
        time.sleep(0.005)


def test_plain_progress_beats_only_once_due_and_only_while_something_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(progress, "INTERVAL_SECONDS", 0.005)
    stream, clock, timings = io.StringIO(), _Clock(), RunTimings()
    display = ProgressDisplay(Style(Terminal()), live=False, stream=stream, timings=timings, clock=clock)
    display.start()
    try:
        # Past the delay, short of a heartbeat: the loop draws nothing.
        clock.now += progress.DELAY_SECONDS + 1
        time.sleep(0.05)
        # A heartbeat is due, but nothing runs that it could name.
        clock.now += progress.HEARTBEAT_SECONDS
        _wait_for(lambda: display._last_heartbeat == clock.now)
    finally:
        display.stop()
    assert stream.getvalue() == ""


def test_a_line_printed_before_the_display_starts_goes_nowhere() -> None:
    display = ProgressDisplay(Style(Terminal()), live=False, stream=None, timings=RunTimings(), clock=_Clock())
    display.line("early")
    assert display.live is False
