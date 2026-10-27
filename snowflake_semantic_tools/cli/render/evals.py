"""How `sst eval` shows eval results: each attempt as a tree of its metrics, cost and questions.

An attempt's line names its eval and status, and how many questions passed. Under it come a
published attempt's run, each metric's pass count, average and threshold, what the attempt
cost, and the questions:
those that fell short, or every one under `-v`, each with its score per metric, the tools it
was expected to call and called, the subagents a CoWork profile's question accepts and its
router chose, and why a judge scored it as it did. `-v` adds each
question's time and tokens and the agent's answer. A reason or an answer is cut to two lines.

Every count names whose it is: the agent's time and tokens, and the judge's tokens; a count
nothing measured says so rather than showing 0. Under `--quiet` only the attempt's line, its
metrics and its cost are shown. Under `-o plain` the tree is printed without connectors or
color, each line indented by its depth, and nothing is cut.
"""

from __future__ import annotations

import textwrap
from collections.abc import Sequence
from dataclasses import dataclass

import click

from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.tree import TreeNode, connectors, tree_lines
from snowflake_semantic_tools.domain.evaluate.scores import average_text, score_text, threshold_phrase
from snowflake_semantic_tools.domain.model.eval import (
    ROUTING_ACCURACY,
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
)

TOOL_METRIC = "tool_selection_accuracy"
# The run name an unpublished attempt reports, as `app.evals.inline` names it.
UNPUBLISHED_RUN = "unpublished"
# Each count a cost reports, by the fields it is made of, and what it is called when unmeasured.
_UNMEASURED_LABELS = (
    (("total_input_tokens", "total_output_tokens"), "agent tokens"),
    (("llm_call_count",), "LLM calls"),
    (("total_tokens",), "judge tokens"),
)
_INDENT = 4
_MIN_ROOM = 24


@dataclass(frozen=True, slots=True)
class EvalShown:
    """One eval's attempts, as the report shows them."""

    eval_key: str
    attempts: tuple[EvalRunAttempt, ...]


@dataclass(frozen=True, slots=True)
class EvalMarks:
    """What the report marks beyond the results themselves.

    Attributes:
        approximate: The metrics an unpublished run judged, which never gate it, so a miss on
            one alone fails no question.
        unscored: The metrics the run could not score; their missing scores fail no question.
        regressions: `(question_key, metric_name)` pairs the gate found regressed.
    """

    approximate: frozenset[str] = frozenset()
    unscored: frozenset[str] = frozenset()
    regressions: frozenset[tuple[str, str]] = frozenset()


NO_MARKS = EvalMarks()


def eval_lines(
    evals: Sequence[EvalShown],
    style: Style,
    *,
    verbose: bool = False,
    summary_only: bool = False,
    plain: bool = False,
    marks: EvalMarks = NO_MARKS,
) -> list[str]:
    """Return the report's lines: each attempt's line, then its tree.

    Args:
        verbose: Show every question, with its time, tokens and answer; else only those that
            fell short.
        summary_only: Show each attempt's metrics and cost, and no questions.
        plain: Indent each line by its depth instead of drawing connectors, and cut nothing.
    """
    lines: list[str] = []
    for shown in evals:
        if not shown.attempts:
            lines.append(style(shown.eval_key, "strong"))
        for attempt in shown.attempts:
            several = len(shown.attempts) > 1
            lines.append(_header(shown.eval_key, attempt, several, style, marks))
            layout = _Layout(style, None if plain else style.terminal.width, verbose, marks)
            nodes = _attempt_nodes(attempt, layout, summary_only)
            if plain:
                lines.extend(_indented(nodes, 1))
            else:
                lines.extend(
                    style(prefix, "dim") + text
                    for prefix, text in tree_lines(nodes, connectors(style.terminal.unicode))
                )
    return lines


@dataclass(frozen=True, slots=True)
class _Layout:
    """How one attempt's tree is laid out: its styling, its width, and what it shows.

    Attributes:
        width: The terminal's width, which reasons and answers are cut to fit; None cuts nothing.
    """

    style: Style
    width: int | None
    verbose: bool
    marks: EvalMarks


def _header(eval_key: str, attempt: EvalRunAttempt, several: bool, style: Style, marks: EvalMarks) -> str:
    """Name the eval, the attempt when there are several, its status, and how many passed.

    An unpublished attempt says so here; a published one's run is the first row under it.
    """
    status_role = "success" if attempt.terminal_status == "COMPLETED" else "error"
    parts = [style(eval_key, "strong")]
    if several:
        parts.append(f"attempt {attempt.attempt}")
    if attempt.run_name == UNPUBLISHED_RUN:
        parts.append(attempt.run_name)
    parts.append(style(attempt.terminal_status, status_role))
    if attempt.rows:
        passed = sum(not _fell_short(row, marks) for row in attempt.rows)
        parts.append(f"{passed}/{_count(len(attempt.rows), 'question')} passed")
    return "  ".join(parts)


def _attempt_nodes(attempt: EvalRunAttempt, layout: _Layout, summary_only: bool) -> list[TreeNode]:
    """Return what goes under an attempt's line: metrics, cost, its trouble, then its questions."""
    style = layout.style
    nodes = [] if attempt.run_name == UNPUBLISHED_RUN else [TreeNode("run  " + attempt.run_name)]
    nodes.extend(_metric_nodes(attempt.rows, layout))
    first, more = _packed("cost  ", _cost_parts(attempt.cost, attempt.wall_ms), style.sep, layout.width, 1)
    nodes.append(TreeNode(first, more))
    if attempt.status_details:
        nodes.append(TreeNode("status details  " + "; ".join(attempt.status_details)))
    if attempt.retrieval_error:
        nodes.append(TreeNode(style("retrieval error", "error") + "  " + attempt.retrieval_error))
    if summary_only or not attempt.rows:
        return nodes
    rows = attempt.rows
    short = [row for row in rows if _fell_short(row, layout.marks)]
    shown = rows if layout.verbose else tuple(short)
    questions = tuple(_question_node(row, layout, depth=2) for row in shown)
    if layout.verbose:
        heading = f"questions  {len(rows) - len(short)} of {len(rows)} passed"
    elif short:
        heading = f"{len(short)} of {_count(len(rows), 'question')} fell short  " + style(
            "(-v lists every question)", "dim"
        )
    else:
        everything = "the question" if len(rows) == 1 else f"all {len(rows)} questions"
        heading = f"{everything} passed  " + style("(-v lists them)" if len(rows) > 1 else "(-v shows it)", "dim")
    nodes.append(TreeNode(heading, children=questions))
    return nodes


def _metric_nodes(rows: tuple[EvalResultRow, ...], layout: _Layout) -> list[TreeNode]:
    """Summarize each metric over the attempt's questions: passes, average, threshold and kind."""
    by_name: dict[str, list[EvalMetricResult]] = {}
    for row in rows:
        for metric in row.metrics:
            by_name.setdefault(metric.metric_name, []).append(metric)
    width = max(map(len, by_name), default=0)
    marks, style = layout.marks, layout.style
    nodes = []
    for name, results in sorted(by_name.items()):
        parts = []
        if name in marks.unscored:
            parts.append(style("not scored by this run", "dim"))
        else:
            scores = [result.score for result in results if result.score is not None]
            parts.append(f"{sum(result.passed for result in results)}/{len(results)} passed")
            parts.append(f"average {average_text(sum(scores) / len(scores) if scores else None)}")
            phrase = threshold_phrase(next((item.threshold for item in results if item.threshold), None))
            if phrase:
                parts.append(phrase)
        if name in marks.approximate:
            parts.append(style("approximate, never gates", "dim"))
        regressed = sum((result.question_key, name) in marks.regressions for result in results)
        if regressed:
            parts.append(style(f"{regressed} regressed", "error"))
        first, more = _packed(name.ljust(width) + "  ", parts, "  ", layout.width, 1)
        nodes.append(TreeNode(first, more))
    return nodes


def _question_node(row: EvalResultRow, layout: _Layout, depth: int) -> TreeNode:
    """Show one question: whether it passed, its number and text, and what each metric made of it."""
    style = layout.style
    short = _fell_short(row, layout.marks)
    mark = style(style.symbol("fail"), "error") if short else style(style.symbol("ok"), "success")
    label = f"{mark} {row.index if row.index is not None else '?'}  "
    plain_label = f"{style.symbol('fail' if short else 'ok')} {row.index if row.index is not None else '?'}  "
    first, more = _wrapped(plain_label, row.input_query, layout, depth)
    first = label + first[len(plain_label) :]
    if layout.verbose and row.cost is not None:
        # The question's cost follows its text, on a line of its own when the last one is full.
        spent = style(_question_cost(row.cost, style.sep), "dim")
        lines = [first, *more]
        if layout.width is not None and _visible(lines[-1]) + 2 + _visible(spent) > layout.width - _INDENT * depth:
            lines.append(" " * len(plain_label) + spent)
        else:
            lines[-1] += "  " + spent
        first, more = lines[0], tuple(lines[1:])
    children: list[TreeNode] = []
    if row.error is not None:
        children.append(_labeled(style("error", "error"), "error", row.error, layout, depth + 1))
    metrics = [metric for metric in row.metrics if metric.metric_name not in layout.marks.unscored]
    width = max((len(metric.metric_name) for metric in metrics), default=0)
    for metric in metrics:
        children.append(_metric_node(row, metric, width, layout, depth + 1))
    if layout.verbose and row.answer is not None:
        children.append(_labeled("answer", "answer", row.answer, layout, depth + 1))
    return TreeNode(first, more, tuple(children))


def _metric_node(row: EvalResultRow, metric: EvalMetricResult, width: int, layout: _Layout, depth: int) -> TreeNode:
    """Show one metric's score on a question; why it fell short, or under `-v` why it scored as it did."""
    style = layout.style
    failed = not metric.passed
    parts = [metric.metric_name.ljust(width), style(score_text(metric.score), "error" if failed else "success")]
    phrase = threshold_phrase(metric.threshold)
    if failed and phrase:
        parts.append(phrase)
    if (row.question_key, metric.metric_name) in layout.marks.regressions:
        parts.append(style("regressed", "error"))
    children: list[TreeNode] = []
    explain = failed or layout.verbose
    tools_known = metric.metric_name == TOOL_METRIC and row.called_tools is not None
    # Snowflake private preview (PrPr): CoWork profiles.
    routes_known = metric.metric_name == ROUTING_ACCURACY and (
        row.expected_subagents is not None or row.routed_subagents is not None
    )
    if explain and tools_known:
        children.append(TreeNode("expected  " + _tool_list(row.expected_tools)))
        children.append(TreeNode("called    " + _tool_list(row.called_tools)))
    elif explain and routes_known:
        children.append(TreeNode("expected  " + _subagent_list(row.expected_subagents, "none (a decline)")))
        children.append(TreeNode("routed    " + _subagent_list(row.routed_subagents, "none")))
    elif explain and metric.explanation:
        children.append(_labeled("reason", "reason", metric.explanation, layout, depth + 1))
    return TreeNode("  ".join(parts).rstrip(), children=tuple(children))


def _labeled(label: str, plain_label: str, text: str, layout: _Layout, depth: int) -> TreeNode:
    """A `label  text` row, its text cut to two lines; `plain_label` is the label's width."""
    first, more = _wrapped(plain_label + "  ", text, layout, depth)
    return TreeNode(label + first[len(plain_label) :], more)


def _wrapped(label: str, text: str, layout: _Layout, depth: int) -> tuple[str, tuple[str, ...]]:
    """Fit `label` and `text` on one line or two in a row `depth` levels down.

    Depth 1 is just under the attempt's line. The second line ends in an ellipsis when more was
    left; with no width, everything goes on one line.
    """
    flat = " ".join(text.split())
    if layout.width is None:
        return label + flat, ()
    room = max(layout.width - _INDENT * depth - len(label), _MIN_ROOM)
    lines = textwrap.wrap(flat, room) or [""]
    if len(lines) > 2:
        ellipsis = layout.style.symbol("ellipsis")
        lines = [lines[0], lines[1][: room - len(ellipsis)].rstrip() + ellipsis]
    return label + lines[0], tuple(" " * len(label) + line for line in lines[1:])


def _fell_short(row: EvalResultRow, marks: EvalMarks) -> bool:
    """Whether the question errored, or a metric that can fail it did not pass."""
    lenient = marks.approximate | marks.unscored
    return row.error is not None or any(
        not metric.passed for metric in row.metrics if metric.metric_name not in lenient
    )


def _packed(label: str, parts: list[str], sep: str, width: int | None, depth: int) -> tuple[str, tuple[str, ...]]:
    """Join `parts` after `label`, on as many lines as fit `depth` levels down, each part whole."""
    if not parts:
        return label.rstrip(), ()
    room = None if width is None else max(width - _INDENT * depth - len(label), _MIN_ROOM)
    lines = [parts[0]]
    for part in parts[1:]:
        if room is not None and _visible(lines[-1]) + len(sep) + _visible(part) > room:
            lines.append(part)
        else:
            lines[-1] += sep + part
    return label + lines[0], tuple(" " * len(label) + line for line in lines[1:])


def _visible(text: str) -> int:
    """Count the characters of `text` a terminal shows, leaving out color codes."""
    return len(click.unstyle(text))


def _cost_parts(cost: EvalCostSummary, wall_ms: int | None) -> list[str]:
    """Say what an attempt cost, naming whose each count is, and which counts nothing measured."""
    parts = []
    if wall_ms is not None:
        parts.append(f"{wall_ms / 1000:.1f}s wall")
    parts.append(f"{cost.duration_ms / 1000:.1f}s agent time")
    unmeasured = set(cost.unmeasured)
    if not unmeasured & {"total_input_tokens", "total_output_tokens"}:
        parts.append(f"agent {cost.total_input_tokens:,} in / {cost.total_output_tokens:,} out")
    if "llm_call_count" not in unmeasured:
        parts.append(_count(cost.llm_call_count, "LLM call"))
    if "total_tokens" not in unmeasured:
        parts.append(f"judge {cost.total_tokens:,} tokens")
    missing = [label for fields, label in _UNMEASURED_LABELS if unmeasured & set(fields)]
    if missing:
        parts.append("not measured: " + ", ".join(missing))
    return parts


def _question_cost(cost: EvalCostSummary, sep: str) -> str:
    """A question's own time, and the agent's tokens when they were measured."""
    parts = [f"{cost.duration_ms / 1000:.1f}s"]
    if not set(cost.unmeasured) & {"total_input_tokens", "total_output_tokens"}:
        parts.append(f"{cost.total_input_tokens:,} in / {cost.total_output_tokens:,} out")
    return sep.join(parts)


def _count(number: int, noun: str) -> str:
    return f"{number:,} {noun}" + ("" if number == 1 else "s")


def _tool_list(tools: tuple[str, ...] | None) -> str:
    if tools is None:
        return "no expectation"
    return ", ".join(tools) if tools else "none"


def _subagent_list(subagents: tuple[str, ...] | None, empty: str) -> str:
    """Name a profile question's subagents; `empty` for none, and `not known` when unread."""
    if subagents is None:
        return "not known"
    return ", ".join(subagents) if subagents else empty


def _indented(nodes: Sequence[TreeNode], depth: int) -> list[str]:
    """Lay out `nodes` without connectors, each line indented two spaces per level."""
    lines = []
    for node in nodes:
        lines.append("  " * depth + node.text)
        lines.extend("  " * depth + text for text in node.more)
        lines.extend(_indented(node.children, depth + 1))
    return lines
