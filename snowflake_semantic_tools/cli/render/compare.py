"""How `sst eval --compare-models` shows a comparison: ranked models, what differs, the verdict.

Each eval's line names it, its questions and runs, and its verdict. Under it, one row per model in
rank order, the pinned one marked: its rank, model, role, the headline metric's mean with a bar
of its spread across runs, every other metric's mean, its credits per question (or `pending`),
its p95 latency, and the version and alias its runs evaluated; a model that failed says why.
Then the questions on which a candidate fixes or breaks what the pinned model did, the verdict,
the config line to review for a winner, and the notes. The report's files close the output.
Under `-o plain` nothing is coloured or cut.

Example:
    eval:sales_agent  8 questions × 2 runs  winner claude-opus-4-8
      #  model              role       answer_correctness       credits/q  p95    version
      1  claude-opus-4-8    candidate  0.94  [   |-o|]  0.90-0.98  0.0466   12.1s  VERSION$6 EVAL_CLAUDE_OPUS_4_8
      2  claude-sonnet-4-6  pinned     0.88  [  |o-| ]  0.85-0.90  0.0258   14.2s  VERSION$4
      questions that differ
        q3  claude-opus-4-8 breaks  What was revenue by month in 2025?
      verdict  claude-opus-4-8 beats the pinned claude-sonnet-4-6 on answer_correctness ...
      pin      agents.+orchestration_model: claude-opus-4-8
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.domain.evaluate.compare import (
    KNOWN,
    PINNED,
    EvalComparison,
    ModelReport,
    headline_metric,
)
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport

# The cells of the spread bar, from a score of 0 at its left to 1 at its right.
_BAR_CELLS = 10
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_VERDICT_ROLES = {"winner": "success", "pinned_holds": "strong", "no_clear_winner": "warning", "incomplete": "error"}


def comparison_lines(report: CompareReport, style: Style, *, files: Sequence[str] = ()) -> list[str]:
    """Return the comparison's lines: each eval's models, questions that differ and verdict, then files.

    Args:
        files: Where the report was written, each shown on the closing line.
    """
    lines: list[str] = []
    for evaluation in report.evals:
        lines.extend(_eval_lines(evaluation, style))
    if files:
        lines.append(style("report", "dim") + "  " + ", ".join(files))
    return lines


def _eval_lines(evaluation: EvalComparison, style: Style) -> list[str]:
    """Lay out one eval: its line, the model table, the differing questions, the verdict and notes."""
    kind = evaluation.verdict.kind
    head = [
        style(_eval_key(evaluation), "strong"),
        f"{_count(evaluation.dataset.questions, 'question')} {'×' if style.terminal.unicode else 'x'} "
        f"{_count(evaluation.runs_per_model, 'run')}",
        style(
            kind.replace("_", " ") + (f" {evaluation.verdict.model}" if evaluation.verdict.model else ""),
            _VERDICT_ROLES.get(kind, "strong"),
        ),
    ]
    lines = ["  ".join(head)]
    lines.extend("  " + row for row in _table(evaluation, style))
    changed = [question for question in evaluation.questions if question.changes]
    if changed:
        lines.append("  " + style("questions that differ", "strong"))
        for question in changed:
            moves = ", ".join(f"{model} {change}" for model, change in question.changes)
            lines.append(f"    {question.id}  {moves}  {question.text}")
    lines.append("  " + style("verdict", "dim") + "  " + evaluation.verdict.summary)
    if evaluation.verdict.pin_suggestion:
        first, *rest = evaluation.verdict.pin_suggestion.split("\n")
        lines.append("  " + style("pin", "dim") + "      " + style(first, "dim"))
        lines.extend("           " + style(line, "strong") for line in rest)
    lines.extend("  " + style("note", "dim") + "     " + note for note in evaluation.notes)
    return lines


def _table(evaluation: EvalComparison, style: Style) -> list[str]:
    """Return the model table: a header, then one row per model in rank order, columns aligned."""
    metric = headline_metric(evaluation.metrics)
    others = [item.name for item in evaluation.metrics if metric is None or item.name != metric.name]
    header = ["#", "model", "role", *([metric.name] if metric else []), *others, "credits/q", "p95", "version"]
    ranked = sorted(evaluation.models, key=lambda model: (model.rank, model.role != PINNED))
    rows = [header, *(_row(model, metric.name if metric else None, others, style) for model in ranked)]
    widths = [max(len(_plain(row[index])) for row in rows) for index in range(len(header) - 1)]
    rendered = []
    for number, row in enumerate(rows):
        cells = [cell + " " * (width - len(_plain(cell))) for cell, width in zip(row, widths, strict=False)]
        text = "  ".join((*cells, row[-1])).rstrip()
        rendered.append(style(text, "dim") if number == 0 else text)
    for model in ranked:
        if model.error:
            rendered.append(f"   {style(model.model, 'strong')}: {style(model.error, 'error')}")
    return rendered


def _row(model: ModelReport, headline: str | None, others: Sequence[str], style: Style) -> list[str]:
    """Return one model's cells, the headline metric's with its spread bar."""
    cells = [str(model.rank), model.model, style(model.role, "strong" if model.role == PINNED else "dim")]
    if headline is not None:
        summary = model.metric(headline)
        if summary is None or summary.mean is None:
            cells.append(style(model.status, "error"))
        else:
            spread = f"{_score(summary.min)}-{_score(summary.max)}"
            cells.append(f"{_score(summary.mean)}  {_bar(summary.mean, summary.min, summary.max, style)}  {spread}")
    cells.extend(_score(summary.mean) if (summary := model.metric(name)) is not None else "-" for name in others)
    price = model.cost.credits_per_question
    cells.append(
        f"{price.value:.4f}" if price.state == KNOWN and price.value is not None else style(price.state, "dim")
    )
    cells.append(f"{model.latency_p95 / 1000:.1f}s" if model.latency_p95 is not None else "-")
    cells.append(" ".join(part for part in (model.version or "not committed", model.alias or "") if part))
    return cells


def _bar(mean: float, low: float | None, high: float | None, style: Style) -> str:
    """Draw a run spread on a 0-to-1 scale: its ends, the span between, and the mean."""
    unicode = style.terminal.unicode
    cells = [" "] * _BAR_CELLS
    start, end, middle = (
        _cell(value) for value in (low if low is not None else mean, high if high is not None else mean, mean)
    )
    for index in range(start, end + 1):
        cells[index] = "─" if unicode else "-"
    cells[start], cells[end] = ("├", "┤") if unicode else ("|", "|")
    cells[middle] = "●" if unicode else "o"
    return "[" + "".join(cells) + "]"


def _cell(value: float) -> int:
    return min(_BAR_CELLS - 1, max(0, round(value * (_BAR_CELLS - 1))))


def _score(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _count(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _eval_key(evaluation: EvalComparison) -> str:
    folded = evaluation.eval.casefold()
    return f"eval:{folded}" if evaluation.kind == "agent" else f"eval:cowork_profile:{folded}"


def _plain(text: str) -> str:
    """Return text without its ANSI styling, which a column's width does not count."""
    return _ANSI.sub("", text)
