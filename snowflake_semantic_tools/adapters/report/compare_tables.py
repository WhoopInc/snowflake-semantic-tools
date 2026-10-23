"""The comparison report's two tables: each question's result by model, and each model's costs.

A cell's color is never its only signal: a result carries a symbol and its score, a change a
triangle, and a cost not known yet a labeled pill, never a zero. A heatmap cell's tooltip holds
the judge's reason and, for a CoWork profile, the subagents the router chose.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from snowflake_semantic_tools.adapters.report.compare_model import Cost, EvalView, ModelRow, Question, Result
from snowflake_semantic_tools.adapters.report.values import MISSING, count, credits, esc, pill, score

_RESULTS = {
    True: ("c-pass", "✓", "passed"),
    False: ("c-fail", "✗", "failed"),
}
_CHANGES = {
    "fixes": ("▲", "fixes: the pinned model failed this question, this model passed it"),
    "breaks": ("▼", "breaks: the pinned model passed this question, this model failed it"),
}
_PINNED = ' <span class="muted">(pinned)</span>'
_COST_COLUMNS: tuple[tuple[str, str, Callable[[float], str]], ...] = (
    ("agent_credits", "Agent credits", credits),
    ("sql_credits", "SQL credits", credits),
    ("judge_tokens", "Judge tokens", count),
    ("credits_per_question", "Credits / question", credits),
)
_TOKEN_COLUMNS = (("agent_input", "Agent tokens in"), ("agent_output", "Agent tokens out"), ("llm_calls", "LLM calls"))


def question_heatmap(view: EvalView) -> str:
    """Render one row per question and one cell per model, with a legend; a sentence when none."""
    if not view.questions:
        return '<p class="empty">The report holds no question for this eval.</p>'
    head = "".join(f'<th scope="col">{esc(model.name)}{_PINNED if model.pinned else ""}</th>' for model in view.models)
    rows = "".join(_question_row(view, question) for question in view.questions)
    legend = (
        '<div class="legend" aria-label="Legend">'
        '<span><span class="swatch c-pass">✓</span>passed</span>'
        '<span><span class="swatch c-fail">✗</span>failed</span>'
        '<span><span class="swatch c-score">◐</span>scored, pass not recorded</span>'
        '<span><span class="swatch c-none">—</span>no result</span>'
        '<span><span class="change fixes">▲</span> fixes a question the pinned model failed</span>'
        '<span><span class="change breaks">▼</span> breaks a question the pinned model passed</span>'
        "<span>Hover a cell for the judge’s reason"
        f"{' and the route' if view.kind == 'cowork_profile' else ''}.</span></div>"
    )
    return (
        '<div class="table-wrap"><table class="heat"><caption class="sr">Result of each question by model</caption>'
        f'<thead><tr><th scope="col">Question</th>{head}</tr></thead><tbody>{rows}</tbody></table></div>{legend}'
    )


def cost_table(view: EvalView) -> str:
    """Render each model's costs and token counts, with what pending and unavailable mean."""
    head = "".join(f'<th scope="col" class="n">{label}</th>' for _, label, _ in _COST_COLUMNS)
    head += "".join(f'<th scope="col" class="n">{label}</th>' for _, label in _TOKEN_COLUMNS)
    rows = "".join(_cost_row(model) for model in view.models)
    notes = [
        "<strong>Pending</strong>: the usage has not landed in the account’s usage views yet.",
        "<strong>Unavailable</strong>: the role may not read the usage view, or nothing can attribute the usage.",
    ]
    if view.kind == "cowork_profile":
        notes.insert(0, "For a CoWork profile these costs cover the router only, not the subagents it routes to.")
    footnotes = "".join(f"<p>{note}</p>" for note in notes)
    return (
        '<div class="table-wrap"><table><caption class="sr">Cost and usage by model</caption>'
        f'<thead><tr><th scope="col">Model</th>{head}</tr></thead><tbody>{rows}</tbody></table></div>'
        f'<div class="aside">{footnotes}</div>'
    )


def _question_row(view: EvalView, question: Question) -> str:
    label = esc(question.id) if question.id else MISSING
    prompt = esc(question.text) if question.text else '<span class="muted">No question text</span>'
    cells = "".join(_cell(view, question, model) for model in view.models)
    return f'<tr><th scope="row"><span class="qid">{label}</span><span class="qtext">{prompt}</span></th>{cells}</tr>'


def _cell(view: EvalView, question: Question, model: ModelRow) -> str:
    """Render one heatmap cell: its class and symbol from the result, its change, its tooltip."""
    result = question.results.get(model.name)
    change = None if model.pinned else question.changes.get(model.name)
    if result is None:
        css, symbol, said = "c-none", MISSING, "no result"
    elif result.passed is None:
        css, symbol, said = ("c-score", "◐", "scored") if result.score is not None else ("c-none", MISSING, "unscored")
    else:
        css, symbol, said = _RESULTS[result.passed]
    lines = [f"{model.name}: {said}"]
    if result is not None:
        lines.extend(_details(view, result))
    marker = ""
    if change is not None:
        arrow, meaning = _CHANGES[change]
        lines.append(meaning[0].upper() + meaning[1:])
        marker = f'<span class="change {change}" aria-hidden="true">{arrow}</span><span class="sr">, {change}</span>'
    value = f" {esc(score(result.score))}" if result is not None and result.score is not None else ""
    changed = change or "none"
    return (
        f'<td class="cell {css}" data-cell="{esc(model.name)}" data-change="{changed}" '
        f'title="{esc(chr(10).join(lines))}"><span aria-hidden="true">{symbol}</span>'
        f'<span class="sr">{said}</span>{value}{marker}</td>'
    )


def _details(view: EvalView, result: Result) -> list[str]:
    lines = [] if result.score is None else [f"Score {score(result.score)}"]
    if result.reason:
        lines.append(f"Reason: {result.reason}")
    if view.kind == "cowork_profile":
        route = result.route
        lines.append("Route: not recorded" if route is None else f"Route: {', '.join(route) or 'declined'}")
    return lines


def _cost_row(model: ModelRow) -> str:
    cells = [_cost_cell(model.costs[key], render) for key, _, render in _COST_COLUMNS]
    cells.extend(_figure(model.tokens[key]) for key, _ in _TOKEN_COLUMNS)
    role = ' <span class="badge b-pinned">Pinned</span>' if model.pinned else ""
    return f'<tr><th scope="row">{esc(model.name)}{role}</th>{_numbers(cells)}</tr>'


def _cost_cell(cost: Cost, render: Callable[[float], str]) -> str:
    return pill(cost.state, cost.state) if cost.value is None else esc(render(cost.value))


def _figure(value: float | None) -> str:
    return MISSING if value is None else esc(count(value))


def _numbers(cells: Sequence[str]) -> str:
    return "".join(f'<td class="n">{cell}</td>' for cell in cells)
