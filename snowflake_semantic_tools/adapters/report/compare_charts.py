"""The comparison report's charts, drawn as inline SVG from the report's numbers.

Each chart is sized in a fixed coordinate space and scales to its container through `viewBox`.
It draws with the stylesheet's classes, never a color of its own, carries a `<title>` and an
`aria-label` saying what it shows, and prints every value as text beside its mark, so a reader
who cannot tell the colors apart, or sees no chart at all, loses nothing.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from snowflake_semantic_tools.adapters.report.compare_model import EvalView, Metric, ModelRow
from snowflake_semantic_tools.adapters.report.values import credits, esc, pill, score

_WIDTH = 900
_LABEL = 232
_PLOT = 540
_ROW = 32
_TOP = 26
_AXIS = 30
_NAME_CHARS = 30

_SCATTER_HEIGHT = 320
_LEFT = 64
_RIGHT = 36
_SCATTER_TOP = 18
_BOTTOM = 54
_SCATTER_LEGEND = '<p class="aside">◆ the pinned model · ● a candidate</p>'


def metric_chart(metric: Metric, models: Sequence[ModelRow]) -> str:
    """Draw one metric as a horizontal bar per model, its spread as a whisker, its gate as a line.

    The scale is 0 to 1. A model with no mean for the metric gets an empty track and the words
    ``no score`` where its value would be.
    """
    height = _TOP + _ROW * len(models) + _AXIS
    bottom = _TOP + _ROW * len(models)
    parts = [_tick(0.25 * step, bottom) for step in range(5)]
    spoken: list[str] = []
    for index, model in enumerate(models):
        found = model.scores.get(metric.name)
        mean = None if found is None else found.mean
        spoken.append(f"{model.name} {score(mean)}")
        parts.append(_bar_row(index, model, found.low if found else None, found.high if found else None, mean))
    if metric.threshold is not None:
        x = _x(metric.threshold)
        parts.append(f'<line class="threshold" x1="{x:.1f}" y1="{_TOP - 8}" x2="{x:.1f}" y2="{bottom}"/>')
        parts.append(
            f'<text class="threshold-label" x="{x:.1f}" y="{_TOP - 12}" text-anchor="middle">'
            f"gate ≥ {esc(score(metric.threshold))}</text>"
        )
    label = f"{metric.name} by model, scale 0 to 1: " + ", ".join(spoken)
    return _svg(_WIDTH, height, label, parts)


def quality_cost_chart(view: EvalView) -> str:
    """Plot each model's headline mean against its credits per question, with what cannot be plotted.

    Only a model with both a known cost and a mean is a point; the rest are listed beneath the
    chart with why. With no point at all the chart is replaced by a sentence saying so.
    """
    headline = view.headline
    points: list[tuple[ModelRow, float, float]] = []
    left_out: list[str] = []
    for model in view.models:
        cost = model.costs["credits_per_question"]
        found = model.scores.get(headline.name) if headline else None
        mean = None if found is None else found.mean
        if cost.value is None:
            left_out.append(f"{esc(model.name)} {pill(cost.state, f'cost {cost.state}')}")
        elif mean is None:
            left_out.append(f"{esc(model.name)} {pill('unavailable', 'not scored')}")
        else:
            points.append((model, cost.value, mean))
    if points:
        body = _scatter(points, headline.name if headline else "score") + _SCATTER_LEGEND
    else:
        body = '<p class="empty">No model has both a known cost and a score to plot yet.</p>'
    if left_out:
        body += f'<p class="aside">Not plotted: {", ".join(left_out)}.</p>'
    return body


def _bar_row(index: int, model: ModelRow, low: float | None, high: float | None, mean: float | None) -> str:
    """Draw one model's label, track, bar, whisker, and value; the caller has checked nothing."""
    top = _TOP + _ROW * index
    middle = top + _ROW / 2
    name = model.name if len(model.name) <= _NAME_CHARS else model.name[: _NAME_CHARS - 1] + "…"
    role = " (pinned)" if model.pinned else ""
    parts = [
        f'<text x="{_LABEL - 12}" y="{middle:.1f}" text-anchor="end" dominant-baseline="middle">'
        f"<title>{esc(model.name)}</title>{esc(name)}{role}</text>",
        f'<rect class="track" x="{_LABEL}" y="{middle - 7:.1f}" width="{_PLOT}" height="14" rx="3"/>',
    ]
    if mean is not None:
        bar = "bar pinned" if model.pinned else "bar"
        width = _x(mean) - _LABEL
        parts.append(f'<rect class="{bar}" x="{_LABEL}" y="{middle - 7:.1f}" width="{width:.1f}" height="14" rx="3"/>')
    if low is not None and high is not None:
        x1, x2 = _x(low), _x(high)
        parts.append(f'<line class="whisker" x1="{x1:.1f}" y1="{middle:.1f}" x2="{x2:.1f}" y2="{middle:.1f}"/>')
        parts.extend(
            f'<line class="whisker" x1="{x:.1f}" y1="{middle - 6:.1f}" x2="{x:.1f}" y2="{middle + 6:.1f}"/>'
            for x in (x1, x2)
        )
    value = esc(score(mean)) if mean is not None else "no score"
    spread = f" ({esc(score(low))}–{esc(score(high))})" if low is not None and high is not None and low != high else ""
    parts.append(
        f'<text class="value" x="{_LABEL + _PLOT + 12}" y="{middle:.1f}" dominant-baseline="middle">'
        f'{value}<tspan class="tick">{spread}</tspan></text>'
    )
    return "".join(parts)


def _tick(value: float, bottom: int) -> str:
    x = _x(value)
    return (
        f'<line class="grid" x1="{x:.1f}" y1="{_TOP - 4}" x2="{x:.1f}" y2="{bottom}"/>'
        f'<text class="tick" x="{x:.1f}" y="{bottom + 18}" text-anchor="middle">{value:g}</text>'
    )


def _x(value: float) -> float:
    return _LABEL + _PLOT * min(max(value, 0.0), 1.0)


def _scatter(points: Sequence[tuple[ModelRow, float, float]], metric: str) -> str:
    """Draw the points of `quality_cost_chart` on axes fitted to them; one point is enough."""
    x_ticks = _cost_ticks(max(cost for _, cost, _ in points))
    y_ticks = _score_ticks(min(mean for _, _, mean in points))
    plot_w = _WIDTH - _LEFT - _RIGHT
    plot_h = _SCATTER_HEIGHT - _SCATTER_TOP - _BOTTOM
    floor_y, top_x = y_ticks[0], x_ticks[-1]

    def place(cost: float, mean: float) -> tuple[float, float]:
        y = (mean - floor_y) / (1.0 - floor_y)
        return _LEFT + plot_w * cost / top_x, _SCATTER_TOP + plot_h * (1 - min(max(y, 0.0), 1.0))

    base = _SCATTER_TOP + plot_h
    parts = [f'<line class="axis-line" x1="{_LEFT}" y1="{base}" x2="{_LEFT + plot_w}" y2="{base}"/>']
    for tick in x_ticks:
        x, _ = place(tick, floor_y)
        parts.append(f'<text class="tick" x="{x:.1f}" y="{base + 18}" text-anchor="middle">{_trim(tick)}</text>')
    for tick in y_ticks:
        _, y = place(0.0, tick)
        parts.append(f'<line class="grid" x1="{_LEFT}" y1="{y:.1f}" x2="{_LEFT + plot_w}" y2="{y:.1f}"/>')
        parts.append(f'<text class="tick" x="{_LEFT - 10}" y="{y + 4:.1f}" text-anchor="end">{tick:.2f}</text>')
    parts.append(
        f'<text class="axis" x="{_LEFT + plot_w / 2:.1f}" y="{_SCATTER_HEIGHT - 8}" text-anchor="middle">'
        "Credits per question →</text>"
        f'<text class="axis" transform="translate(16 {_SCATTER_TOP + plot_h / 2:.1f}) rotate(-90)" '
        f'text-anchor="middle">{esc(metric)} (mean) →</text>'
    )
    spoken = []
    for model, cost, mean in points:
        x, y = place(cost, mean)
        parts.append(_point(model, x, y, x > _LEFT + plot_w - 170, f"{score(mean)} at {credits(cost)} credits"))
        spoken.append(f"{model.name} {score(mean)} at {credits(cost)}")
    return _svg(_WIDTH, _SCATTER_HEIGHT, f"{metric} against credits per question: " + ", ".join(spoken), parts)


def _point(model: ModelRow, x: float, y: float, flip: bool, detail: str) -> str:
    """Draw one point, a diamond for the pinned model, labeled on whichever side has room."""
    name = esc(model.name) + (" (pinned)" if model.pinned else "")
    if model.pinned:
        mark = f'<path class="point pinned" d="M{x:.1f} {y - 8:.1f}l8 8l-8 8l-8 -8z"/>'
    else:
        mark = f'<circle class="point" cx="{x:.1f}" cy="{y:.1f}" r="7"/>'
    anchor, dx = ("end", -13) if flip else ("start", 13)
    return (
        f"<g><title>{esc(model.name)}: {esc(detail)}</title>{mark}"
        f'<text x="{x + dx:.1f}" y="{y + 4:.1f}" text-anchor="{anchor}">{name}</text></g>'
    )


def _cost_ticks(high: float) -> list[float]:
    """Return evenly spaced ticks from 0 past `high`, at a step of 1, 2, 2.5, or 5 times a power of ten."""
    if high <= 0:
        return [0.0, 1.0]
    raw = high * 1.15 / 4
    magnitude = 10 ** math.floor(math.log10(raw))
    step = next(factor * magnitude for factor in (1, 2, 2.5, 5, 10) if factor * magnitude >= raw)
    count = math.ceil(high * 1.15 / step)
    return [step * index for index in range(count + 1)]


def _score_ticks(low: float) -> list[float]:
    """Return ticks from just under `low` to 1, at least a tenth apart in span, never below 0."""
    floor = max(0.0, min(math.floor((low - 0.05) * 10) / 10, 0.9))
    step = 0.05 if 1.0 - floor <= 0.3 else 0.1 if 1.0 - floor <= 0.6 else 0.2
    count = round((1.0 - floor) / step)
    return [round(floor + step * index, 4) for index in range(count + 1)]


def _trim(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _svg(width: int, height: int, label: str, parts: Sequence[str]) -> str:
    return (
        f'<svg class="plot" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(label)}" '
        f'preserveAspectRatio="xMinYMin meet"><title>{esc(label)}</title>{"".join(parts)}</svg>'
    )
