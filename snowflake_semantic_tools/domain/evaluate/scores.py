"""How eval scores and thresholds read to a person: one phrase each, the same everywhere.

A threshold is a pass band whose open side is unbounded, so `{min: 0.8}` reads "needs at
least 0.8", never as a range with an infinite end. A question's score has two places, an
average three.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.eval.model import ThresholdRange


def threshold_phrase(threshold: ThresholdRange | None) -> str | None:
    """Say what score a threshold needs, or None without a threshold or with both sides open.

    Example:
        `needs at least 0.8`, `needs at most 0.2`, or `needs 0.5 to 0.9`.
    """
    if threshold is None or (threshold.min is None and threshold.max is None):
        return None
    if threshold.max is None:
        return f"needs at least {threshold.min:g}"
    if threshold.min is None:
        return f"needs at most {threshold.max:g}"
    return f"needs {threshold.min:g} to {threshold.max:g}"


def score_text(score: float | None) -> str:
    """Write one question's score with two places, or `no score`."""
    return "no score" if score is None else f"{score:.2f}"


def average_text(average: object) -> str:
    """Write an average score with three places, or `no score` when none was scored."""
    return f"{average:.3f}" if isinstance(average, (int, float)) and not isinstance(average, bool) else "no score"
