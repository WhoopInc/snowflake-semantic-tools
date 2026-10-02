"""How old the Snowflake observation a saved plan recorded is, when apply takes the plan up.

Apply plans afresh before it writes, so a saved plan's own observation is never acted on;
an old one is reported as ignored, which tells the user the fresh one was used instead.
"""

from __future__ import annotations

from datetime import datetime

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.state import OBSERVATION_TTL_SECONDS, SavedPlan


def stale_observation(saved: SavedPlan, current: SavedPlan, *, source: str) -> Diagnostic | None:
    """Report a saved observation older than `OBSERVATION_TTL_SECONDS` when `current` observed.

    A time that does not parse as an ISO 8601 time with a zone is never past its time to live.

    Args:
        current: The fresh plan apply made just now, whose observation replaces the saved one.
        source: How a report names the saved plan, such as its file path.

    Diagnostics:
        SST-MAN030: the saved observation is older than its time to live.
    """
    age = _seconds_between(saved.observation_at, current.observation_at)
    if age is None or age <= OBSERVATION_TTL_SECONDS:
        return None
    return D("SST-MAN030", value=source, detail=_duration(age))


def _seconds_between(earlier: str, later: str) -> float | None:
    """The seconds from one ISO 8601 time to another; None when either does not parse or lacks a zone."""
    try:
        return (datetime.fromisoformat(later) - datetime.fromisoformat(earlier)).total_seconds()
    except (TypeError, ValueError):
        return None


def _duration(seconds: float) -> str:
    """A duration as hours and minutes, such as `2h 05m`."""
    minutes = int(seconds) // 60
    return f"{minutes // 60}h {minutes % 60:02d}m"
