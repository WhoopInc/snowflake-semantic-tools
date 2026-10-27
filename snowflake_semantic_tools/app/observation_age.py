"""How old a recorded observation is: the time between two ISO 8601 times, and how to say it."""

from __future__ import annotations

from datetime import datetime


def seconds_between(earlier: str, later: str) -> float | None:
    """The seconds from one ISO 8601 time to another; None when either does not parse or lacks a zone."""
    try:
        return (datetime.fromisoformat(later) - datetime.fromisoformat(earlier)).total_seconds()
    except (TypeError, ValueError):
        return None


def duration(seconds: float) -> str:
    """A duration as hours and minutes, such as `2h 05m`."""
    minutes = int(seconds) // 60
    return f"{minutes // 60}h {minutes % 60:02d}m"
