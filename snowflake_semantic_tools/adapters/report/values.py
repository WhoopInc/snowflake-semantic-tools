"""Typed reads of a parsed JSON report, the escaping every value takes into HTML, and number formats.

A report is JSON a run wrote and a user may have edited, so no reader trusts its shape: a value of
the wrong type reads as missing (None, an empty mapping, or an empty list) and never raises.
"""

from __future__ import annotations

import html
import math
from collections.abc import Mapping
from datetime import datetime

MISSING = "—"

_EMPTY: Mapping[str, object] = {}


def esc(value: object) -> str:
    """Return `value` as text safe in HTML content and inside a double- or single-quoted attribute.

    `html.escape` with quotes, then ``/`` and ``=`` as character references, so no value can put
    ``</``, ``//`` (a scheme-relative URL), or anything reading as ``name=`` into the document's
    text: what a browser shows is unchanged, and a scan of the raw output finds no such pattern.
    """
    return html.escape(str(value), quote=True).replace("/", "&#47;").replace("=", "&#61;")


def pill(state: str, label: str) -> str:
    """Return a labelled pill for a figure not known yet, `state` ``pending`` or ``unavailable``.

    The label is escaped; `state` names a class and must be one of the two, never report text.
    """
    return f'<span class="pill pill-{state}">{esc(label)}</span>'


def mapping(value: object) -> Mapping[str, object]:
    """Return `value` when it is a JSON object, else an empty one."""
    return value if isinstance(value, Mapping) else _EMPTY


def records(value: object) -> list[Mapping[str, object]]:
    """Return the objects in `value` when it is a JSON array, skipping any item that is not one."""
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def strings(value: object) -> list[str]:
    """Return the non-empty strings in `value` when it is a JSON array, skipping anything else."""
    return [item for item in value if isinstance(item, str) and item.strip()] if isinstance(value, list) else []


def number(value: object) -> float | None:
    """Return `value` as a float when it is a finite JSON number; None otherwise, booleans included."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def text(value: object) -> str | None:
    """Return `value` as display text: a non-blank string as it is, a number in its shortest form.

    None for null, a blank string, a boolean, an object, or an array.
    """
    if isinstance(value, str):
        return value if value.strip() else None
    found = number(value)
    if found is None:
        return None
    return str(int(found)) if found.is_integer() else str(found)


def score(value: float | None) -> str:
    """Format a 0-1 score to two places, or the missing mark."""
    return MISSING if value is None else f"{value:.2f}"


def credits(value: float) -> str:
    """Format a number of credits to four places, enough for a per-question cost."""
    return f"{value:.4f}"


def count(value: float) -> str:
    """Format a count of tokens or calls as a whole number with thousands separators."""
    return f"{round(value):,}"


def duration(milliseconds: float) -> str:
    """Format a latency: whole milliseconds under a second, else seconds to one place."""
    return f"{milliseconds:.0f} ms" if milliseconds < 1000 else f"{milliseconds / 1000:.1f} s"


def timestamp(value: object) -> str | None:
    """Format an ISO 8601 time as ``2026-10-22 14:03 UTC-04:00``; text that does not parse as it is.

    None when `value` is not text. A time without an offset is shown without one.
    """
    raw = text(value)
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return raw
    offset = parsed.strftime("%z")
    zone = f" UTC{offset[:3]}:{offset[3:5]}" if offset else ""
    return f"{parsed:%Y-%m-%d %H:%M}{zone}"
