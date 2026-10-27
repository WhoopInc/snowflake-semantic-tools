"""Read a window frame clause in Snowflake's grammar, which is all a metric's `frame:` may be.

A frame is never passed through as free SQL: `canonical_frame` reads it as `ROWS` or `RANGE`
`BETWEEN` two bounds, and spells it canonically, or refuses it. `frame_order_problem` then says
why Snowflake would refuse a well-formed frame whose start bound comes after its end bound.
"""

from __future__ import annotations

import re

_FRAME_BOUND = (
    r"(?:UNBOUNDED\s+(?:PRECEDING|FOLLOWING)|CURRENT\s+ROW|(?:\d+|INTERVAL\s+'[^']*')\s+(?:PRECEDING|FOLLOWING))"
)
_FRAME = re.compile(rf"(ROWS|RANGE)\s+BETWEEN\s+({_FRAME_BOUND})\s+AND\s+({_FRAME_BOUND})", re.IGNORECASE)


def canonical_frame(value: object) -> str | None:
    """Return the frame clause in canonical spelling, or None when `value` is not one."""
    match = _FRAME.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        return None
    return f"{match.group(1).upper()} BETWEEN {_frame_bound(match.group(2))} AND {_frame_bound(match.group(3))}"


def _frame_bound(bound: str) -> str:
    return " ".join(token if token.startswith("'") else token.upper() for token in re.findall(r"'[^']*'|\S+", bound))


# A bound's place on the frame's axis: its side, then how far along that side it lies.
_SIDES = {"PRECEDING": 1, "FOLLOWING": 3}
_INTERVAL = re.compile(r"INTERVAL\s+'\s*(\d+)\s*([A-Za-z]+?)s?\s*'\s+(PRECEDING|FOLLOWING)", re.IGNORECASE)


def frame_order_problem(frame: str) -> str | None:
    """Say why a canonical frame's bounds are out of order, or None when they are in order.

    `UNBOUNDED PRECEDING` < `n PRECEDING` (larger n first) < `CURRENT ROW` < `n FOLLOWING`
    (smaller n first) < `UNBOUNDED FOLLOWING`. A frame may not start at `UNBOUNDED FOLLOWING` or
    end at `UNBOUNDED PRECEDING`. Two `INTERVAL` bounds on one side in different units cannot
    be compared here, and are left to Snowflake.
    """
    match = _FRAME.fullmatch(frame)
    if match is None:
        return None
    start, end = match.group(2), match.group(3)
    if start.upper() == "UNBOUNDED FOLLOWING":
        return "a frame cannot start at UNBOUNDED FOLLOWING"
    if end.upper() == "UNBOUNDED PRECEDING":
        return "a frame cannot end at UNBOUNDED PRECEDING"
    first, last = _rank(start), _rank(end)
    if first is None or last is None or (first[0] == last[0] and first[2] != last[2]) or first <= last:
        return None
    return f"its start bound '{start}' comes after its end bound '{end}'"


def _rank(bound: str) -> tuple[int, int, str] | None:
    """Return a bound's place, as `(side, distance, unit)`; None for an interval in an unknown unit."""
    upper = bound.upper()
    if upper == "UNBOUNDED PRECEDING":
        return (0, 0, "")
    if upper == "CURRENT ROW":
        return (2, 0, "")
    if upper == "UNBOUNDED FOLLOWING":
        return (4, 0, "")
    interval = _INTERVAL.fullmatch(bound)
    if interval is not None:
        count, unit, side = int(interval.group(1)), interval.group(2).upper(), interval.group(3).upper()
    elif upper.startswith("INTERVAL"):
        return None
    else:
        number, side = bound.split()
        count, unit, side = int(number), "", side.upper()
    return (_SIDES[side], -count if side == "PRECEDING" else count, unit)
