"""Read a window frame clause in Snowflake's grammar, which is all a metric's `frame:` may be.

A frame is never passed through as free SQL: `canonical_frame` reads it as `ROWS` or `RANGE`
`BETWEEN` two bounds, and spells it canonically, or refuses it.
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
