"""Find relative dates: phrases in prose, and functions in SQL, whose meaning moves with the clock.

A relative date re-evaluates on every run, so text that holds one silently changes meaning
while it reads the same. Prose is an eval question, an expected answer, or an agent's sample
question; SQL is a verified query.
"""

from __future__ import annotations

import re

_PHRASE = re.compile(
    r"\b(?:last|this|current|recent)\s+(?:day|week|month|quarter|year)\b|"
    r"\b(?:ytd|mtd|yesterday|today|tomorrow)\b",
    re.IGNORECASE,
)
# A quarter on its own -- Q3, or the third quarter -- is relative: it names a different
# quarter in each year it is read. A quarter a year anchors is a fixed date.
_QUARTER = re.compile(r"\b(?:q[1-4]|(?:first|second|third|fourth)\s+quarter)\b", re.IGNORECASE)
# A year that pins a quarter: 2024, FY24 or FY2024, or '24 after it.
_YEAR = r"(?:(?:19|20)\d{2}|FY\s?\d{2}(?:\d{2})?)"
_YEAR_AFTER = re.compile(rf"\s*(?:(?:of|in)\s+|[-/,]\s*)?(?:{_YEAR}|'\d{{2}})\b", re.IGNORECASE)
_YEAR_BEFORE = re.compile(rf"(?:^|\W){_YEAR}\s*[-/]?\s*$", re.IGNORECASE)
# The SQL functions and keywords that read the session's clock.
_SQL = re.compile(
    r"\b(?:CURRENT_DATE|CURRENT_TIMESTAMP|CURRENT_TIME|LOCALTIMESTAMP|LOCALTIME|SYSDATE|"
    r"SYSTIMESTAMP|GETDATE|NOW)\b",
    re.IGNORECASE,
)
_SQL_NOISE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|/\*.*?\*/", re.DOTALL)


def relative_date(text: str) -> str | None:
    """Return the first relative date in prose, as written; None when the text has none.

    A relative date is a phrase such as "last month", "this year", "YTD" or "yesterday", or
    a quarter no year pins: "Q3" and "the third quarter" are relative, "Q3 2024", "2024-Q3",
    "FY24 Q3" and "the third quarter of 2024" are not. A `q1` inside an identifier such as
    `q1_total` is no quarter at all.

    Example:
        relative_date("Revenue in Q3?") == "Q3"; relative_date("Revenue in Q3 2024?") is None.
    """
    found = [match for match in (_PHRASE.search(text),) if match is not None]
    found.extend(match for match in _QUARTER.finditer(text) if not _anchored(text, match))
    first = min(found, key=lambda match: match.start(), default=None)
    return first.group(0) if first is not None else None


def _anchored(text: str, quarter: re.Match[str]) -> bool:
    """Report whether a year written beside the quarter pins it, before it or after it."""
    return bool(_YEAR_AFTER.match(text, quarter.end()) or _YEAR_BEFORE.search(text[: quarter.start()]))


def sql_relative_date(sql: str) -> str | None:
    """Return the first clock-reading function in SQL, as written; None when it reads none.

    String literals, quoted identifiers and comments are skipped, so `'CURRENT_DATE'` and
    `-- as of CURRENT_DATE` read no clock.
    """
    match = _SQL.search(_SQL_NOISE.sub(" ", sql))
    return match.group(0) if match is not None else None
