"""Read custom-instruction prose for the rules a channel's text can break.

A custom instruction carries two channels: `ai_sql_generation` shapes the SQL, and
`ai_question_categorization` decides whether to answer at all. Each function here reads the
text of one channel, or of two blocks, and reports what it finds; the semantic load decides
which members and views to ask about.
"""

from __future__ import annotations

import re

SQL_CHANNEL = "ai_sql_generation"
CATEGORIZATION_CHANNEL = "ai_question_categorization"

# Wording that decides whether to answer -- inert in the SQL channel.
_CATEGORIZATION_RULE = re.compile(
    r"\b(?:out\s+of\s+scope|off[-\s]topic|ask\s+(?:the\s+user\s+)?(?:for\s+)?clarif\w*|clarifying\s+question|"
    r"(?:decline|refuse)\s+to\s+answer|do\s+not\s+answer|don't\s+answer|reject\s+(?:the\s+)?question|"
    r"categori[sz]e\s+(?:the\s+)?question|classify\s+(?:the\s+)?question)\b",
    re.IGNORECASE,
)
# Wording that shapes the SQL -- inert in the categorization channel.
_SQL_RULE = re.compile(
    r"\b(?:round(?:ed|ing)?\s+to|group\s+by|order\s+by|date_trunc|in\s+the\s+sql|"
    r"the\s+(?:query|sql)\s+(?:should|must)|join\s+(?:on|to|with)|where\s+clause)\b",
    re.IGNORECASE,
)
# The state words Cortex Analyst categorization answers with; an agent reads plain prose.
_STATE_KEYWORD = re.compile(r"\b(?:UNCLEAR|AMBIGUOUS|UNAMBIGUOUS_SQL|UNANSWERABLE|OUT_OF_SCOPE)\b")
_DIRECTIVE = re.compile(
    r"\b(?P<word>always|never|must\s+not|must|do\s+not|don't|should\s+not|shouldn't|should)\s+(?P<body>[^.;!?\n]+)",
    re.IGNORECASE,
)
_NEGATIVE = frozenset(("never", "must not", "do not", "don't", "should not", "shouldn't"))


def misplaced_rule(text: str, channel: str) -> str | None:
    """Name the kind of rule the channel's text holds that only the other channel acts on.

    Args:
        channel: `SQL_CHANNEL` or `CATEGORIZATION_CHANNEL`, the channel `text` was written in.

    Returns:
        "question categorization" for a rule about whether to answer written in the SQL
        channel, "sql generation" for a rule shaping the SQL written in the categorization
        channel; None when the text holds neither.
    """
    if channel == SQL_CHANNEL:
        return "question categorization" if _CATEGORIZATION_RULE.search(text) else None
    return "sql generation" if _SQL_RULE.search(text) else None


def state_keywords(text: str) -> tuple[str, ...]:
    """Return each Cortex Analyst state keyword the text uses, uppercase as written, once each in order."""
    return tuple(dict.fromkeys(match.group(0) for match in _STATE_KEYWORD.finditer(text)))


def contradicts(first: str, second: str) -> bool:
    """Report whether one text directs what the other forbids.

    A directive is "always", "must" or "should", or its negation "never", "must not",
    "do not" or "should not", followed by the rest of its clause. Two directives contradict
    when their clauses read the same, ignoring case, punctuation and spacing, and exactly
    one is negative.

    Example:
        contradicts("Always round to cents.", "Never round to cents.") is True.
    """
    ours = _directives(first)
    return any((body, not negative) in ours for body, negative in _directives(second))


def _directives(text: str) -> frozenset[tuple[str, bool]]:
    return frozenset(
        (" ".join(re.findall(r"[a-z0-9_]+", match.group("body").casefold())), _is_negative(match.group("word")))
        for match in _DIRECTIVE.finditer(text)
    )


def _is_negative(word: str) -> bool:
    return " ".join(word.casefold().split()) in _NEGATIVE
