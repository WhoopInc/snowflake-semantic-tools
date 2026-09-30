"""SQL expression shape checks that need no SQL parser.

SST does not parse SQL. These answer the few structural questions it must --
whether an expression is wrapped in redundant parentheses, what its root
function is, whether it is a predicate -- and they are shared by validation and
by the `sst migrate refs` codemod, so both classify a filter the same way.
"""

from __future__ import annotations

import re


def outer_parentheses(text: str) -> bool:
    if not text.startswith("(") or not text.endswith(")"):
        return False
    depth = 0
    quote: str | None = None
    for index, character in enumerate(text):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in ("'", '"'):
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0 and index != len(text) - 1:
                return False
    return depth == 0 and quote is None


def root_function(expression: str) -> str | None:
    text = expression.strip()
    while outer_parentheses(text):
        text = text[1:-1].strip()
    match = re.match(r"^([A-Za-z_][A-Za-z0-9_$]*)\s*\(", text)
    if match is None:
        return None
    depth = 0
    quote: str | None = None
    end = -1
    for index in range(match.end() - 1, len(text)):
        character = text[index]
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in ("'", '"'):
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                end = index
                break
    if end < 0 or text[end + 1 :].strip():
        return None
    return match.group(1).upper()


def call_arguments(expression: str) -> tuple[str, ...] | None:
    """The top-level arguments of the one function call `expression` is, or None if it is not one call."""
    if root_function(expression) is None:
        return None
    text = expression.strip()
    while outer_parentheses(text):
        text = text[1:-1].strip()
    # `root_function` has checked that the call's closing parenthesis ends the text.
    inner = text[text.index("(") + 1 : -1]
    arguments: list[str] = []
    depth = 0
    quote: str | None = None
    start = 0
    for index, character in enumerate(inner):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in ("'", '"'):
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            arguments.append(inner[start:index].strip())
            start = index + 1
    arguments.append(inner[start:].strip())
    return tuple(arguments)


def is_boolean_expression(expression: str) -> bool:
    text = expression.strip()
    while outer_parentheses(text):
        text = text[1:-1].strip()
    if text.upper() in {"TRUE", "FALSE"}:
        return True
    if re.match(r"(?is)^NOT\s+.+$", text):
        return True
    if re.match(r"(?is)^EXISTS\s*\(", text):
        return True
    root = root_function(text)
    if root in {
        "BOOLAND",
        "BOOLOR",
        "BOOLXOR",
        "COALESCE",
        "EQUAL_NULL",
        "IS_BOOLEAN",
        "REGEXP_LIKE",
        "RLIKE",
    }:
        return True
    return bool(
        re.search(
            r"(?:=|<>|!=|<=|>=|<|>|\bBETWEEN\b|\bIN\s*\(|\bIS\s+(?:NOT\s+)?NULL\b|\bLIKE\b|\bRLIKE\b)",
            text,
            re.IGNORECASE,
        )
    )


# Snowflake's aggregate functions. A table-scoped metric must aggregate every
# column it reads with one of these.
AGGREGATE_FUNCTIONS = frozenset(
    (
        "ANY_VALUE",
        "APPROX_COUNT_DISTINCT",
        "APPROX_PERCENTILE",
        "APPROX_TOP_K",
        "ARRAY_AGG",
        "ARRAY_UNION_AGG",
        "ARRAY_UNIQUE_AGG",
        "AVG",
        "BITAND_AGG",
        "BITOR_AGG",
        "BITXOR_AGG",
        "BOOLAND_AGG",
        "BOOLOR_AGG",
        "BOOLXOR_AGG",
        "CORR",
        "COUNT",
        "COUNT_IF",
        "COVAR_POP",
        "COVAR_SAMP",
        "HASH_AGG",
        "HLL",
        "KURTOSIS",
        "LISTAGG",
        "MAX",
        "MAX_BY",
        "MEDIAN",
        "MIN",
        "MIN_BY",
        "MODE",
        "OBJECT_AGG",
        "PERCENTILE_CONT",
        "PERCENTILE_DISC",
        "REGR_AVGX",
        "REGR_AVGY",
        "REGR_COUNT",
        "REGR_INTERCEPT",
        "REGR_R2",
        "REGR_SLOPE",
        "REGR_SXX",
        "REGR_SXY",
        "REGR_SYY",
        "SKEW",
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "SUM",
        "VAR_POP",
        "VAR_SAMP",
        "VARIANCE",
        "VARIANCE_POP",
        "VARIANCE_SAMP",
    )
)
_LITERAL_OR_TEMPLATE = re.compile(r"'(?:[^']|'')*'|\{\{.*?\}\}", re.DOTALL)
_COLUMN_REF = re.compile(r"\{\{\s*ref\(\s*['\"][^'\"]+['\"]\s*,\s*['\"][^'\"]+['\"]\s*\)\s*\}\}")
_METRIC_REF = re.compile(r"\{\{\s*metric\(")
_CALL = re.compile(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(")
_WINDOW = re.compile(r"\)\s*OVER\s*\(", re.IGNORECASE)
_WITHIN_GROUP = re.compile(r"\s*WITHIN\s+GROUP\s*\(", re.IGNORECASE)


def is_aggregate_expression(expression: str) -> bool:
    """Whether a table-scoped metric aggregates every column it reads.

    Aggregates compose: `SUM(a) / NULLIF(SUM(b), 0)`, `ROUND(AVG(x), 2)`, and
    `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` all qualify, as does an
    expression over other metrics. A window (`... OVER (...)`) never does -- that
    belongs in the dbt model -- and neither does a column reference outside every
    aggregate call. String literals and templates are masked first, so a literal
    such as `'overall'` is not read as a window.
    """
    masked = _LITERAL_OR_TEMPLATE.sub(lambda match: "_" * len(match.group(0)), expression)
    if _WINDOW.search(masked):
        return False
    spans = _aggregate_spans(masked)
    if not spans and not _METRIC_REF.search(expression):
        return False
    return all(
        any(start <= match.start() and match.end() <= end for start, end in spans)
        for match in _COLUMN_REF.finditer(expression)
    )


def _aggregate_spans(masked: str) -> tuple[tuple[int, int], ...]:
    spans: list[tuple[int, int]] = []
    for call in _CALL.finditer(masked):
        if call.group(1).upper() not in AGGREGATE_FUNCTIONS:
            continue
        end = _close(masked, call.end() - 1)
        within = _WITHIN_GROUP.match(masked, end)
        if within is not None:
            end = _close(masked, within.end() - 1)
        spans.append((call.start(), end))
    return tuple(spans)


def _close(masked: str, opening: int) -> int:
    """The index just past the parenthesis that closes the one at `opening`."""
    depth = 0
    for index in range(opening, len(masked)):
        if masked[index] == "(":
            depth += 1
        elif masked[index] == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(masked)
