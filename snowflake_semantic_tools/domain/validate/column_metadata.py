"""Rules about column metadata that validation and `sst enrich` share.

Validation reports a value that breaks one of these rules, and enrich applies the same
rules before it writes, so it never writes a value validation would then report.
"""

from __future__ import annotations

import re
import unicodedata

NUMERIC_TYPES = frozenset(
    (
        "BIGINT",
        "BYTEINT",
        "DECIMAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "FLOAT",
        "INT",
        "INTEGER",
        "NUMBER",
        "NUMERIC",
        "REAL",
        "SMALLINT",
        "TINYINT",
    )
)
TEMPORAL_TYPES = frozenset(
    (
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
    )
)

# A sample value that is really a missing value written out as text, compared casefolded.
SAMPLE_SENTINELS = frozenset(("nan", "none", "null", "<na>"))

# Text a template engine would read. dbt renders the YAML it reads, and the YAML enrich writes,
# as Jinja, so a synonym or value holding one would run as code at the next parse; SST's own
# YAML would publish the template's text, not what it stands for.
_TEMPLATE_MARKERS = ("{{", "{%", "{#")
# Unicode categories no synonym needs, and a terminal or a reviewer may misread: control
# characters (newlines and tabs among them), format characters such as bidirectional
# overrides and zero-width spaces, private-use, surrogate and unassigned code points, and
# the line and paragraph separators.
_UNPRINTABLE = frozenset(("Cc", "Cf", "Co", "Cs", "Cn", "Zl", "Zp"))


def base_type(data_type: str | None) -> str:
    """Return a data type upper-cased, without its parameters: `number(38, 0)` is `NUMBER`."""
    return re.sub(r"\s*\(.*\)\s*$", "", (data_type or "").strip().upper())


def is_numeric(data_type: str | None) -> bool:
    """Report whether a data type is one of Snowflake's numeric types."""
    return base_type(data_type) in NUMERIC_TYPES


def is_temporal(data_type: str | None) -> bool:
    """Report whether a data type is one of Snowflake's date, time, or timestamp types."""
    return base_type(data_type) in TEMPORAL_TYPES


def is_boolean_type(data_type: str | None) -> bool:
    """Report whether a data type is Snowflake's BOOLEAN."""
    return base_type(data_type) == "BOOLEAN"


def is_sentinel(value: str) -> bool:
    """Report whether a sample value is a missing value written as text, such as `nan`."""
    return value.casefold() in SAMPLE_SENTINELS


def has_template_syntax(text: str) -> bool:
    """Report whether `text` opens a Jinja expression, statement or comment."""
    return any(marker in text for marker in _TEMPLATE_MARKERS)


def synonym_problem(synonym: str) -> str | None:
    """Return what makes a synonym unusable, as SST-PRS030 words it; None when it is usable.

    A quote is usable: the DDL escapes it, so the synonym reads as written.
    """
    if any(unicodedata.category(character) in _UNPRINTABLE for character in synonym):
        return "control characters"
    if has_template_syntax(synonym):
        return "template syntax"
    return None


def printable(text: str, limit: int = 80) -> str:
    """Return `text` safe to print in a diagnostic: unprintable characters escaped, then cut to `limit`.

    Each character `synonym_problem` calls a control character is written as its `\\uXXXX`
    escape, so a message can neither move the cursor nor hide what follows; a longer result
    ends in `...`.
    """
    escaped = "".join(
        f"\\u{ord(character):04x}" if unicodedata.category(character) in _UNPRINTABLE else character
        for character in text
    )
    return escaped if len(escaped) <= limit else escaped[: limit - 3] + "..."
