"""Rules about column metadata that validation and `sst enrich` share.

Validation reports a value that breaks one of these rules, and enrich applies the same
rules before it writes, so it never writes a value validation would then report.
"""

from __future__ import annotations

import re

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

# Characters a synonym may not hold: Snowflake quotes each synonym in the DDL it renders.
_SYNONYM_QUOTES = ("'", '"')


def base_type(data_type: str | None) -> str:
    """Return a data type upper-cased, without its parameters: `number(38, 0)` is `NUMBER`."""
    return re.sub(r"\s*\(.*\)\s*$", "", (data_type or "").strip().upper())


def is_numeric(data_type: str | None) -> bool:
    """Report whether a data type is one of Snowflake's numeric types."""
    return base_type(data_type) in NUMERIC_TYPES


def is_temporal(data_type: str | None) -> bool:
    """Report whether a data type is one of Snowflake's date, time, or timestamp types."""
    return base_type(data_type) in TEMPORAL_TYPES


def is_sentinel(value: str) -> bool:
    """Report whether a sample value is a missing value written as text, such as `nan`."""
    return value.casefold() in SAMPLE_SENTINELS


def synonym_problem(synonym: str) -> str | None:
    """Return what makes a synonym unusable, as SST-PRS030 words it; None when it is usable."""
    return "quotes" if any(character in synonym for character in _SYNONYM_QUOTES) else None
