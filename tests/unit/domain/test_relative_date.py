"""The relative-date detectors: prose phrases and bare quarters, and SQL that reads the clock."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.validate.relative_date import relative_date, sql_relative_date


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("How many orders last month?", "last month"),
        ("Revenue this year by location", "this year"),
        ("YTD revenue", "YTD"),
        ("Orders placed yesterday", "yesterday"),
        ("Revenue in Q3?", "Q3"),
        ("Revenue in the third quarter", "third quarter"),
        # The earlier relative date wins, whichever kind it is.
        ("Sum q2 and Q3 2024 against last year", "q2"),
    ],
)
def test_a_relative_date_is_found_as_written(text: str, expected: str) -> None:
    assert relative_date(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "Revenue in Q3 2024?",
        "Revenue in 2024-Q3",
        "Revenue in 2024 Q1",
        "Revenue in FY24 Q3",
        "Revenue in Q3 FY2024",
        "Revenue in Q3 '24",
        "Revenue in Q4 of 2023",
        "Revenue in Q3, 2024",
        "Revenue in the third quarter of 2024",
        # A quarter token inside an identifier is no quarter at all.
        "Sum q1_total by region",
        "Revenue between January and March of 2026",
    ],
)
def test_a_pinned_date_or_an_identifier_is_not_relative(text: str) -> None:
    assert relative_date(text) is None


def test_sql_that_reads_the_clock_names_the_function() -> None:
    assert sql_relative_date("SELECT * FROM t WHERE d > CURRENT_DATE - 7") == "CURRENT_DATE"
    assert sql_relative_date("select dateadd(day, -1, getdate())") == "getdate"


def test_sql_literals_identifiers_and_comments_read_no_clock() -> None:
    assert sql_relative_date("SELECT 'CURRENT_DATE', \"NOW\" FROM t -- as of SYSDATE\n/* GETDATE */") is None
