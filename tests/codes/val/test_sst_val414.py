"""SST-VAL414: a verified query's SQL reads a table outside its `tables:`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import QUERIES, edited, project_copy, reported


def test_sst_val414_fires(tmp_path: Path) -> None:
    joined = "      FROM orders AS orders\n      CROSS JOIN customers AS customers\n      GROUP BY ALL\n"
    [diagnostic] = reported(
        edited(
            tmp_path,
            QUERIES,
            "      FROM orders AS orders\n      GROUP BY ALL\n      ORDER BY order_count DESC\n",
            joined,
        ),
        "SST-VAL414",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "verified_query 'order_count_by_state' queries 'SST_REF_DEV.JAFFLE.CUSTOMERS', absent from tables:"
    )
    assert diagnostic.subject == "verified_query:order_count_by_state"


def test_sst_val414_silent(tmp_path: Path) -> None:
    # `revenue_by_location` joins locations and lists it.
    assert reported(project_copy(tmp_path), "SST-VAL414") == []
