"""SST-VAL414: a verified query's SQL reads a table it does not name with ref()."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.semantic.expressions import sql_tables
from tests.helpers.reference_project import edited, member_file, project_copy, reported


def test_sst_val414_fires(tmp_path: Path) -> None:
    joined = "    FROM {{ ref('orders') }} AS orders\n    CROSS JOIN customers AS customers\n    GROUP BY ALL\n"
    [diagnostic] = reported(
        edited(
            tmp_path,
            member_file("verified_queries", "order_count_by_state"),
            "    FROM {{ ref('orders') }} AS orders\n    GROUP BY ALL\n    ORDER BY order_count DESC\n",
            joined,
        ),
        "SST-VAL414",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "verified_query 'order_count_by_state' queries 'SST_REF_DEV.JAFFLE.CUSTOMERS' without ref()"
    )
    assert diagnostic.subject == "verified_query:order_count_by_state"


def test_sst_val414_silent(tmp_path: Path) -> None:
    # `revenue_by_location` joins locations through ref().
    assert reported(project_copy(tmp_path), "SST-VAL414") == []


def test_sst_val414_silent_for_a_from_that_reads_no_table() -> None:
    sql = (
        "WITH c AS (SELECT 1 FROM {{ ref('a') }} WHERE x IS DISTINCT FROM TRUE)\n"
        "SELECT EXTRACT(YEAR FROM DATE(c.d)), TRIM(LEADING 'x' FROM c.y)\n"
        "FROM c LEFT JOIN {{ ref('b') }} AS bc ON c.p IS NOT DISTINCT FROM bc.p\n"
        "JOIN raw_t ON 1 = 1 WHERE c.z IN (SELECT id FROM other)\n"
    )
    assert sql_tables(sql) == ("raw_t", "other")
