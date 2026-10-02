"""SST-VAL017: a custom instruction repeats a filter's predicate in prose."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import INSTRUCTIONS, edited, found

LINE = "      Round every monetary amount to two decimal places.\n"


def test_sst_val017_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path, INSTRUCTIONS, LINE, LINE + "      Count an order as done when order_state = 'completed'.\n"
    )
    [diagnostic] = found(project, "SST-VAL017")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "custom_instruction 'jaffle_sql_conventions' duplicates filter 'is_completed_order'"
    assert diagnostic.subject == "custom_instruction:jaffle_sql_conventions"


def test_sst_val017_silent(tmp_path: Path) -> None:
    # Naming the filter, as the fixture does, is the enforceable way to say it.
    assert found(edited(tmp_path, INSTRUCTIONS, LINE, LINE), "SST-VAL017") == []
