"""SST-VAL409: a rule about whether to answer sits in the SQL channel."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import INSTRUCTIONS, edited, project_copy, reported


def test_sst_val409_fires(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        INSTRUCTIONS,
        "      Round every monetary amount to two decimal places.\n",
        "      Round every monetary amount to two decimal places.\n      A question about payroll is out of scope.\n",
    )
    [diagnostic] = reported(project, "SST-VAL409")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "custom_instruction 'jaffle_sql_conventions': a question categorization rule appears in the "
        "ai_sql_generation channel"
    )
    assert diagnostic.subject == "custom_instruction:jaffle_sql_conventions"


def test_sst_val409_silent(tmp_path: Path) -> None:
    # The scope block says "OUT OF SCOPE" too, in the channel that acts on it.
    assert reported(project_copy(tmp_path), "SST-VAL409") == []
