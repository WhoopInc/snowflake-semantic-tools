"""SST-VAL019: an instruction's prose names a metric or filter that does not resolve."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import INSTRUCTIONS, edited, reported

LINE = "      Round every monetary amount to two decimal places.\n"


def test_sst_val019_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, INSTRUCTIONS, LINE, LINE + "      Prefer the gross_profit metric for margin.\n")
    [diagnostic] = reported(project, "SST-VAL019")
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == "custom_instruction 'jaffle_sql_conventions': prose names 'metric gross_profit', which does not resolve"
    )
    assert diagnostic.subject == "custom_instruction:jaffle_sql_conventions"


def test_sst_val019_silent(tmp_path: Path) -> None:
    project = edited(tmp_path, INSTRUCTIONS, LINE, LINE + "      Prefer the gross_margin metric for margin.\n")
    assert reported(project, "SST-VAL019") == []
