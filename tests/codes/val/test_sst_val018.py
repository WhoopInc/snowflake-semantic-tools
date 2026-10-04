"""SST-VAL018: a view's description and instructions exceed `validation.instruction_budget`."""

from __future__ import annotations

import re
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import CONFIG, edited, reported

STRICT = "  strict: false\n"


def test_sst_val018_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, CONFIG, STRICT, STRICT + "  instruction_budget: 100\n")
    diagnostic = next(item for item in reported(project, "SST-VAL018") if item.subject == "semantic_view:jaffle_sales")
    assert diagnostic.severity is Severity.WARNING
    assert re.fullmatch(
        r"semantic_view:jaffle_sales: composed instruction surface is \d+ chars, over 100", diagnostic.message
    )


def test_sst_val018_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, CONFIG, STRICT, STRICT + "  instruction_budget: 100000\n"), "SST-VAL018") == []
