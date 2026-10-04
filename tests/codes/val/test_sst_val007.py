"""SST-VAL007: a declared member has no consumer anywhere in the project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import INSTRUCTIONS, edited, reported

ANCHOR = "snowflake_custom_instructions:\n"
ORPHAN = "  - name: orphan_guidance\n    ai_sql_generation: |-\n      Prefer whole numbers.\n"


def test_sst_val007_fires(tmp_path: Path) -> None:
    [diagnostic] = reported(edited(tmp_path, INSTRUCTIONS, ANCHOR, ANCHOR + ORPHAN), "SST-VAL007")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "custom_instruction 'orphan_guidance' is referenced by nothing"
    assert diagnostic.subject == "custom_instruction:orphan_guidance"


def test_sst_val007_silent(tmp_path: Path) -> None:
    assert reported(edited(tmp_path, INSTRUCTIONS, ANCHOR, ANCHOR), "SST-VAL007") == []
