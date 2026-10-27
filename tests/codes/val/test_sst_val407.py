"""SST-VAL407: a custom instruction holds no text in either channel."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import INSTRUCTIONS, appended, reported


def test_sst_val407_fires(tmp_path: Path) -> None:
    project = appended(tmp_path, INSTRUCTIONS, "\n  - name: placeholder\n    description: Still to write.\n")
    [diagnostic] = reported(project, "SST-VAL407")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "custom_instruction 'placeholder' declares no non-empty channel"
    assert diagnostic.subject == "custom_instruction:placeholder"


def test_sst_val407_silent(tmp_path: Path) -> None:
    block = "\n  - name: placeholder\n    description: Written.\n    ai_sql_generation: Report counts.\n"
    assert reported(appended(tmp_path, INSTRUCTIONS, block), "SST-VAL407") == []
