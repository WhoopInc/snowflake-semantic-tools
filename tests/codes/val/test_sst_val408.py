"""SST-VAL408: a view's `custom_instructions:` is the legacy bare string."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.semantic_edits import VIEWS, edited, reported

LIST = (
    "    custom_instructions:\n"
    "      - \"{{ custom_instructions('jaffle_sql_conventions') }}\"\n"
    "      - \"{{ custom_instructions('jaffle_question_scope') }}\"\n"
)


def test_sst_val408_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, VIEWS, LIST, "    custom_instructions: Round money to cents.\n")
    [diagnostic] = reported(project, "SST-VAL408")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "custom_instruction 'jaffle_sales' would render as a bare string"
    assert diagnostic.subject == "semantic_view:jaffle_sales"


def test_sst_val408_silent(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-VAL408") == []
