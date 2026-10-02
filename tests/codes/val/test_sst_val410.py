"""SST-VAL410: two blocks one view attaches contradict each other."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.semantic_edits import INSTRUCTIONS, reported


def _directives(tmp_path: Path, scope_rule: str) -> Path:
    project = project_copy(tmp_path)
    path = project / INSTRUCTIONS
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "      Round every monetary amount to two decimal places.\n",
        "      Round every monetary amount to two decimal places.\n" + "      Always show the location name.\n",
        1,
    )
    text = text.replace(
        "      These views answer questions about orders, the customers who placed them,\n",
        scope_rule + "      These views answer questions about orders, the customers who placed them,\n",
        1,
    )
    path.write_text(text, encoding="utf-8")
    return project


def test_sst_val410_fires(tmp_path: Path) -> None:
    project = _directives(tmp_path, "      Never show the location name.\n")
    [diagnostic] = reported(project, "SST-VAL410")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:jaffle_sales: 'jaffle_question_scope' and 'jaffle_sql_conventions' give contradictory directives"
    )
    assert diagnostic.subject == "semantic_view:jaffle_sales"


def test_sst_val410_silent(tmp_path: Path) -> None:
    project = _directives(tmp_path, "      Always show the location name.\n")
    assert reported(project, "SST-VAL410") == []
