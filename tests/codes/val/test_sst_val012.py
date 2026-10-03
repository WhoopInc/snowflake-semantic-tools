"""SST-VAL012: a custom instruction writes a 0.3 key spelling, which SST still honours."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import INSTRUCTIONS, edited, found, load

CURRENT = "    ai_question_categorization: |-\n"


def test_sst_val012_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, INSTRUCTIONS, CURRENT, "    question_categorization: |-\n")
    [diagnostic] = found(project, "SST-VAL012")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "custom_instruction 'jaffle_question_scope' uses 'question_categorization'; "
        "the current spelling is 'ai_question_categorization'"
    )
    assert diagnostic.subject == "custom_instruction:jaffle_question_scope"
    sales = next(view for view in load(project).views if view.fqn.endswith(".JAFFLE_SALES"))
    assert sales.ai_question_categorization is not None


def test_sst_val012_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, INSTRUCTIONS, CURRENT, CURRENT), "SST-VAL012") == []
