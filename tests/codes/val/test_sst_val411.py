"""SST-VAL411: an instruction uses Cortex Analyst state keywords."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.reference_project import INSTRUCTIONS, edited, reported


def test_sst_val411_fires(tmp_path: Path) -> None:
    rule = "      Answer UNCLEAR when the question names no location.\n"
    project = edited(
        tmp_path,
        INSTRUCTIONS,
        "      These views answer questions about orders, the customers who placed them,\n",
        rule + "      These views answer questions about orders, the customers who placed them,\n",
    )
    [diagnostic] = reported(project, "SST-VAL411")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "custom_instruction 'jaffle_question_scope' uses UNCLEAR keywords, which an agent does not need"
    )
    assert diagnostic.subject == "custom_instruction:jaffle_question_scope"


def test_sst_val411_silent(tmp_path: Path) -> None:
    rule = "      Ask which location is meant when the question names none.\n"
    assert (
        reported(
            edited(
                tmp_path,
                INSTRUCTIONS,
                "      These views answer questions about orders, the customers who placed them,\n",
                rule + "      These views answer questions about orders, the customers who placed them,\n",
            ),
            "SST-VAL411",
        )
        == []
    )
