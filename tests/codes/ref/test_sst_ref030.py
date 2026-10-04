"""SST-REF030: a custom instruction's text names a filter, by `filter()`, that is not declared."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load

FILE = "semantic_models/custom_instructions/custom_instructions.yml"
BEFORE = "using the is_completed_order filter"


def test_sst_ref030_fires(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "using the {{ filter('is_complete_order') }} filter"))
    [diagnostic] = coded(project.diagnostics, "SST-REF030")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ filter('is_complete_order') } does not resolve"
    assert diagnostic.subject == "custom_instruction:jaffle_sql_conventions"


def test_sst_ref030_silent(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "using the {{ filter('is_completed_order') }} filter"))
    assert coded(project.diagnostics, "SST-REF030") == []
