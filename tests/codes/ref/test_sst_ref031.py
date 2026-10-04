"""SST-REF031: a custom instruction's text names a verified query, by `verified_query()`, that is not declared."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load

FILE = "semantic_models/custom_instructions/custom_instructions.yml"
BEFORE = "using the is_completed_order filter"


def test_sst_ref031_fires(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "using the {{ verified_query('no_such_query') }} filter"))
    [diagnostic] = coded(project.diagnostics, "SST-REF031")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ verified_query('no_such_query') } does not resolve"
    assert diagnostic.subject == "custom_instruction:jaffle_sql_conventions"


def test_sst_ref031_silent(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "using the {{ verified_query('order_count_by_state') }} filter"))
    assert coded(project.diagnostics, "SST-REF031") == []
