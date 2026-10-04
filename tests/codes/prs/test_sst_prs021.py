"""SST-PRS021: a relationship uses the removed 0.3 column shape rather than `relationship_conditions`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import KEY, RELATIONSHIPS, SmallProject, relationship_file

LEGACY = (
    "snowflake_relationships:\n  - name: self_join\n    left_table: products\n    right_table: products\n"
    "    relationship_columns:\n      - left_column: products_id\n        right_column: products_id\n"
)


def test_sst_prs021_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files={RELATIONSHIPS: LEGACY}).load().diagnostics, "SST-PRS021")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "relationship:self_join: relationship_columns / left_column / right_column is not supported"
    )
    assert diagnostic.subject == "relationship:self_join"


def test_sst_prs021_silent(tmp_path: Path) -> None:
    files = relationship_file(f"{KEY} = {KEY}")
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS021") == []
