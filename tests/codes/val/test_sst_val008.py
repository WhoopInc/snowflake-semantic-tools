"""SST-VAL008: a multi-line string uses a folded scalar, which reflows its lines."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import VIEWS, edited, found

LITERAL = "  - name: jaffle_sales\n    description: |-\n"


def test_sst_val008_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, VIEWS, LITERAL, "  - name: jaffle_sales\n    description: >-\n")
    [diagnostic] = found(project, "SST-VAL008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view 'jaffle_sales': 'description' uses a folded scalar"
    assert diagnostic.subject == "semantic_view:jaffle_sales"


def test_sst_val008_silent(tmp_path: Path) -> None:
    assert found(edited(tmp_path, VIEWS, LITERAL, LITERAL), "SST-VAL008") == []
