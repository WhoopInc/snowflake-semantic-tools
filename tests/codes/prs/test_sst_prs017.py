"""SST-PRS017: a verified query's `verified_at` is neither an epoch nor an ISO-8601 timestamp."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject

QUERY = (
    "snowflake_verified_queries:\n  - name: how_many\n    question: How many?\n    sql: SELECT 1\n"
    "    tables: [\"{{{{ ref('products') }}}}\"]\n    verified_at: {value}\n"
)


def _project(tmp_path: Path, value: str) -> SmallProject:
    return SmallProject(tmp_path, files={"semantic_models/verified_queries/vq.yml": QUERY.format(value=value)})


def test_sst_prs017_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        _project(tmp_path, "'last tuesday'").load()
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRS017", Severity.ERROR)
    assert (
        diagnostic.message
        == "verified_query:how_many: 'verified_at' value 'last tuesday' is not an epoch or ISO timestamp"
    )
    assert diagnostic.subject == "verified_query:how_many"


def test_sst_prs017_silent(tmp_path: Path) -> None:
    _project(tmp_path, "'2026-09-28T10:00:00Z'").load()
    _project(tmp_path / "epoch", "1790000000").load()
