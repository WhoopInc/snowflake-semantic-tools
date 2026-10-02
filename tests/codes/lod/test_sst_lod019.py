"""SST-LOD019: a sidecar file a document names exists and is empty."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found

VERIFIED_QUERY = (
    "snowflake_verified_queries:\n"
    "  - name: how_many\n"
    "    question: How many?\n"
    "    sql_file: sql/how_many.sql\n"
    "    tables: [\"{{ ref('products') }}\"]\n"
)
QUERIES = "semantic_models/verified_queries"


def test_sst_lod019_fires(tmp_path: Path) -> None:
    files = {f"{QUERIES}/vq.yml": VERIFIED_QUERY, f"{QUERIES}/sql/how_many.sql": ""}
    [diagnostic] = found(SmallProject(tmp_path, files=files).load(), "SST-LOD019")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"sql/how_many.sql, referenced by {QUERIES}/vq.yml, is empty"
    assert diagnostic.subject == "verified_query:how_many"


def test_sst_lod019_silent(tmp_path: Path) -> None:
    files = {f"{QUERIES}/vq.yml": VERIFIED_QUERY, f"{QUERIES}/sql/how_many.sql": "SELECT 1\n"}
    assert found(SmallProject(tmp_path, files=files).load(), "SST-LOD019") == []
