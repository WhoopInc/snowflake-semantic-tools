"""SST-LOD018: a document names a sidecar file that does not exist."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject

VERIFIED_QUERY = (
    "snowflake_verified_queries:\n"
    "  - name: how_many\n"
    "    question: How many?\n"
    "    sql_file: sql/how_many.sql\n"
    "    tables: [\"{{ ref('products') }}\"]\n"
)
QUERIES = "semantic_models/verified_queries"


def test_sst_lod018_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={f"{QUERIES}/vq.yml": VERIFIED_QUERY}).load()
    [diagnostic] = coded(project.diagnostics, "SST-LOD018")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{QUERIES}/vq.yml references sql/how_many.sql, which does not exist"
    assert diagnostic.subject == "verified_query:how_many"
    assert diagnostic.origin == Origin(f"{QUERIES}/vq.yml", 2, 5)


def test_sst_lod018_silent(tmp_path: Path) -> None:
    files = {f"{QUERIES}/vq.yml": VERIFIED_QUERY, f"{QUERIES}/sql/how_many.sql": "SELECT 1\n"}
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-LOD018") == []
