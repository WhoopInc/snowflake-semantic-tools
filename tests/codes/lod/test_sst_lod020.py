"""SST-LOD020: a file is read under a tolerated spelling of its extension, such as `.YML`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import SmallProject, found


def test_sst_lod020_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"semantic_models/metrics/m.YML": "snowflake_metrics: []\n"}).load()
    [diagnostic] = found(project, "SST-LOD020")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_models/metrics/m.YML uses '.YML'"
    assert diagnostic.origin == Origin("semantic_models/metrics/m.YML")


def test_sst_lod020_silent(tmp_path: Path) -> None:
    files = {"semantic_models/metrics/m.yml": "snowflake_metrics: []\n"}
    assert found(SmallProject(tmp_path, files=files).load(), "SST-LOD020") == []
