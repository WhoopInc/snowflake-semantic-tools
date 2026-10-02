"""SST-LOD200: a file uses `.yaml`, which SST reads as it reads `.yml`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found


def test_sst_lod200_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"semantic_models/metrics/m.yaml": "snowflake_metrics: []\n"}).load()
    [diagnostic] = found(project, "SST-LOD200")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "semantic_models/metrics/m.yaml uses .yaml; accepted"
    assert diagnostic.subject is None


def test_sst_lod200_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-LOD200") == []
