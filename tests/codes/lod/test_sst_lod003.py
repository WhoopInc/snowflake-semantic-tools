"""SST-LOD003: a semantic-model file holds no document, only whitespace or comments."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject


def test_sst_lod003_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"semantic_models/semantic_views/empty.yml": "# nothing yet\n"}).load()
    [diagnostic] = coded(project.diagnostics, "SST-LOD003")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_models/semantic_views/empty.yml is empty"
    assert diagnostic.subject is None


def test_sst_lod003_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path).load().diagnostics, "SST-LOD003") == []
