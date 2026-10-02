"""SST-LOD021: a semantic-model document has root keys, and no registered type owns any of them."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import SmallProject, found

FILE = "semantic_models/metrics/m.yml"


def test_sst_lod021_fires(tmp_path: Path) -> None:
    [diagnostic] = found(SmallProject(tmp_path, files={FILE: "models:\n  - name: x\n"}).load(), "SST-LOD021")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{FILE} declares no recognised root key"
    assert diagnostic.origin == Origin(FILE)


def test_sst_lod021_silent(tmp_path: Path) -> None:
    # A document with one recognised root key is not refused; its other keys warn instead.
    project = SmallProject(tmp_path, files={FILE: "snowflake_metrics: []\nextras: 1\n"}).load()
    assert found(project, "SST-LOD021") == []
