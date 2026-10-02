"""SST-PRS001: a semantic-model document has a root key no registered type owns, beside one it does."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found

FILE = "semantic_models/metrics/m.yml"


def test_sst_prs001_fires(tmp_path: Path) -> None:
    [diagnostic] = found(
        SmallProject(tmp_path, files={FILE: "snowflake_metrics: []\nextras: 1\n"}).load(), "SST-PRS001"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "no registered type owns root key 'extras'"
    assert diagnostic.subject is None


def test_sst_prs001_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path, files={FILE: "snowflake_metrics: []\n"}).load(), "SST-PRS001") == []
