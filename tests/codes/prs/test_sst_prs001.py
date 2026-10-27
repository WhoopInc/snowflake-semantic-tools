"""SST-PRS001: a semantic-model document has a root key no registered type owns, beside one it does."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject

FILE = "semantic_models/metrics/m.yml"


def test_sst_prs001_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files={FILE: "snowflake_metrics: []\nextras: 1\n"}).load().diagnostics, "SST-PRS001"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "no registered type owns root key 'extras'"
    assert diagnostic.subject is None


def test_sst_prs001_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files={FILE: "snowflake_metrics: []\n"}).load().diagnostics, "SST-PRS001") == []
