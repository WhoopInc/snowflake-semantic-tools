"""SST-LOD007: a semantic-model file is larger than SST reads."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.text_checks import MAX_FILE_BYTES
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject

BIG = "semantic_models/metrics/big.yml"


def test_sst_lod007_fires(tmp_path: Path) -> None:
    text = "snowflake_metrics: []\n# " + "x" * MAX_FILE_BYTES + "\n"
    [diagnostic] = coded(SmallProject(tmp_path, files={BIG: text}).load().diagnostics, "SST-LOD007")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{BIG} is {len(text)} bytes, over the {MAX_FILE_BYTES}-byte limit"
    assert diagnostic.origin == Origin(BIG)


def test_sst_lod007_silent(tmp_path: Path) -> None:
    text = "snowflake_metrics: []\n# " + "x" * 1000 + "\n"
    assert coded(SmallProject(tmp_path, files={BIG: text}).load().diagnostics, "SST-LOD007") == []
