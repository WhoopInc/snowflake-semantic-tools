"""SST-DIS014: the semantic-model directory exists but holds no `semantic_views/` folder."""

from __future__ import annotations

import shutil
from pathlib import Path

from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import project_copy


def test_sst_dis014_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    shutil.move(project / "semantic_models" / "semantic_views", project / "semantic_models" / "core")
    code, diagnostics = validate_json(project)
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-DIS014"]
    assert code != 0 and diagnostic["severity"] == "error"
    assert diagnostic["message"] == (
        "semantic_models/semantic_views does not exist, and SST reads semantic views only from it"
    )
    assert "SST-DIS001" not in [item["code"] for item in diagnostics]


def test_sst_dis014_silent(tmp_path: Path) -> None:
    _, diagnostics = validate_json(project_copy(tmp_path))
    assert "SST-DIS014" not in [item["code"] for item in diagnostics]
