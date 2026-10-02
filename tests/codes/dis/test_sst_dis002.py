"""SST-DIS002: the semantic-models path is a file."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity


def models(project: Path, *files: str) -> Path:
    root = project / "semantic_models"
    root.mkdir()
    for name in files:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("snowflake_metrics: []\n", encoding="utf-8")
    return root


def test_sst_dis002_fires(tmp_path: Path) -> None:
    (tmp_path / "semantic_models").write_text("", encoding="utf-8")
    with pytest.raises(ProjectError) as caught:
        discover_yaml(tmp_path, "semantic_models")
    [diagnostic] = caught.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS002", Severity.ERROR)
    assert diagnostic.message == "semantic_models is not a directory"


def test_sst_dis002_silent(tmp_path: Path) -> None:
    models(tmp_path, "metrics.yml")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()
