"""SST-DIS008: a semantic-model file holds no root key a registered type owns."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.semantic import read_semantic_inputs
from snowflake_semantic_tools.domain.diagnostics import Severity


def project(tmp_path: Path, text: str) -> Path:
    (tmp_path / "sst_config.yml").write_text("project: {}\n", encoding="utf-8")
    (tmp_path / "semantic_models").mkdir()
    (tmp_path / "semantic_models" / "a.yml").write_text(text, encoding="utf-8")
    return tmp_path


def test_sst_dis008_fires(tmp_path: Path) -> None:
    inputs = read_semantic_inputs(project(tmp_path, "dashboards: []\n"))
    [diagnostic] = inputs.documents.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS008", Severity.WARNING)
    assert diagnostic.message == "semantic_models/a.yml matches no registered artifact type"


def test_sst_dis008_silent(tmp_path: Path) -> None:
    assert read_semantic_inputs(project(tmp_path, "snowflake_metrics: []\n")).documents.diagnostics == ()
