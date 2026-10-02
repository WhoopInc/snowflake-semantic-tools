"""SST-DIS003: the semantic-models directory holds no YAML file."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity


def models(project: Path, *files: str) -> Path:
    root = project / "semantic_models"
    root.mkdir()
    for name in files:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("snowflake_metrics: []\n", encoding="utf-8")
    return root


def test_sst_dis003_fires(tmp_path: Path) -> None:
    root = models(tmp_path)
    (root / "README.md").write_text("notes", encoding="utf-8")
    [diagnostic] = discover_yaml(tmp_path, "semantic_models").diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS003", Severity.WARNING)
    assert diagnostic.message == "no candidate files under semantic_models"


def test_sst_dis003_silent(tmp_path: Path) -> None:
    models(tmp_path, "views/views.yaml")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()
