"""SST-DIS003: the semantic-models directory holds no YAML file."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dis_codes import models


def test_sst_dis003_fires(tmp_path: Path) -> None:
    root = models(tmp_path)
    (root / "README.md").write_text("notes", encoding="utf-8")
    [diagnostic] = discover_yaml(tmp_path, "semantic_models").diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS003", Severity.WARNING)
    assert diagnostic.message == "no candidate files under semantic_models"


def test_sst_dis003_silent(tmp_path: Path) -> None:
    models(tmp_path, "views/views.yml")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()
