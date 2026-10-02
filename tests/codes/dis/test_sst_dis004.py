"""SST-DIS004: a discovered file cannot be read."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity


def models(project: Path, *files: str) -> Path:
    root = project / "semantic_models"
    root.mkdir()
    for name in files:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("snowflake_metrics: []\n", encoding="utf-8")
    return root


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs POSIX permissions and a non-root user")
def test_sst_dis004_fires(tmp_path: Path) -> None:
    root = models(tmp_path, "metrics.yml", "secret.yml")
    (root / "secret.yml").chmod(0)
    try:
        found = discover_yaml(tmp_path, "semantic_models")
    finally:
        (root / "secret.yml").chmod(0o600)
    [diagnostic] = found.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS004", Severity.ERROR)
    assert diagnostic.message == "semantic_models/secret.yml is not readable"
    assert [item.path for item in found.files] == ["semantic_models/metrics.yml"]


def test_sst_dis004_silent(tmp_path: Path) -> None:
    models(tmp_path, "metrics.yml", "secret.yml")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()
