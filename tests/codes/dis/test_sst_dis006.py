"""SST-DIS006: two discovered paths are one path under case folding."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml, fold_key
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dis_codes import models


def case_sensitive(directory: Path) -> bool:
    (directory / "probe").write_text("", encoding="utf-8")
    return not (directory / "PROBE").exists()


def test_sst_dis006_fires(tmp_path: Path) -> None:
    # A case-insensitive file system holds the two as one file, which is why CI on another
    # platform is where two of them can meet.
    assert fold_key("semantic_models/Straße.yml") == fold_key("semantic_models/strasse.yml")
    if not case_sensitive(tmp_path):
        pytest.skip("the file system folds case, so the two paths cannot both exist")
    models(tmp_path, "Straße.yml", "strasse.yml")
    found = discover_yaml(tmp_path, "semantic_models")
    [diagnostic] = found.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS006", Severity.ERROR)
    assert diagnostic.message == "semantic_models/Straße.yml and semantic_models/strasse.yml collide under case folding"
    assert [item.path for item in found.files] == ["semantic_models/Straße.yml"]


def test_sst_dis006_silent(tmp_path: Path) -> None:
    models(tmp_path, "strasse.yml", "street.yml")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()
