"""SST-CFG005: both spellings of the configuration file exist, so which one configures the run is ambiguous."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_cfg005_fires(tmp_path: Path) -> None:
    for name in ("sst_config.yml", "sst_config.yaml"):
        (tmp_path / name).write_text("project: {}\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        locate_project(tmp_path, required=False)
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG005", Severity.ERROR)
    assert diagnostic.message == (
        f"2 candidate config files found; used {tmp_path / 'sst_config.yml'}, shadowed {tmp_path / 'sst_config.yaml'}"
    )
    assert diagnostic.subject == "config:discovery"


def test_sst_cfg005_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("project: {}\n", encoding="utf-8")
    assert locate_project(tmp_path).config_file == tmp_path / "sst_config.yml"
