"""SST-CFG001: there is no configuration file where the run looks for one."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_cfg001_fires(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        locate_project(tmp_path)
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG001", Severity.ERROR)
    assert diagnostic.message == f"no sst_config.yml at {tmp_path}"
    assert diagnostic.subject == "config:discovery"


def test_sst_cfg001_fires_for_an_explicit_config_that_does_not_exist(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        locate_project(tmp_path, tmp_path / "elsewhere.yml")
    assert [(item.code, item.subject) for item in raised.value.diagnostics] == [("SST-CFG001", "config:--config")]


def test_sst_cfg001_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yaml").write_text("project: {}\n", encoding="utf-8")
    assert locate_project(tmp_path).config_file == tmp_path / "sst_config.yaml"
    assert locate_project(tmp_path / "missing", required=False).config_file is None
