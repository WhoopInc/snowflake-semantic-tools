"""SST-CFG032: `--config` or `$SST_CONFIG` is used, and the project root holds a configuration file it shadows."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import project_copy


def test_sst_cfg032_fires(tmp_path: Path) -> None:
    explicit = tmp_path / "ci.yml"
    explicit.write_text("project: {}\n", encoding="utf-8")
    (tmp_path / "sst_config.yml").write_text("project: {}\n", encoding="utf-8")
    [diagnostic] = locate_project(tmp_path, explicit).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG032", Severity.WARNING)
    assert diagnostic.message == f"using {explicit}; shadowed {tmp_path / 'sst_config.yml'}"
    assert diagnostic.subject == "config:ci.yml"


def test_sst_cfg032_silent(tmp_path: Path) -> None:
    explicit = tmp_path / "ci.yml"
    explicit.write_text("project: {}\n", encoding="utf-8")
    assert locate_project(tmp_path, explicit).diagnostics == ()


def test_sst_cfg032_silent_when_the_file_named_is_the_discoverable_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    discovered = tmp_path / "sst_config.yml"
    discovered.write_text("project: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    # A file cannot shadow itself, however its path is spelled.
    spellings = [Path("sst_config.yml"), discovered, tmp_path / "." / "sst_config.yml"]
    if (tmp_path / "SST_CONFIG.YML").exists():
        spellings.append(tmp_path / "SST_CONFIG.YML")  # a case-insensitive filesystem
    for spelling in spellings:
        assert locate_project(tmp_path, spelling).diagnostics == (), spelling


def test_sst_cfg032_silent_for_sst_config_naming_the_project_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    monkeypatch.setenv("SST_CONFIG", str(project / "sst_config.yml"))
    _, diagnostics = validate_json(project)
    assert "SST-CFG032" not in [item["code"] for item in diagnostics]
