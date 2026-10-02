"""SST-CFG032: `--config` or `$SST_CONFIG` is used, and the project root holds a configuration file it shadows."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.domain.diagnostics import Severity


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
