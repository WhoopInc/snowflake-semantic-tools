"""SST-CFG061: the configuration file holds no YAML document."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy


def test_sst_cfg061_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("# nothing yet\n\n", encoding="utf-8")
    with pytest.raises(ProjectError) as raised:
        load_project_config(project_paths(tmp_path))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG061", Severity.ERROR)
    assert diagnostic.message == "sst_config.yml is empty"
    assert diagnostic.subject == "config:sst_config.yml"
    # A run refused for it exits 4 with the error counted, not as a warning alone (GitHub #271).
    project = project_copy(tmp_path / "project")
    (project / "sst_config.yml").write_text("", encoding="utf-8")
    result = CliRunner().invoke(
        cli, ["-o", "agent", "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "validate"]
    )
    assert result.exit_code == 4
    assert result.stdout.startswith("sst validate: CONFIG exit=4 errors=1 ")


def test_sst_cfg061_silent(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: true\n", encoding="utf-8")
    assert [item.code for item in load_project_config(project_paths(tmp_path)).diagnostics] == []
