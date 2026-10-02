"""SST-CFG037: a strict project without a baseline is told, once, how many warnings now block."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy


def _strict(project: Path) -> Path:
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8").replace("strict: false", "strict: true"), encoding="utf-8")
    return project


def _codes(project: Path, *flags: str) -> list[dict[str, object]]:
    result = CliRunner().invoke(cli, ["validate", *common(project), *flags, "--output", "json"])
    diagnostics: list[dict[str, object]] = json.loads(result.output)["diagnostics"]
    return [item for item in diagnostics if item["code"] == "SST-CFG037"]


def test_sst_cfg037_fires(tmp_path: Path) -> None:
    project = _strict(project_copy(tmp_path))
    [diagnostic] = _codes(project)
    assert diagnostic["severity"] == "warning"
    assert str(diagnostic["message"]).startswith("validation.strict: true is enforced from 1.0; ")
    assert str(diagnostic["message"]).endswith(" warnings now block")
    # Once only, per project.
    assert _codes(project) == []


def test_sst_cfg037_silent(tmp_path: Path) -> None:
    assert _codes(project_copy(tmp_path)) == []
    strict = _strict(project_copy(tmp_path / "with-baseline"))
    (strict / ".sst").mkdir()
    (strict / ".sst" / "baseline.json").write_text(
        '{"version": 1, "expires_on": "2999-01-01", "entries": []}', encoding="utf-8"
    )
    assert _codes(strict) == []
