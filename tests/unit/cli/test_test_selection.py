"""`sst test --select`, `--exclude` and `--update-golden`: which artifacts the suites cover, and rewriting goldens."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import REPO_ROOT, common, compile_project, project_copy

GOLDEN = REPO_ROOT / "tests" / "golden" / "expected" / "ddl"


def _golden(project: Path, golden_dir: Path, *flags: str) -> Result:
    args = ["test", *common(project), "--suite", "golden", "--golden-dir", str(golden_dir), *flags]
    return CliRunner().invoke(cli, args)


def _goldens(tmp_path: Path) -> Path:
    """A copy of the committed goldens, so a test may change them."""
    root = tmp_path / "expected"
    shutil.copytree(GOLDEN.parent, root)
    return root / "ddl"


def test_select_and_exclude_narrow_the_golden_suite(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    selected = _golden(project, GOLDEN, "--select", "jaffle_minimal")
    assert selected.exit_code == 0, selected.output
    assert "golden suite passed for 1 artifact(s)" in selected.output
    excluded = _golden(project, GOLDEN, "--exclude", "type:semantic_view")
    assert excluded.exit_code == 0, excluded.output
    assert "golden suite passed for 11 artifact(s)" in excluded.output

    goldens = _goldens(tmp_path)
    (goldens / "jaffle_menu.sql").unlink()
    assert _golden(project, goldens).exit_code == 4
    assert _golden(project, goldens, "--exclude", "jaffle_menu").exit_code == 0

    nothing = _golden(project, GOLDEN, "--select", "no_such_artifact", "-o", "json")
    assert nothing.exit_code == 4
    assert "SST-DIS010" in {item["code"] for item in json.loads(nothing.stdout)["diagnostics"]}


def test_a_selected_connected_suite_still_checks_the_whole_compiled_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    def unreachable(params: object) -> None:
        raise SnowflakePortError("unreachable", diagnostic=None)

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", unreachable)
    smoke = CliRunner().invoke(cli, ["test", *common(project), "--suite", "smoke", "--select", "jaffle_minimal"])
    assert smoke.exit_code == 5, smoke.output
    evals = CliRunner().invoke(cli, ["test", *common(project), "--suite", "evals", "--select", "type:eval"])
    assert evals.exit_code == 5, evals.output


def test_update_golden_creates_and_rewrites_goldens(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CI", raising=False)
    project = project_copy(tmp_path)
    goldens = _goldens(tmp_path)
    minimal = goldens / "jaffle_minimal.sql"
    minimal.write_text(minimal.read_text(encoding="utf-8").replace("Menu products only", "Drifted"), encoding="utf-8")
    (goldens / "jaffle_menu.sql").unlink()
    assert _golden(project, goldens).exit_code == 4

    updated = _golden(project, goldens, "--update-golden", "-o", "json")
    assert updated.exit_code == 0, updated.output
    assert json.loads(updated.stdout)["data"]["written"] == [str(goldens / "jaffle_menu.sql"), str(minimal)]
    assert _golden(project, goldens).exit_code == 0
    again = _golden(project, goldens, "--update-golden")
    assert again.exit_code == 0
    assert "0 golden file(s) written for 14 artifact(s)" in again.output


def test_update_golden_is_refused_for_a_suite_without_goldens_and_in_ci(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    monkeypatch.delenv("CI", raising=False)
    smoke = CliRunner().invoke(cli, ["test", *common(project), "--suite", "smoke", "--update-golden"])
    assert smoke.exit_code == 3
    assert "cannot run with --suite smoke" in smoke.output
    monkeypatch.setenv("CI", "false")
    assert _golden(project, _goldens(tmp_path), "--update-golden").exit_code == 0
    monkeypatch.setenv("CI", "true")
    ci = _golden(project, GOLDEN, "--update-golden")
    assert ci.exit_code == 3
    assert "never valid in CI" in ci.output
