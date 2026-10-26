"""Still more `sst` commands at their edges: eval's scratch copies, format, lineage, list, and migrate."""

from __future__ import annotations

import json
import shutil
import stat
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake, failed


def _codes(output: str) -> list[str]:
    return [item["code"] for item in json.loads(output)["diagnostics"]]


def test_an_unpublished_eval_whose_scratch_copy_is_refused_runs_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    port.execute_results = [failed("Insufficient privileges to operate on schema 'JAFFLE'")]
    args = ["eval", "--unpublished", *common(project_copy(tmp_path)), "--target", "dev", "-o", "json"]
    result = invoke_with_port(monkeypatch, port, args)
    assert result.exit_code == 1, result.output
    assert _codes(result.output) == ["SST-ASK008"]
    assert port.stream_runs == []


def _yaml_project(root: Path) -> Path:
    (root / "semantic_models").mkdir(parents=True)
    (root / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    (root / "semantic_models" / "views.yml").write_text("b:   1\n", encoding="utf-8")
    (root / "semantic_models" / "notes.txt").write_text("not yaml\n", encoding="utf-8")
    return root


def test_a_glob_over_yaml_and_other_files_formats_only_the_yaml(tmp_path: Path) -> None:
    project = _yaml_project(tmp_path / "project")
    result = CliRunner().invoke(cli, ["format", "--project-dir", str(project), "semantic_models/*", "--check"])
    assert result.exit_code == 2, result.output
    assert "would reformat semantic_models/views.yml" in result.output and "notes.txt" not in result.output


def test_a_file_outside_the_project_is_named_in_full_and_formatted(tmp_path: Path) -> None:
    project = _yaml_project(tmp_path / "project")
    outside = tmp_path / "elsewhere.yml"
    outside.write_text("a:   1\n", encoding="utf-8")
    result = CliRunner().invoke(cli, ["format", "--project-dir", str(project), str(outside), "--check"])
    assert result.exit_code == 2, result.output
    assert f"would reformat {outside.as_posix()}" in result.output


def test_lineage_against_a_state_folder_with_no_manifest_is_refused(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    empty = tmp_path / "state"
    empty.mkdir()
    args = ["lineage", "--project-dir", str(project), "metric:line_item_count", "--state", str(empty), "-o", "json"]
    refused = CliRunner().invoke(cli, args)
    assert refused.exit_code == 4, refused.output
    assert _codes(refused.output) == ["SST-MAN001"]


def test_the_lineage_tree_marks_what_changed_since_the_state(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    state = tmp_path / "state"
    state.mkdir()
    shutil.copy(project / "sst_target" / "manifest.json", state / "manifest.json")
    metric = project / "semantic_models" / "metrics" / "line_item_count.yml"
    metric.write_text(metric.read_text(encoding="utf-8").replace("COUNT({{", "COUNT(DISTINCT {{", 1), encoding="utf-8")
    compile_project(project)
    args = ["lineage", "--project-dir", str(project), "metric:line_item_count", "--state", str(state)]
    shown = CliRunner().invoke(cli, args)
    assert shown.exit_code == 0, shown.output
    assert "metric:line_item_count  ~ changed" in shown.output.splitlines()


def test_a_relationship_lists_with_no_expression(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    listed = CliRunner().invoke(cli, ["list", "relationships", "--project-dir", str(project), "-o", "json"])
    assert listed.exit_code == 0, listed.output
    items = json.loads(listed.output)["data"]["items"]
    assert items and all("expression" not in item and "tables" in item for item in items)


CONFIG = "project:\n  semantic_models_dir: semantic_models\nvalidation:\n  snowflake_syntax_check: false\n"


@pytest.fixture
def read_only() -> Iterator[list[Path]]:
    """Folders made read-only by a test, made writable again after it."""
    folders: list[Path] = []
    yield folders
    for folder in folders:
        folder.chmod(stat.S_IRWXU)


def _lock(read_only: list[Path], folder: Path) -> None:
    folder.chmod(stat.S_IRUSR | stat.S_IXUSR)
    read_only.append(folder)


def test_migrate_refs_reports_a_file_it_cannot_rewrite(tmp_path: Path, read_only: list[Path]) -> None:
    views = tmp_path / "semantic_models" / "semantic_views"
    views.mkdir(parents=True)
    (tmp_path / "sst_config.yml").write_text(CONFIG, encoding="utf-8")
    (views / "views.yml").write_text("semantic_views:\n  - name: v\n    tables:\n      - {{ table('orders') }}\n")
    _lock(read_only, views)
    result = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(tmp_path), "--write", "-o", "json"])
    assert result.exit_code == 1, result.output
    assert _codes(result.output) == ["SST-PRT008"]


def test_migrate_split_reports_a_list_it_cannot_split(tmp_path: Path, read_only: list[Path]) -> None:
    metrics = tmp_path / "semantic_models" / "metrics"
    metrics.mkdir(parents=True)
    (tmp_path / "sst_config.yml").write_text(CONFIG, encoding="utf-8")
    (metrics / "metrics.yml").write_text("snowflake_metrics:\n  - name: a\n    expr: x\n", encoding="utf-8")
    _lock(read_only, metrics)
    result = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(tmp_path), "--write", "-o", "json"])
    assert result.exit_code == 1, result.output
    assert _codes(result.output) == ["SST-PRT008"]
    assert (metrics / "metrics.yml").is_file()


def test_migrate_split_reports_a_configuration_it_cannot_rewrite(tmp_path: Path, read_only: list[Path]) -> None:
    (tmp_path / "semantic_models").mkdir()
    (tmp_path / "tools").mkdir()
    (tmp_path / "sst_config.yml").write_text(CONFIG, encoding="utf-8")
    (tmp_path / "tools" / "g.yml").write_text(
        "tools:\n  - group: g\n    immutable: true\n    reference:\n      - name: r\n        type: stage\n",
        encoding="utf-8",
    )
    _lock(read_only, tmp_path)
    result = CliRunner().invoke(cli, ["migrate", "split", "--project-dir", str(tmp_path), "--write", "-o", "json"])
    assert result.exit_code == 1, result.output
    assert _codes(result.output) == ["SST-PRT008"]
    assert "+immutable" not in (tmp_path / "sst_config.yml").read_text(encoding="utf-8")
