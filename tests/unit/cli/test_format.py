"""`sst format`: the configured paths by default, `--check` at exit 2, and every flag composing."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli

_MESSY = 'semantic_views:\n- description: "one\\ntwo"   \n  name: sales\n'
_CANONICAL = "semantic_views:\n  - name: sales\n    description: |-\n      one\n      two\n"


def _project(tmp_path: Path) -> Path:
    (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p\nmodel-paths: [models]\n", encoding="utf-8")
    views = tmp_path / "semantic_models"
    views.mkdir()
    (views / "views.yml").write_text(_MESSY, encoding="utf-8")
    (views / "done.yml").write_text("a: 1\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "schema.yml").write_text("models:\n- name: m\n", encoding="utf-8")
    hidden = tmp_path / "models" / "target"
    hidden.mkdir()
    (hidden / "skip.yml").write_text("x:   1\n", encoding="utf-8")
    return tmp_path


def _format(project: Path, *args: str) -> Result:
    return CliRunner().invoke(cli, ["format", "--project-dir", str(project), *args])


def test_check_reports_without_writing_and_exits_2(tmp_path: Path) -> None:
    project = _project(tmp_path)
    result = _format(project, "--check")
    assert result.exit_code == 2, result.output
    assert "would reformat semantic_models/views.yml" in result.output
    assert "would reformat models/schema.yml" in result.output
    assert "2 file(s) would be reformatted; run `sst format`" in result.output
    assert (project / "semantic_models" / "views.yml").read_text(encoding="utf-8") == _MESSY
    assert _format(project, "--check", "--no-detailed-exitcode").exit_code == 0


def test_format_writes_the_configured_paths_then_is_already_canonical(tmp_path: Path) -> None:
    project = _project(tmp_path)
    machine = _format(project, "--output", "json")
    assert machine.exit_code == 0, machine.output
    data = json.loads(machine.output)["data"]
    assert data == {
        "formatted": ["models/schema.yml", "semantic_models/views.yml"],
        "unchanged": ["semantic_models/done.yml"],
        "would_change": [],
    }
    assert (project / "semantic_models" / "views.yml").read_text(encoding="utf-8") == _CANONICAL
    assert (project / "models" / "target" / "skip.yml").read_text(encoding="utf-8") == "x:   1\n"
    again = _format(project, "--check")
    assert again.exit_code == 0
    assert "3 file(s) already canonical" in again.output


def test_dry_run_prints_the_diff_and_composes_with_sanitize(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / "semantic_models" / "syn.yml").write_text('t:\n  synonyms: ["bob\'s"]\n', encoding="utf-8")
    result = _format(project, "semantic_models/syn.yml", "--sanitize", "--dry-run")
    assert result.exit_code == 0, result.output
    assert "--- a/semantic_models/syn.yml" in result.output
    assert '+  synonyms: ["bobs"]' in result.output
    assert "bob's" in (project / "semantic_models" / "syn.yml").read_text(encoding="utf-8")


def test_force_rewrites_a_canonical_file_and_a_glob_selects_files(tmp_path: Path) -> None:
    project = _project(tmp_path)
    machine = _format(project, "semantic_models/d*.yml", "--force", "--output", "json")
    assert json.loads(machine.output)["data"]["formatted"] == ["semantic_models/done.yml"]
    assert (project / "semantic_models" / "views.yml").read_text(encoding="utf-8") == _MESSY


def test_a_file_that_is_not_yaml_is_left_alone_and_exits_1(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / "semantic_models" / "bad.yml").write_text("a: [1,\n", encoding="utf-8")
    (project / "semantic_models" / "binary.yml").write_bytes(b"\xff\xfe")
    result = _format(project, "--output", "json")
    envelope = json.loads(result.output)
    assert result.exit_code == 1
    assert {item["code"] for item in envelope["diagnostics"]} == {"SST-LOD001", "SST-PRT009"}
    assert (project / "semantic_models" / "bad.yml").read_text(encoding="utf-8") == "a: [1,\n"


def test_a_path_naming_no_yaml_file_is_a_usage_error(tmp_path: Path) -> None:
    result = _format(_project(tmp_path), "missing.yml")
    assert result.exit_code == 3
    assert "error[SST-PRT100]: PATH 'missing.yml' names no YAML file" in result.output


def test_no_path_and_no_configuration_exits_4_but_a_path_needs_none(tmp_path: Path) -> None:
    assert _format(tmp_path).exit_code == 4
    (tmp_path / "loose.yml").write_text("k:   v\n", encoding="utf-8")
    result = _format(tmp_path, str(tmp_path / "loose.yml"))
    assert result.exit_code == 0, result.output
    assert (tmp_path / "loose.yml").read_text(encoding="utf-8") == "k: v\n"
    assert "formatted" in result.output


def test_validate_finding_is_cured_by_format(tmp_path: Path) -> None:
    from snowflake_semantic_tools.domain.validate.semantic.files import formatting_problem

    project = _project(tmp_path)
    path = project / "semantic_models" / "views.yml"
    path.write_text(_MESSY.replace("\n", "\r\n"), encoding="utf-8")
    assert formatting_problem(path.read_bytes().decode("utf-8")) is not None
    assert _format(project, "semantic_models").exit_code == 0
    assert formatting_problem(path.read_bytes().decode("utf-8")) is None
