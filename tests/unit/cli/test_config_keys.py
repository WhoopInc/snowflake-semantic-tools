"""Configuration keys the spec keeps and the engine reads: each changes what a run does.

`generation.view_timeout`, `validation.exclude_dirs`, `dbt.*`, `snowflake.tool_types`,
`agents.+secure`, `agents.+tags` and `semantic_views.+tags`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml, excluded_globs
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.cli.wiring.project import view_timeout
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.config_schema import DbtSettings, dbt_settings
from tests.helpers.cli_projects import MANIFEST, common, project_copy
from tests.helpers.projects import project_paths
from tests.helpers.recorded_snowflake import RecordedSnowflake
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest


def _append(project: Path, text: str) -> None:
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + text, encoding="utf-8")


def test_plan_sessions_bound_every_statement_by_the_view_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    _append(project, "\ngeneration:\n  view_timeout: 120\n")
    opened: list[dict[str, Any]] = []
    port = RecordedSnowflake(state={})
    port.close = lambda: None  # type: ignore[attr-defined]

    def connector(params: dict[str, Any]) -> RecordedSnowflake:
        opened.append(params)
        return port

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connector)
    assert CliRunner().invoke(cli, ["compile", *common(project)]).exit_code == 0
    result = CliRunner().invoke(cli, ["plan", *common(project), "--output", "json"])
    assert result.exit_code in (0, 2), result.output
    assert opened and all(params["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 120 for params in opened)
    assert view_timeout({}) == 300 and view_timeout({"generation": {"view_timeout": 30}}) == 30


def test_excluded_directories_are_not_discovered(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    config = {"validation": {"exclude_dirs": ["semantic_models/filters", "semantic_models/*/core"]}}
    assert excluded_globs(config) == ("semantic_models/filters", "semantic_models/*/core")
    assert excluded_globs({"validation": {"exclude_dirs": "nope"}}) == ()
    found = {item.path for item in discover_yaml(project, "semantic_models", config=config).files}
    assert "semantic_models/filters/filters.yml" not in found
    assert "semantic_models/semantic_views/core/semantic_views.yml" not in found
    assert "semantic_models/metrics/metrics.yml" in found


def test_validate_reads_no_file_under_an_excluded_directory(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    broken = project / "semantic_models" / "scratch" / "broken.yml"
    broken.parent.mkdir()
    broken.write_text("snowflake_metrics: [ {name: \n", encoding="utf-8")
    failing = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict"])
    assert failing.exit_code != 0
    text = (project / "sst_config.yml").read_text(encoding="utf-8")
    (project / "sst_config.yml").write_text(
        text.replace("validation:\n", "validation:\n  exclude_dirs: [semantic_models/scratch]\n", 1), encoding="utf-8"
    )
    passing = CliRunner().invoke(cli, ["validate", *common(project), "--no-strict"])
    assert passing.exit_code == 0, passing.output


def test_the_dbt_block_reads_its_defaults_and_its_values() -> None:
    assert dbt_settings({}) == DbtSettings()
    block = {"dbt": {"invoke": False, "command": "compile", "manifest_schema_versions": [11, 12, True, "13"]}}
    assert dbt_settings(block) == DbtSettings(False, "compile", frozenset((11, 12)))
    assert dbt_settings({"dbt": {"command": 3, "manifest_schema_versions": []}}) == DbtSettings()


def test_dbt_command_and_invoke_decide_how_the_manifest_is_produced(tmp_path: Path) -> None:
    SmallProject(tmp_path, files={"sst_config.yml": "project: {}\ndbt:\n  command: compile\n"}).write()
    dbt = FakeDbt(writes=(tmp_path / "target" / "manifest.json", manifest()))
    dbt.parse = CompletedRun(0, "", "")
    # FakeDbt writes only for `parse`; a compile run writes the manifest itself here.
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest()), encoding="utf-8")
    YamlProjectSource(project_paths(tmp_path), dbt_runner=dbt).dbt_catalog()
    assert [run[1] for run in dbt.runs] == ["--version", "compile"]
    quiet = tmp_path / "quiet"
    SmallProject(quiet, files={"sst_config.yml": "project: {}\ndbt:\n  invoke: false\n"}).write()
    (quiet / "target").mkdir()
    (quiet / "target" / "manifest.json").write_text(json.dumps(manifest()), encoding="utf-8")
    idle = FakeDbt()
    assert YamlProjectSource(project_paths(quiet), dbt_runner=idle).dbt_catalog().models
    assert idle.runs == []


def test_manifest_schema_versions_set_which_manifests_are_read(tmp_path: Path) -> None:
    path = SmallProject(
        tmp_path, files={"sst_config.yml": "project: {}\ndbt:\n  manifest_schema_versions: [11]\n"}
    ).write()
    with pytest.raises(ProjectError) as raised:
        YamlProjectSource(project_paths(tmp_path), manifest_path=path, invoke_dbt=False).dbt_catalog()
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.context["expected"]) == ("SST-DBT017", "v11")


def _compile(project: Path, *flags: str) -> dict[str, Any]:
    result = CliRunner().invoke(cli, ["compile", *common(project), *flags, "--output", "json"])
    assert result.exit_code in (0, 1), result.output
    envelope: dict[str, Any] = json.loads(result.output)
    return envelope


def _replace(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def _minimal_agent(project: Path) -> AgentModel:
    compiled = compile_result(project_paths(project), None, MANIFEST)
    [agent] = [
        item for item in compiled.compiled if isinstance(item, CompiledAgent) and item.name == "jaffle_minimal_agent"
    ]
    return agent.resolved.model


def test_tool_types_extend_the_types_an_agent_tool_may_take(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    agent = project / "agents" / "jaffle_minimal" / "agent.yml"
    agent.write_text(
        "name: jaffle_minimal_agent\nspec:\n  tools:\n"
        "    - type: future_tool\n      name: future\n      description: A tool type Snowflake shipped later.\n",
        encoding="utf-8",
    )
    assert "SST-RND012" in [item["code"] for item in _compile(project)["diagnostics"]]
    _replace(project / "sst_config.yml", "\nsnowflake:\n", "\nsnowflake:\n  tool_types: [future_tool]\n")
    assert "SST-RND012" not in [item["code"] for item in _compile(project)["diagnostics"]]


def test_agents_inherit_secure_and_tags_they_do_not_set(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    assert (_minimal_agent(project).secure, _minimal_agent(project).tags) == (False, ())
    _replace(project / "sst_config.yml", "\nagents:\n", "\nagents:\n  +secure: true\n")
    assert _minimal_agent(project).secure is True
    agent = project / "agents" / "jaffle_minimal" / "agent.yml"
    agent.write_text(agent.read_text(encoding="utf-8") + "secure: false\n", encoding="utf-8")
    assert _minimal_agent(project).secure is False
    _replace(
        project / "sst_config.yml",
        "\nagents:\n",
        "\nagents:\n  +tags:\n    - name: COST_CENTER\n      value: analytics\n",
    )
    assert _minimal_agent(project).tags == (("COST_CENTER", "analytics"),)


def test_views_take_the_default_tags_when_they_set_none(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    tagged = "\nsemantic_views:\n  +tags:\n    - name: \"{{ tag('data_domain') }}\"\n      value: routed_default\n"
    _replace(project / "sst_config.yml", "\nsemantic_views:\n", tagged)
    ddl = tmp_path / "ddl"
    envelope = _compile(project, "--emit-ddl", str(ddl))
    assert [item["code"] for item in envelope["diagnostics"] if item["severity"] == "error"] == []
    minimal = (ddl / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert "DATA_DOMAIN" in minimal and "routed_default" in minimal
    # A view that writes its own tags keeps them.
    assert "routed_default" not in (ddl / "jaffle_sales.sql").read_text(encoding="utf-8")
