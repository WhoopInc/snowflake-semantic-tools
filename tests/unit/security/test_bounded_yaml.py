"""Every YAML SST parses is read within a nesting bound, so a crafted file is refused, not a crash.

PyYAML and ruamel compose nested collections by recursion: a file nesting a few thousand levels
deep used to end a run in `RecursionError`, reported as an internal error. Each reader now
refuses it as YAML it cannot read, with the code it already gives such a file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.dbt.invoke import _yaml_mapping
from snowflake_semantic_tools.adapters.dbt.profiles import _resolve_env
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.roundtrip import check_depth, load_editable
from snowflake_semantic_tools.adapters.yaml.compose import compose_single
from snowflake_semantic_tools.adapters.yaml.format import canonical_yaml
from snowflake_semantic_tools.adapters.yaml.migrate import filter_sites
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import project_copy

PAST_THE_BOUND = bounded_yaml.MAX_YAML_DEPTH + 50
PAST_RECURSION = 20_000


def flow(depth: int) -> str:
    """A mapping whose one key holds flow sequences nested `depth` deep."""
    return "k: " + "[" * depth + "]" * depth + "\n"


def block(depth: int) -> str:
    """Block mappings nested `depth` deep, one key per level."""
    return "".join(f"{'  ' * level}k{level}:\n" for level in range(depth)) + f"{'  ' * depth}v: 1\n"


DEPTHS = pytest.mark.parametrize(
    "text",
    [flow(PAST_THE_BOUND), flow(PAST_RECURSION), block(PAST_THE_BOUND)],
    ids=["flow-past-the-bound", "flow-past-recursion", "block-past-the-bound"],
)


@DEPTHS
def test_the_bounded_loader_refuses_deep_yaml_as_yaml(text: str) -> None:
    with pytest.raises(yaml.YAMLError, match="nest deeper than"):
        bounded_yaml.safe_load(text)
    with pytest.raises(yaml.YAMLError, match="nest deeper than"):
        bounded_yaml.compose(text)


def test_the_bounded_loader_reads_what_is_within_the_bound() -> None:
    within = flow(bounded_yaml.MAX_YAML_DEPTH - 1)
    assert isinstance(bounded_yaml.safe_load(within), dict)
    node = bounded_yaml.compose(within)
    assert isinstance(node, yaml.MappingNode)
    assert bounded_yaml.safe_load("a: 1") == {"a": 1}
    check_depth(within)
    check_depth("a: [")  # not YAML: left to the load that follows


@DEPTHS
def test_a_deep_project_file_is_a_syntax_error(text: str) -> None:
    with pytest.raises(ProjectError) as raised:
        compose_single(text, "semantic_models/deep.yml")
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.code == "SST-LOD001"
    assert "nest deeper than" in diagnostic.message


@DEPTHS
def test_format_and_round_trip_edits_refuse_deep_yaml(text: str) -> None:
    with pytest.raises(ProjectError) as raised:
        canonical_yaml(text, "models/deep.yml")
    assert [item.code for item in raised.value.diagnostics] == ["SST-LOD001"]
    with pytest.raises(ProjectError, match="cannot parse YAML to edit it: .*nest deeper than"):
        load_editable(text, "models/deep.yml")


@DEPTHS
def test_dbt_owned_and_filter_reads_treat_deep_yaml_as_unreadable(tmp_path: Path, text: str) -> None:
    path = tmp_path / "packages.yml"
    path.write_text(text, encoding="utf-8")
    assert _yaml_mapping(path) == {}
    assert filter_sites("snowflake_filters:\n" + text, "deep.yml") == ()


def test_an_env_var_that_is_not_a_yaml_scalar_stays_its_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SST_DEEP", "[" * PAST_RECURSION)
    assert _resolve_env("{{ env_var('SST_DEEP') | as_native }}") == "[" * PAST_RECURSION
    monkeypatch.setenv("SST_DEEP", "a: [")
    assert _resolve_env("{{ env_var('SST_DEEP') | as_number }}") == "a: ["


@pytest.mark.parametrize(
    ("file", "code"),
    [("sst_config.yml", "SST-CFG002"), ("profiles.yml", "SST-DBT019"), ("dbt_project.yml", "SST-CFG002")],
)
def test_a_deep_configuration_file_is_refused_with_its_code(tmp_path: Path, file: str, code: str) -> None:
    project = project_copy(tmp_path)
    path = project / file
    path.write_text(path.read_text(encoding="utf-8") + "\nzz_" + flow(PAST_RECURSION), encoding="utf-8")
    result = CliRunner().invoke(cli, ["validate", "--project-dir", str(project), "--output", "json"])
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert result.exit_code == 4
    assert code in [item["code"] for item in json.loads(result.output)["diagnostics"]]
