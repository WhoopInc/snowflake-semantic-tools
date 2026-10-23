"""dbt_project.yml is dbt's YAML: anchors, aliases and merge keys load as dbt loads them.

SST's strict parser refuses them in SST's own files (SST-LOD013, SST-LOD014); dbt accepts them,
so a dbt project that uses them must still load.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import read_plain_yaml
from snowflake_semantic_tools.adapters.dbt.project import check_model_paths, model_paths, target_path
from snowflake_semantic_tools.adapters.errors import ProjectError
from tests.helpers.cli_json import invoke_json
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

_ANCHORED = """name: p
paths: &paths [models, marts]
model-paths: *paths
target-path: &out build
base: &base {materialized: view}
models:
  p:
    +database: &db ANALYTICS
    marts:
      <<: *base
      +database: *db
"""


def test_anchors_aliases_and_merge_keys_load(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(_ANCHORED, encoding="utf-8")
    for folder in ("models", "marts"):
        (tmp_path / folder).mkdir()
    assert model_paths(tmp_path, read_plain_yaml) == (("models", "marts"), ())
    check_model_paths(tmp_path, read_plain_yaml)
    assert target_path(tmp_path, read_plain_yaml) == tmp_path / "build" / "manifest.json"
    assert read_plain_yaml(tmp_path / "dbt_project.yml")["models"]["p"]["marts"]["materialized"] == "view"


@pytest.mark.parametrize(
    ("text", "code"),
    [("name: [unclosed\n", "SST-CFG002"), ("- a list\n", "SST-LOD002")],
)
def test_a_dbt_project_that_is_not_a_yaml_mapping_is_still_refused(tmp_path: Path, text: str, code: str) -> None:
    (tmp_path / "dbt_project.yml").write_text(text, encoding="utf-8")
    with pytest.raises(ProjectError) as caught:
        read_plain_yaml(tmp_path / "dbt_project.yml")
    assert [item.code for item in caught.value.diagnostics] == [code]


def test_an_unreadable_dbt_project_is_reported_not_raised(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as caught:
        read_plain_yaml(tmp_path / "dbt_project.yml")
    assert [item.code for item in caught.value.diagnostics] == ["SST-PRT009"]


def test_validate_loads_a_dbt_project_that_uses_anchors(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    with (project / "dbt_project.yml").open("a", encoding="utf-8") as file:
        file.write("\nvars:\n  base: &b SST_X\n  other: *b\n")
    exit_code, diagnostics = invoke_json(
        [
            "validate",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--no-snowflake-syntax-check",
            "--no-strict",
        ]
    )
    codes = [item["code"] for item in diagnostics]
    assert "SST-LOD013" not in codes and "SST-LOD014" not in codes
    assert exit_code == 0, codes
