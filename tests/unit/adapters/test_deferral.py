"""Deferral: relations resolve to another target's manifest while artifacts publish to the run's own."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.deferral import Deferral, resolve_deferral
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs, YamlProjectSource
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtSource
from snowflake_semantic_tools.domain.resolve.defer import defer_relations
from tests.helpers.cli_projects import DBT_MANIFEST, common, project_copy
from tests.helpers.projects import project_paths
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest, model_node

PROFILES = (
    "fixture:\n  target: dev\n  outputs:\n"
    "    dev:\n      type: snowflake\n      database: DB\n      schema: SCH\n"
    "    prod:\n      type: snowflake\n      database: PROD_DB\n      schema: SCH\n"
)


def _model(unique_id: str, relation: str) -> DbtModel:
    return DbtModel(unique_id, unique_id.rsplit(".", 1)[-1], relation, (), (), (), raw_relation_name=relation.lower())


def test_defer_relations_takes_the_deferred_relation_of_each_node_both_hold() -> None:
    current = DbtCatalog(
        "v12",
        None,
        "p",
        (_model("model.p.orders", "DEV.S.ORDERS"), _model("model.p.new", "DEV.S.NEW")),
        sources=(
            DbtSource("source.p.raw.a", "raw", "a", "DEV.RAW.A"),
            DbtSource("source.p.raw.b", "raw", "b", "DEV.RAW.B"),
        ),
    )
    deferred = DbtCatalog(
        "v12",
        None,
        "p",
        (_model("model.p.orders", "PROD.S.ORDERS"),),
        sources=(DbtSource("source.p.raw.a", "raw", "a", "PROD.RAW.A"),),
    )
    result = defer_relations(current, deferred)
    assert [(model.relation_name, model.raw_relation_name) for model in result.models] == [
        ("PROD.S.ORDERS", "prod.s.orders"),
        ("DEV.S.NEW", "dev.s.new"),
    ]
    assert [source.relation_name for source in result.sources] == ["PROD.RAW.A", "DEV.RAW.B"]


def _small(tmp_path: Path, config: str = "") -> Path:
    files = {"profiles.yml": PROFILES, "sst_config.yml": f"project:\n  semantic_models_dir: semantic_models\n{config}"}
    return SmallProject(tmp_path, files=files).write()


def test_a_run_defers_to_the_flag_else_the_config_and_to_nothing_when_told_not_to(tmp_path: Path) -> None:
    _small(tmp_path, "defer:\n  target: prod\n  state_path: state\n")
    paths = project_paths(tmp_path)
    assert resolve_deferral(paths) == Deferral("prod", tmp_path / "state", produce=False)
    assert resolve_deferral(dataclasses.replace(paths, defer_disabled=True)) is None
    flagged = resolve_deferral(dataclasses.replace(paths, defer_target="dev"))
    assert flagged is not None and flagged.target == "dev"
    bare = tmp_path / "bare"
    _small(bare, "defer:\n  auto_compile: true\n  state_path: /abs/state\n")
    assert resolve_deferral(project_paths(bare)) is None
    produced = resolve_deferral(dataclasses.replace(project_paths(bare), defer_target="prod"))
    assert produced == Deferral("prod", Path("/abs/state"), produce=True)
    assert produced.manifest == Path("/abs/state/manifest.json")
    defaulted = tmp_path / "defaulted"
    _small(defaulted)
    found = resolve_deferral(dataclasses.replace(project_paths(defaulted), defer_target="prod"))
    assert found == Deferral("prod", defaulted / "target" / "sst" / "defer" / "prod", produce=False)


def test_deferring_to_a_target_the_profile_does_not_declare_is_refused(tmp_path: Path) -> None:
    _small(tmp_path, "defer:\n  target: qa\n")
    with pytest.raises(ProjectError) as raised:
        resolve_deferral(project_paths(tmp_path))
    [diagnostic] = raised.value.diagnostics
    assert (diagnostic.code, diagnostic.subject) == ("SST-CFG010", "config:defer.target")
    assert diagnostic.message == "target 'qa' is absent from profile 'fixture'"


def _deferred_manifest(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    document = manifest({"model.fixture.products": model_node("products", "PROD_DB.SCH.PRODUCTS")})
    path.write_text(json.dumps(document), encoding="utf-8")


def test_the_catalog_and_the_manifest_digest_read_the_deferred_relations(tmp_path: Path) -> None:
    manifest_path = _small(tmp_path, "defer:\n  target: prod\n")
    _deferred_manifest(tmp_path / "target" / "sst" / "defer" / "prod" / "manifest.json")
    paths = project_paths(tmp_path)
    source = YamlProjectSource(paths, manifest_path=manifest_path, invoke_dbt=False)
    assert [model.relation_name for model in source.dbt_catalog().models] == ["PROD_DB.SCH.PRODUCTS"]
    undeferred = dataclasses.replace(project_paths(tmp_path), defer_disabled=True)
    plain = YamlProjectSource(undeferred, manifest_path=manifest_path, invoke_dbt=False)
    assert [model.relation_name for model in plain.dbt_catalog().models] == ["DB.SCH.PRODUCTS"]

    def digest(files: ProjectPaths) -> str:
        inputs = YamlProjectInputs(files, target_name=None, manifest_path=manifest_path, git_sha=lambda: "abc1234")
        return inputs.manifest_sources().dbt_digest

    assert digest(paths) != digest(undeferred)


def test_auto_compile_runs_dbt_for_the_deferred_target_into_its_state_path(tmp_path: Path) -> None:
    manifest_path = _small(tmp_path, "defer:\n  target: prod\n  state_path: prod_state\n  auto_compile: true\n")
    state = tmp_path / "prod_state"
    document = manifest({"model.fixture.products": model_node("products", "PROD_DB.SCH.PRODUCTS")})
    dbt = FakeDbt(writes=(state / "manifest.json", document))
    source = YamlProjectSource(project_paths(tmp_path), manifest_path=manifest_path, dbt_runner=dbt)
    assert [model.relation_name for model in source.dbt_catalog().models] == ["PROD_DB.SCH.PRODUCTS"]
    [_, parse] = dbt.runs
    assert parse[:2] == ("dbt", "parse") and parse[-4:] == ("--target", "prod", "--target-path", str(state))
    # The deferred manifest is read once per source.
    source.dbt_catalog()
    assert len(dbt.runs) == 2


def test_dbt_invoke_false_reads_the_deferred_manifest_as_supplied(tmp_path: Path) -> None:
    manifest_path = _small(tmp_path, "dbt:\n  invoke: false\ndefer:\n  target: prod\n  auto_compile: true\n")
    _deferred_manifest(tmp_path / "target" / "sst" / "defer" / "prod" / "manifest.json")
    dbt = FakeDbt(version=CompletedRun(1, "", "no dbt"))
    source = YamlProjectSource(project_paths(tmp_path), manifest_path=manifest_path, dbt_runner=dbt)
    assert [model.relation_name for model in source.dbt_catalog().models] == ["PROD_DB.SCH.PRODUCTS"]
    assert dbt.runs == []


def _defer_project(tmp_path: Path) -> Path:
    project = project_copy(tmp_path)
    prod = project / "target" / "sst" / "defer" / "prod" / "manifest.json"
    prod.parent.mkdir(parents=True)
    prod.write_text(DBT_MANIFEST.read_text(encoding="utf-8").replace("SST_REF_DEV", "SST_REF_PROD"), encoding="utf-8")
    return project


def _compiled(project: Path, *flags: str, env: dict[str, str] | None = None) -> tuple[int, str]:
    result = CliRunner().invoke(cli, ["compile", *common(project), *flags, "--output", "json"], env=env)
    manifest_file = project / "target" / "sst" / "manifest.json"
    return result.exit_code, manifest_file.read_text(encoding="utf-8") if manifest_file.is_file() else result.output


def test_compile_resolves_refs_to_the_deferred_target_and_publishes_to_its_own(tmp_path: Path) -> None:
    project = _defer_project(tmp_path)
    exit_code, plain = _compiled(project)
    assert exit_code == 0 and "SST_REF_PROD" not in plain
    exit_code, deferred = _compiled(project, "--defer-target", "prod")
    assert exit_code == 0 and "SST_REF_PROD.JAFFLE.ORDERS" in deferred
    # Views still publish to the run's target.
    assert json.loads(plain)["project"]["target"] == json.loads(deferred)["project"]["target"] == "dev"
    exit_code, from_env = _compiled(project, env={"SST_DEFER_TARGET": "prod"})
    assert exit_code == 0 and "SST_REF_PROD.JAFFLE.ORDERS" in from_env


def test_no_defer_overrides_the_configured_target_and_an_undeclared_target_exits_4(tmp_path: Path) -> None:
    project = _defer_project(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + "\ndefer:\n  target: prod\n", encoding="utf-8")
    assert "SST_REF_PROD.JAFFLE.ORDERS" in _compiled(project)[1]
    assert "SST_REF_PROD" not in _compiled(project, "--no-defer")[1]
    result = CliRunner().invoke(cli, ["compile", *common(project), "--defer-target", "qa", "--output", "json"])
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG010"]
