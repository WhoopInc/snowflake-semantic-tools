"""A compile driven as a library: no click context, the caches the caller owns, files edited in between.

This is the contract a long-lived caller builds on: two compiles of one project in one process,
sharing a load cache and a dbt cache, see a file edited between them and agree with a fresh
compile, and no click context is needed for either.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.dbt.run_cache import DbtCache
from snowflake_semantic_tools.adapters.git import git_dirty, git_sha
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.diagnostics_json import diagnostic_json
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

_METRIC = "semantic_models/metrics/gross_margin.yml"


def _compile(project: Path, load_cache: LoadCache, dbt_cache: DbtCache) -> CompileResult:
    inputs = YamlProjectInputs(
        project_paths(project),
        target_name=None,
        manifest_path=DBT_MANIFEST,
        git_sha=lambda: git_sha(project),
        git_dirty=lambda: git_dirty(project),
        load_cache=load_cache,
        dbt_cache=dbt_cache,
    )
    return CompileProject(inputs).run()


def _as_data(result: CompileResult) -> tuple[list[tuple[str, str]], list[dict[str, object]]]:
    return (
        [(item.artifact_key, item.rendered_artifact.fingerprint) for item in result.compiled],
        [diagnostic_json(item) for item in result.diagnostics],
    )


def test_one_process_compiles_a_project_twice_with_shared_caches_and_sees_an_edit(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    load_cache, dbt_cache = LoadCache(), DbtCache()
    assert click.get_current_context(silent=True) is None

    before = _compile(project, load_cache, dbt_cache)
    metric = project / _METRIC
    metric.write_text(metric.read_text(encoding="utf-8").replace("Line-item revenue", "Edited revenue", 1))
    after = _compile(project, load_cache, dbt_cache)

    assert _as_data(after) != _as_data(before)
    assert _as_data(after) == _as_data(_compile(project, LoadCache(), DbtCache()))
