"""SST-DBT022: dbt_project.yml's `model-paths` is not a list of directories, so the default is used."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import SmallProject, found

DBT_PROJECT = "name: fixture\nprofile: fixture\nmodel-paths: {paths}\ntarget-path: target\n"


def test_sst_dbt022_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"dbt_project.yml": DBT_PROJECT.format(paths="models")}).load()
    [diagnostic] = found(project, "SST-DBT022")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model-paths could not be read; defaulted to ['models']"
    assert (diagnostic.subject, diagnostic.origin) == ("config:dbt_project.yml", Origin("dbt_project.yml"))


def test_sst_dbt022_silent(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"dbt_project.yml": DBT_PROJECT.format(paths="[models, marts]")}).load()
    assert found(project, "SST-DBT022") == []
