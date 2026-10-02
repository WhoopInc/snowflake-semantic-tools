"""SST-PRT009: SST refuses to read a file reached through a symbolic link or outside the project."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.project import target_path
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.model.diagnostic import Origin, Severity

SKILL = "---\nname: close\ndescription: Close the month.\n---\nSteps.\n"


def _skill(project: Path) -> Path:
    folder = project / "skills" / "close"
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text(SKILL, encoding="utf-8")
    return folder


def test_sst_prt009_fires(tmp_path: Path) -> None:
    project = tmp_path / "project"
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    (_skill(project) / "notes.txt").symlink_to(outside)
    catalog = load_skill_catalog(project, skills_dir="skills", plugins_dir="plugins")
    [diagnostic] = catalog.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRT009", Severity.ERROR)
    assert diagnostic.message == (
        "could not read skills/close/notes.txt: it is a symbolic link, which SST does not follow"
    )
    assert diagnostic.origin == Origin("skills/close/notes.txt")
    assert [item.path for item in catalog.skills[0].files] == ["SKILL.md"]


def test_sst_prt009_fires_for_a_dbt_target_path_outside_the_project(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        target_path(tmp_path, lambda path: {"target-path": "../elsewhere"})
    [diagnostic] = raised.value.diagnostics
    assert diagnostic.code == "SST-PRT009"
    assert diagnostic.message == "could not read target-path '../elsewhere': it resolves outside the project"
    assert diagnostic.origin == Origin("dbt_project.yml")


def test_sst_prt009_silent(tmp_path: Path) -> None:
    project = tmp_path / "project"
    (_skill(project) / "notes.txt").write_text("notes", encoding="utf-8")
    catalog = load_skill_catalog(project, skills_dir="skills", plugins_dir="plugins")
    assert list(catalog.diagnostics) == []
    assert [item.path for item in catalog.skills[0].files] == ["SKILL.md", "notes.txt"]
    assert target_path(project, lambda path: {"target-path": "build/dbt"}) == project / "build/dbt/manifest.json"
