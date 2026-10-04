"""A target's name never places the file or folder SST names after it outside its own folder.

Target names are `profiles.yml` keys and `--target` values, so any text. The local state file,
the recorded observation, and the deferred manifest's folder each write the name as
`domain.file_names.file_name` does: one segment, never hidden, `.` or `..`, holding no separator
or NUL, and distinct for distinct names.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.deferral import resolve_deferral
from snowflake_semantic_tools.adapters.fs.local import StateFileStore, observation_file, state_file
from tests.helpers.projects import project_paths
from tests.helpers.seam_projects import SmallProject

HOSTILE = ("../../evil", "a/b", "a\\b", "x\x00y", ".hidden", "..", ".", "", "/abs", "C:\\x")


def _is_one_safe_segment(path: Path, folder: Path) -> bool:
    name = path.name
    return (
        path.parent == folder
        and not name.startswith(".")
        and not any(separator in name for separator in ("/", "\\", "\x00", os.sep))
    )


@pytest.mark.parametrize("target", HOSTILE)
def test_a_hostile_target_names_a_file_directly_in_its_folder(tmp_path: Path, target: str) -> None:
    assert _is_one_safe_segment(state_file(tmp_path, target), tmp_path)
    assert _is_one_safe_segment(observation_file(tmp_path, target), tmp_path)


def test_target_files_are_distinct_for_distinct_names_and_plain_for_plain_ones(tmp_path: Path) -> None:
    names = (*HOSTILE, "prod", "PROD", "prod-eu", "prod_eu")
    assert len({state_file(tmp_path, name) for name in names}) == len(names)
    assert len({observation_file(tmp_path, name) for name in names}) == len(names)
    assert state_file(tmp_path, "prod-eu").name == "state.prod-eu.json"
    assert observation_file(tmp_path, "dev").name == "observation.dev.json"


def test_a_hostile_target_state_is_written_inside_the_project(tmp_path: Path) -> None:
    project = tmp_path / "project"
    build = project / "target" / "sst"
    path = state_file(build, "../../../evil")
    StateFileStore(path, root=project).write({"schema_version": 1})
    assert [found.parent for found in project.rglob("state.*.json")] == [build]
    assert not list(tmp_path.glob("evil*"))


@pytest.mark.parametrize("target", ["../../evil", "..", ".hidden", "a/b"])
def test_a_hostile_deferred_target_keeps_its_manifest_folder_under_defer(tmp_path: Path, target: str) -> None:
    quoted = target.replace("\\", "\\\\").replace('"', '\\"')
    profiles = (
        "fixture:\n  target: dev\n  outputs:\n"
        "    dev:\n      type: snowflake\n      database: DB\n      schema: SCH\n"
        f'    "{quoted}":\n      type: snowflake\n      database: PROD_DB\n      schema: SCH\n'
    )
    files = {"profiles.yml": profiles, "sst_config.yml": "project:\n  semantic_models_dir: semantic_models\n"}
    SmallProject(tmp_path, files=files).write()
    found = resolve_deferral(dataclasses.replace(project_paths(tmp_path), defer_target=target))
    assert found is not None and found.target == target
    assert _is_one_safe_segment(found.state_dir, tmp_path / "target" / "sst" / "defer")
