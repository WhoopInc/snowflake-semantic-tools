"""The CLI asks git for the project's commit and for the paths it has not committed."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from snowflake_semantic_tools.cli.wiring.project import git_dirty, git_sha
from snowflake_semantic_tools.domain.ports.project import UNKNOWN_GIT_SHA

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@example.com", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
    )


def test_git_dirty_lists_the_projects_uncommitted_paths_but_not_its_build_directory(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    for name in ("a.md", "b.md"):
        (project / name).write_text(name)
    (tmp_path / "outside.md").write_text("outside")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "init")
    assert git_dirty(project) == ()
    assert git_sha(project) != UNKNOWN_GIT_SHA
    (project / "a.md").write_text("changed")
    _git(tmp_path, "mv", "project/b.md", "project/c.md")
    (project / "new.md").write_text("new")
    (tmp_path / "outside.md").write_text("changed outside the project")
    (project / "sst_target").mkdir()
    (project / "sst_target" / "manifest.json").write_text("{}")
    # Paths are the work tree's; a rename is its new path alone.
    assert git_dirty(project) == ("project/a.md", "project/c.md", "project/new.md")


def test_git_dirty_and_git_sha_say_nothing_outside_a_work_tree(tmp_path: Path) -> None:
    assert git_dirty(tmp_path) == ()
    assert git_sha(tmp_path) == UNKNOWN_GIT_SHA
