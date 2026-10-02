"""Path containment: what a project walk may read, and what a project-named path may resolve to."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.paths import resolve_within, walk_refusal


def test_a_path_inside_the_root_resolves_and_one_outside_does_not(tmp_path: Path) -> None:
    (tmp_path / "project/inner").mkdir(parents=True)
    project = tmp_path / "project"
    assert resolve_within(project, project / "inner/../inner") == (project / "inner").resolve()
    assert resolve_within(project, project / "../elsewhere") is None


def test_a_walk_refuses_links_at_any_depth_and_paths_that_climb_out(tmp_path: Path) -> None:
    project = tmp_path / "project"
    (project / "real").mkdir(parents=True)
    (project / "real/file.md").write_text("x", encoding="utf-8")
    (project / "linked").symlink_to(project / "real")
    assert walk_refusal(project, project / "real/file.md") is None
    assert walk_refusal(project, project / "linked") == "it is a symbolic link, which SST does not follow"
    assert walk_refusal(project, project / "linked/file.md") == "linked is a symbolic link, which SST does not follow"
    assert walk_refusal(project, project / "../outside") == "it resolves outside the project"


def test_a_project_reached_through_a_link_is_still_walked(tmp_path: Path) -> None:
    (tmp_path / "real/skills").mkdir(parents=True)
    (tmp_path / "alias").symlink_to(tmp_path / "real")
    assert walk_refusal(tmp_path / "alias", tmp_path / "alias/skills") is None
