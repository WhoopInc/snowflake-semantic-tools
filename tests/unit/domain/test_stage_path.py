"""Every staged file name uses only the characters a stage accepts."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.stage_path import unsafe_segment


@pytest.mark.parametrize(
    "path", ["SKILL.md", "reference/steps_v2.md", "scripts/run-$1.py", ".cortex-plugin/plugin.json"]
)
def test_safe_paths_have_no_unsafe_segment(path: str) -> None:
    assert unsafe_segment(path) is None


@pytest.mark.parametrize(
    ("path", "segment"),
    [
        ("q1+q2.md", "q1+q2.md"),
        ("résumé.md", "résumé.md"),
        ("notes/my file.md", "my file.md"),
        ("bad dir/a.md", "bad dir"),
        ("a//b.md", ""),
        ("../escape.md", ".."),
    ],
)
def test_the_first_unsafe_segment_is_named(path: str, segment: str) -> None:
    assert unsafe_segment(path) == segment
