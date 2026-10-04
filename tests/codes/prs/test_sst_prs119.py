"""SST-PRS119: a skill folder holds another skill folder below its root."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import skill_tree_findings

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def test_sst_prs119_fires(tmp_path: Path) -> None:
    files = {
        "skills/close/SKILL.md": FRONT.format(name="close"),
        "skills/close/inner/SKILL.md": FRONT.format(name="inner"),
    }
    [diagnostic] = skill_tree_findings(tmp_path, "SST-PRS119", files)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: nested skill folder at skills/close/inner/SKILL.md"
    assert diagnostic.subject == "skill:close"


def test_sst_prs119_silent(tmp_path: Path) -> None:
    files = {"skills/close/SKILL.md": FRONT.format(name="close"), "skills/open/SKILL.md": FRONT.format(name="open")}
    assert skill_tree_findings(tmp_path, "SST-PRS119", files) == []
