"""SST-PRS119: a skill folder holds another skill folder below its root."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.file_trees import write_tree

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def _found(tmp_path: Path, code: str, files: dict[str, str]) -> list[Diagnostic]:
    write_tree(tmp_path, files)
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    return [item for item in catalog.diagnostics if item.code == code]


def test_sst_prs119_fires(tmp_path: Path) -> None:
    files = {
        "skills/close/SKILL.md": FRONT.format(name="close"),
        "skills/close/inner/SKILL.md": FRONT.format(name="inner"),
    }
    [diagnostic] = _found(tmp_path, "SST-PRS119", files)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: nested skill folder at skills/close/inner/SKILL.md"
    assert diagnostic.subject == "skill:close"


def test_sst_prs119_silent(tmp_path: Path) -> None:
    files = {"skills/close/SKILL.md": FRONT.format(name="close"), "skills/open/SKILL.md": FRONT.format(name="open")}
    assert _found(tmp_path, "SST-PRS119", files) == []
