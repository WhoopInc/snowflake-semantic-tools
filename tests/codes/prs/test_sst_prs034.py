"""SST-PRS034: a SKILL.md frontmatter omits `name` or `description`."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity

FRONT = "---\nname: {name}\ndescription: {name} steps.\n---\nSteps.\n"


def _found(tmp_path: Path, code: str, files: dict[str, str]) -> list[Diagnostic]:
    for relative, text in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    return [item for item in catalog.diagnostics if item.code == code]


def test_sst_prs034_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(tmp_path, "SST-PRS034", {"skills/close/SKILL.md": "---\nname: close\n---\nSteps.\n"})
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "skill:close: SKILL.md frontmatter omits 'description'"
    assert diagnostic.subject == "skill:close"


def test_sst_prs034_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "SST-PRS034", {"skills/close/SKILL.md": FRONT.format(name="close")}) == []
