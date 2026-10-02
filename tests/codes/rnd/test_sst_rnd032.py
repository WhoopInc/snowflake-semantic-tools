"""SST-RND032: a skill's SKILL.md is within budget as authored and over it once flattened."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.skill import Skill, SkillFile
from snowflake_semantic_tools.domain.render.skill_bundle import SKILL_MD_BUDGET_BYTES, build_skill_bundle


def skill(*files: SkillFile, body: str = "Body.\n") -> Skill:
    return Skill("close", "skills/close", "close", "Close.", body, files, Origin("skills/close/SKILL.md"))


def skill_md(filler: int) -> Skill:
    references = "[a](scripts/a.md)\n" * 100
    head = "---\nname: close\n---\n"
    text = head + "x" * filler + "\n" + references
    return skill(SkillFile("SKILL.md", text.encode()), SkillFile("scripts/a.md", b"A.\n"), body=text[len(head) :])


def test_sst_rnd032_fires() -> None:
    authored = skill_md(SKILL_MD_BUDGET_BYTES - len(skill_md(0).files[0].content))
    assert authored.files[0].size == SKILL_MD_BUDGET_BYTES
    _, found = build_skill_bundle(authored)
    [diagnostic] = [item for item in found if item.code == "SST-RND032"]
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == f"skill 'close': rendered body is {SKILL_MD_BUDGET_BYTES + 100} bytes, over {SKILL_MD_BUDGET_BYTES}"
    )


def test_sst_rnd032_silent() -> None:
    # Over budget as authored is SST-VAL812's to report, so the rendered size is not reported twice.
    _, found = build_skill_bundle(skill_md(SKILL_MD_BUDGET_BYTES))
    assert "SST-VAL812" in [item.code for item in found] and "SST-RND032" not in [item.code for item in found]
    _, small = build_skill_bundle(skill_md(10))
    assert "SST-RND032" not in [item.code for item in small]
