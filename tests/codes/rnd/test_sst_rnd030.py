"""SST-RND030: a rendered skill or plugin bundle names a path it does not hold."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillFile
from snowflake_semantic_tools.domain.render.skill_bundle import build_plugin_bundle, build_skill_bundle


def skill(*files: SkillFile, body: str = "Body.\n") -> Skill:
    return Skill("close", "skills/close", "close", "Close.", body, files, Origin("skills/close/SKILL.md"))


def test_sst_rnd030_fires() -> None:
    bundle, found = build_skill_bundle(skill(SkillFile("notes.md", b"Notes.\n")))
    [diagnostic] = [item for item in found if item.code == "SST-RND030"]
    assert bundle is None
    assert (diagnostic.severity, diagnostic.subject) == (Severity.ERROR, "skill:close")
    assert diagnostic.message == "skill 'close': 'SKILL.md' does not exist at render"


def test_sst_rnd030_fires_for_a_plugin_with_no_bundled_member() -> None:
    plugin = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("close",), Origin("plugin.yml"))
    bundle, [diagnostic] = build_plugin_bundle(plugin, {})
    assert bundle is None and diagnostic.code == "SST-RND030"
    assert diagnostic.message == "skill 'kit': './skills/' does not exist at render"


def test_sst_rnd030_silent() -> None:
    bundle, found = build_skill_bundle(skill(SkillFile("SKILL.md", b"---\nname: close\n---\nBody.\n")))
    assert bundle is not None and not [item for item in found if item.code == "SST-RND030"]
