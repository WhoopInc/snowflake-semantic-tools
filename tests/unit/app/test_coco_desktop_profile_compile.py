"""Profile compile: plugin checks without the catalog channel, and what keeps a profile back."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompileCocoDesktopProfiles, DesktopChannel
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.coco_desktop_profile import CocoDesktopProfile, CocoDesktopProfileCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from tests.helpers.compile_builders import plugin

CHANNEL = DesktopChannel(QualifiedName.parse("DB.S.PROFILES"), QualifiedName.parse("DB.S.PROFILE_REGISTRY"))


def skill(name: str, *unreferenced: str) -> Skill:
    files = (
        SkillFile("SKILL.md", f"---\nname: {name}\n---\nBody.\n".encode()),
        *(SkillFile(path, b"x\n") for path in unreferenced),
    )
    return Skill(name, f"skills/{name}", name, "d", "Body.\n", files, Origin(f"skills/{name}/SKILL.md"))


def profile(name: str, *plugins: str) -> CocoDesktopProfile:
    origin = Origin(f"coco_desktop_profiles/{name}/coco_desktop_profile.yml", 1)
    return CocoDesktopProfile(
        name, f"coco_desktop_profiles/{name}", "P.", "Data", (), (), (), None, origin, plugins=plugins
    )


def compile_profiles(
    profiles: tuple[CocoDesktopProfile, ...], skills: SkillCatalog, *, blocked: frozenset[str] = frozenset()
) -> tuple[list[str], list[tuple[str, str | None]]]:
    result = CompileCocoDesktopProfiles(
        CocoDesktopProfileCatalog(profiles), skills, CHANNEL, catalog_published=frozenset(), blocked_skills=blocked
    ).run_result()
    codes = [(item.code, item.subject) for item in result.diagnostics]
    return [item.artifact_key for item in result.compiled], codes


def test_without_the_catalog_channel_each_member_is_flattened_once_and_healthy_plugins_publish() -> None:
    skills = SkillCatalog(
        (skill("shared-skill", "notes.md"), skill("solo")),
        (plugin("kit", "shared-skill", "solo", "absent"), plugin("other-kit", "shared-skill")),
    )

    compiled, codes = compile_profiles((profile("analyst", "kit", "other-kit"),), skills)

    assert compiled == ["coco_desktop_profile:analyst"]
    # The skill file checks are validate_skill_catalog's, so a profile reports none of them,
    # however many plugins carry the skill.
    assert "SST-VAL813" not in {code for code, _ in codes}
    assert "SST-VAL855" not in {code for code, _ in codes}


def test_an_undeclared_plugin_is_skipped_by_the_checks_and_blocks_only_its_profile() -> None:
    skills = SkillCatalog((skill("solo"),), (plugin("kit", "solo"),))

    compiled, codes = compile_profiles((profile("analyst", "kit"), profile("ghost", "ghost-kit")), skills)

    assert compiled == ["coco_desktop_profile:analyst"]
    assert ("SST-VAL860", "coco_desktop_profile:ghost") in codes
    assert "SST-VAL855" not in {code for code, _ in codes}


def test_a_plugin_holding_a_blocked_skill_blocks_every_profile_that_names_it() -> None:
    skills = SkillCatalog((skill("solo"), skill("other")), (plugin("kit", "solo"), plugin("fine", "other")))

    compiled, codes = compile_profiles(
        (profile("analyst", "kit"), profile("viewer", "fine")), skills, blocked=frozenset(("solo",))
    )

    assert compiled == ["coco_desktop_profile:viewer"]
    assert ("SST-VAL855", "coco_desktop_profile:analyst") in codes
