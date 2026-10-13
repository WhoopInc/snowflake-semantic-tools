"""The agent kit's pure planning: install, upgrade, remove, status, the lock, and the guard."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.agent_kit import (
    ActionKind,
    AgentInstall,
    AgentTarget,
    KitAction,
    KitLock,
    KitPlan,
    LockFormatError,
    SkillStatus,
    content_digest,
    lock_document,
    overlap_diagnostic,
    parse_lock,
    plan_install,
    plan_remove,
    plan_upgrade,
    published_overlap,
    render_kit_text,
    skill_statuses,
)
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.diagnostics.core import DEVELOPMENT_REF, REPOSITORY_URL

CORTEX = AgentTarget.CORTEX
CLAUDE = AgentTarget.CLAUDE
KIT = {"sst-core": {"SKILL.md": "core-1"}, "sst-fix": {"SKILL.md": "fix-1", "reference/codes.md": "codes-1"}}


def _kinds(plan: KitPlan) -> list[tuple[str, str, str]]:
    return [(action.kind.value, action.skill, action.path) for action in plan.actions]


def _codes(plan: KitPlan) -> list[str]:
    return [diagnostic.code for diagnostic in plan.diagnostics]


def _installed(skills: dict[str, dict[str, str]], selection: tuple[str, ...] = ()) -> KitLock:
    return KitLock("0.9.0", {CORTEX: AgentInstall(selection, skills)})


def test_each_agent_names_its_folder_and_skills_root() -> None:
    assert [(agent.folder, agent.skills_root) for agent in AgentTarget] == [
        (".cortex", ".cortex/skills"),
        (".claude", ".claude/skills"),
    ]


def test_a_digest_is_a_labelled_sha256_of_the_bytes() -> None:
    assert content_digest(b"abc") == "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


@pytest.mark.parametrize(("version", "ref"), [("1.2.3", "v1.2.3"), ("1.0.0.dev0", DEVELOPMENT_REF)])
def test_rendering_fills_every_token_for_the_installing_version(version: str, ref: str) -> None:
    text = render_kit_text("v@SST_VERSION@ docs @SST_DOCS@/x.md repo @SST_REPOSITORY@/issues", version)
    assert text == f"v{version} docs {REPOSITORY_URL}/blob/{ref}/docs/x.md repo {REPOSITORY_URL}/issues"


def test_rendering_leaves_sst_templates_alone() -> None:
    assert render_kit_text("{{ ref('orders') }}", "1.0.0") == "{{ ref('orders') }}"


@pytest.mark.parametrize(
    ("published", "found"),
    [
        ({"skills_dir": ".cortex/skills"}, ("skills_dir", ".cortex/skills")),
        ({"skills_dir": ".cortex"}, ("skills_dir", ".cortex")),
        ({"skills_dir": ".cortex/skills/mine"}, ("skills_dir", ".cortex/skills/mine")),
        ({"skills_dir": "./.Cortex/Skills/"}, ("skills_dir", "./.Cortex/Skills/")),
        ({"skills_dir": ".cortex\\skills"}, ("skills_dir", ".cortex\\skills")),
        ({"skills_dir": "."}, ("skills_dir", ".")),
        ({"skills_dir": "", "plugins_dir": ".cortex"}, ("plugins_dir", ".cortex")),
        ({"skills_dir": "skills", "plugins_dir": "plugins"}, None),
        ({"skills_dir": ".cortex/skill"}, None),
        ({"skills_dir": ".cortex/skillsets"}, None),
    ],
)
def test_the_kit_folder_may_not_overlap_a_published_directory(
    published: dict[str, str], found: tuple[str, str] | None
) -> None:
    assert published_overlap(".cortex/skills", published) == found


def test_an_overlap_is_an_error_no_setting_can_demote() -> None:
    diagnostic = overlap_diagnostic(".cortex/skills", "skills_dir", ".cortex")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG052", Severity.ERROR)
    assert diagnostic.message == ".cortex/skills overlaps project.skills_dir (.cortex), a directory SST publishes from"
    assert diagnostic.subject == "config:project"


def test_a_location_names_the_folder_or_the_file() -> None:
    assert KitAction(ActionKind.CONFLICT, CLAUDE, "sst-core").location == ".claude/skills/sst-core"
    assert KitAction(ActionKind.CREATE, CORTEX, "sst-core", "SKILL.md").location == ".cortex/skills/sst-core/SKILL.md"


def test_install_creates_every_kit_file_and_records_the_whole_kit() -> None:
    plan = plan_install(KIT, KitLock(""), {}, CORTEX, version="1.0.0")
    assert _kinds(plan) == [
        ("create", "sst-core", "SKILL.md"),
        ("create", "sst-fix", "SKILL.md"),
        ("create", "sst-fix", "reference/codes.md"),
    ]
    assert plan.lock == KitLock("1.0.0", {CORTEX: AgentInstall((), KIT)})
    assert not plan.refused
    assert plan.diagnostics == ()


def test_install_by_name_records_the_names_asked_for() -> None:
    plan = plan_install(KIT, KitLock(""), {}, CORTEX, wanted=("sst-fix", "sst-fix"), version="1.0.0")
    assert {action.skill for action in plan.actions} == {"sst-fix"}
    assert plan.lock.agents[CORTEX].selection == ("sst-fix",)


def test_install_skips_an_installed_skill_whatever_its_state() -> None:
    lock = _installed({"sst-core": {"SKILL.md": "core-0"}})
    plan = plan_install(KIT, lock, {"sst-core": {"SKILL.md": "edited"}}, CORTEX, version="1.0.0")
    assert plan.skipped == ("sst-core",)
    assert {action.skill for action in plan.actions} == {"sst-fix"}
    assert plan.lock.agents[CORTEX].skills["sst-core"] == {"SKILL.md": "core-0"}


def test_install_adopts_a_folder_that_already_holds_the_kit_exactly() -> None:
    disk = {"sst-core": {"SKILL.md": "core-1", "notes.md": "mine"}}
    plan = plan_install(KIT, KitLock(""), disk, CORTEX, wanted=("sst-core",), version="1.0.0")
    assert _kinds(plan) == [("adopt", "sst-core", "SKILL.md")]
    assert plan.lock.agents[CORTEX].skills == {"sst-core": {"SKILL.md": "core-1"}}


def test_install_refuses_everything_when_a_folder_it_did_not_write_is_in_the_way() -> None:
    lock = KitLock("0.9.0")
    plan = plan_install(KIT, lock, {"sst-fix": {"SKILL.md": "someone-else"}}, CORTEX, version="1.0.0")
    assert plan.refused
    assert plan.lock is lock
    assert ("conflict", "sst-fix", "") in _kinds(plan)
    [diagnostic] = plan.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRT013", Severity.ERROR)
    assert diagnostic.message == ".cortex/skills/sst-fix: the folder exists and SST did not write it"


@pytest.mark.parametrize(
    ("before", "asked", "after"),
    [
        ((), ("sst-fix",), ()),
        (("sst-core",), (), ()),
        (("sst-core",), ("sst-fix",), ("sst-core", "sst-fix")),
    ],
)
def test_install_widens_the_selection_and_never_narrows_it(
    before: tuple[str, ...], asked: tuple[str, ...], after: tuple[str, ...]
) -> None:
    lock = _installed({"sst-core": KIT["sst-core"]}, before)
    plan = plan_install(KIT, lock, {"sst-core": KIT["sst-core"]}, CORTEX, wanted=asked, version="1.0.0")
    assert plan.lock.agents[CORTEX].selection == after


def test_install_keeps_other_agents_and_lists_them_by_name() -> None:
    lock = KitLock("0.9.0", {CORTEX: AgentInstall((), {"sst-core": KIT["sst-core"]})})
    plan = plan_install(KIT, lock, {}, CLAUDE, version="1.0.0")
    assert list(plan.lock.agents) == [CLAUDE, CORTEX]
    assert plan.lock.agents[CORTEX] == lock.agents[CORTEX]


def test_upgrade_does_nothing_to_a_current_skill() -> None:
    lock = _installed(dict(KIT))
    plan = plan_upgrade(KIT, lock, dict(KIT), CORTEX, force=False, version="1.0.0")
    assert plan.actions == ()
    assert plan.lock is lock


def test_a_run_that_only_skips_leaves_the_lock_as_it_was() -> None:
    lock = _installed(dict(KIT))
    plan = plan_install(KIT, lock, dict(KIT), CORTEX, version="1.0.0")
    assert plan.skipped == ("sst-core", "sst-fix")
    assert plan.lock is lock


@pytest.mark.parametrize(
    ("found", "new", "force", "kind", "recorded", "codes"),
    [
        pytest.param("old", "new", False, "replace", "new", [], id="untouched-and-changed"),
        pytest.param("new", "new", False, "adopt", "new", [], id="already-the-kit"),
        pytest.param("mine", "old", False, "keep", "old", [], id="edited-kit-unchanged"),
        pytest.param(None, "old", False, "keep", "old", [], id="deleted-kit-unchanged"),
        pytest.param("mine", "new", False, "keep", "old", ["SST-PRT014"], id="edited-and-changed"),
        pytest.param("mine", "new", True, "replace", "new", [], id="edited-and-forced"),
        pytest.param("mine", "old", True, "replace", "old", [], id="edited-kit-unchanged-forced"),
        pytest.param(None, "new", True, "create", "new", [], id="deleted-and-forced"),
    ],
)
def test_upgrade_decides_each_file_from_disk_lock_and_kit(
    found: str | None, new: str, force: bool, kind: str, recorded: str, codes: list[str]
) -> None:
    lock = _installed({"sst-core": {"SKILL.md": "old"}})
    disk = {"sst-core": {} if found is None else {"SKILL.md": found}}
    plan = plan_upgrade({"sst-core": {"SKILL.md": new}}, lock, disk, CORTEX, force=force, version="1.0.0")
    assert _kinds(plan) == [(kind, "sst-core", "SKILL.md")]
    assert plan.lock.agents[CORTEX].skills == {"sst-core": {"SKILL.md": recorded}}
    assert _codes(plan) == codes


def test_a_kept_file_says_why() -> None:
    lock = _installed({"sst-core": {"SKILL.md": "old"}})
    plan = plan_upgrade(
        {"sst-core": {"SKILL.md": "new"}}, lock, {"sst-core": {"SKILL.md": "mine"}}, CORTEX, force=False, version="1"
    )
    [diagnostic] = plan.diagnostics
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        ".cortex/skills/sst-core/SKILL.md changed since SST wrote it, so it is kept: the kit changed it too"
    )


@pytest.mark.parametrize(
    ("found", "kind", "recorded"),
    [(None, "create", {"SKILL.md": "s", "new.md": "n"}), ("n", "adopt", {"SKILL.md": "s", "new.md": "n"})],
)
def test_upgrade_adds_a_file_the_kit_adds(found: str | None, kind: str, recorded: dict[str, str]) -> None:
    lock = _installed({"sst-core": {"SKILL.md": "s"}})
    disk = {"sst-core": {"SKILL.md": "s"} | ({} if found is None else {"new.md": found})}
    plan = plan_upgrade({"sst-core": {"SKILL.md": "s", "new.md": "n"}}, lock, disk, CORTEX, force=False, version="1")
    assert _kinds(plan) == [(kind, "sst-core", "new.md")]
    assert plan.lock.agents[CORTEX].skills["sst-core"] == recorded


def test_upgrade_refuses_when_a_file_the_kit_adds_is_in_the_way() -> None:
    lock = _installed({"sst-core": {"SKILL.md": "s"}})
    disk = {"sst-core": {"SKILL.md": "s", "new.md": "someone-else"}}
    plan = plan_upgrade({"sst-core": {"SKILL.md": "s", "new.md": "n"}}, lock, disk, CORTEX, force=True, version="1")
    assert plan.refused
    assert plan.lock is lock
    assert plan.diagnostics[0].message == ".cortex/skills/sst-core/new.md: the file exists and SST did not write it"


@pytest.mark.parametrize(
    ("found", "force", "kind", "recorded", "codes"),
    [
        pytest.param("d", False, "remove", {"SKILL.md": "s"}, [], id="untouched"),
        pytest.param(None, False, "remove", {"SKILL.md": "s"}, [], id="already-gone"),
        pytest.param("mine", False, "keep", {"SKILL.md": "s", "drop.md": "d"}, ["SST-PRT014"], id="edited"),
        pytest.param("mine", True, "remove", {"SKILL.md": "s"}, [], id="edited-and-forced"),
    ],
)
def test_upgrade_removes_a_file_the_kit_drops(
    found: str | None, force: bool, kind: str, recorded: dict[str, str], codes: list[str]
) -> None:
    lock = _installed({"sst-core": {"SKILL.md": "s", "drop.md": "d"}})
    disk = {"sst-core": {"SKILL.md": "s"} | ({} if found is None else {"drop.md": found})}
    plan = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, disk, CORTEX, force=force, version="1")
    assert _kinds(plan) == [(kind, "sst-core", "drop.md")]
    assert plan.lock.agents[CORTEX].skills["sst-core"] == recorded
    assert _codes(plan) == codes


def test_upgrade_installs_a_skill_the_kit_adds_only_for_the_whole_kit() -> None:
    disk = {"sst-core": KIT["sst-core"]}
    whole = plan_upgrade(KIT, _installed({"sst-core": KIT["sst-core"]}), disk, CORTEX, force=False, version="1")
    named = plan_upgrade(
        KIT, _installed({"sst-core": KIT["sst-core"]}, ("sst-core",)), disk, CORTEX, force=False, version="1"
    )
    assert {action.skill for action in whole.actions if action.kind is ActionKind.CREATE} == {"sst-fix"}
    assert "sst-fix" in whole.lock.agents[CORTEX].skills
    assert named.actions == ()
    assert "sst-fix" not in named.lock.agents[CORTEX].skills


def test_upgrade_refuses_when_a_skill_the_kit_adds_meets_a_folder_in_the_way() -> None:
    disk = {"sst-core": KIT["sst-core"], "sst-fix": {"SKILL.md": "someone-else"}}
    plan = plan_upgrade(KIT, _installed({"sst-core": KIT["sst-core"]}), disk, CORTEX, force=True, version="1")
    assert plan.refused
    assert _codes(plan) == ["SST-PRT013"]


def test_upgrade_restores_a_deleted_folder_only_when_forced() -> None:
    lock = _installed({"sst-core": {"SKILL.md": "s"}})
    kept = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, {}, CORTEX, force=False, version="1")
    forced = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, {}, CORTEX, force=True, version="1")
    assert _kinds(kept) == [("keep", "sst-core", "SKILL.md")]
    assert _kinds(forced) == [("create", "sst-core", "SKILL.md")]


def test_upgrade_retires_a_skill_the_kit_no_longer_ships() -> None:
    lock = _installed(
        {"sst-core": {"SKILL.md": "s"}, "sst-old": {"SKILL.md": "o", "x.md": "x"}}, ("sst-core", "sst-old")
    )
    disk = {"sst-core": {"SKILL.md": "s"}, "sst-old": {"SKILL.md": "o", "x.md": "edited"}}
    plan = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, disk, CORTEX, force=False, version="1")
    assert _kinds(plan) == [("remove", "sst-old", "SKILL.md"), ("keep", "sst-old", "x.md")]
    assert plan.lock.agents[CORTEX].skills["sst-old"] == {"x.md": "x"}
    assert plan.lock.agents[CORTEX].selection == ("sst-core", "sst-old")
    gone = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, {}, CORTEX, force=False, version="1")
    assert "sst-old" not in gone.lock.agents[CORTEX].skills
    assert gone.lock.agents[CORTEX].selection == ("sst-core",)


def test_upgrade_drops_an_agent_left_with_nothing() -> None:
    lock = _installed({"sst-old": {"SKILL.md": "o"}}, ("sst-old",))
    plan = plan_upgrade({"sst-core": {"SKILL.md": "s"}}, lock, {}, CORTEX, force=False, version="1")
    assert _kinds(plan) == [("remove", "sst-old", "SKILL.md")]
    assert plan.lock.agents == {}


def test_remove_removes_every_untouched_file_and_the_agent() -> None:
    lock = _installed(dict(KIT))
    plan = plan_remove(lock, dict(KIT), CORTEX, force=False, version="1.0.0")
    assert [kind for kind, _, _ in _kinds(plan)] == ["remove", "remove", "remove"]
    assert plan.lock == KitLock("1.0.0", {})


def test_remove_by_name_leaves_the_others_and_narrows_the_selection() -> None:
    lock = _installed(dict(KIT))
    plan = plan_remove(lock, dict(KIT), CORTEX, wanted=("sst-fix", "sst-nope"), force=False, version="1")
    assert {action.skill for action in plan.actions} == {"sst-fix"}
    assert plan.skipped == ("sst-nope",)
    assert plan.lock.agents[CORTEX] == AgentInstall(("sst-core",), {"sst-core": KIT["sst-core"]})


@pytest.mark.parametrize(("force", "kept"), [(False, True), (True, False)])
def test_remove_keeps_an_edited_file_unless_forced(force: bool, kept: bool) -> None:
    lock = _installed({"sst-core": {"SKILL.md": "s"}})
    plan = plan_remove(lock, {"sst-core": {"SKILL.md": "mine"}}, CORTEX, force=force, version="1")
    assert ("sst-core" in plan.lock.agents.get(CORTEX, AgentInstall((), {})).skills) is kept
    assert _codes(plan) == (["SST-PRT014"] if kept else [])


def test_status_reports_every_state() -> None:
    lock = _installed(
        {
            "sst-current": {"SKILL.md": "c"},
            "sst-outdated": {"SKILL.md": "o"},
            "sst-modified": {"SKILL.md": "m"},
            "sst-missing": {"SKILL.md": "g"},
            "sst-dropped": {"SKILL.md": "d"},
        }
    )
    bundle = {
        "sst-current": {"SKILL.md": "c"},
        "sst-outdated": {"SKILL.md": "o2"},
        "sst-modified": {"SKILL.md": "m"},
        "sst-missing": {"SKILL.md": "g"},
        "sst-new": {"SKILL.md": "n"},
    }
    disk = {
        "sst-current": {"SKILL.md": "c"},
        "sst-outdated": {"SKILL.md": "o"},
        "sst-modified": {"SKILL.md": "edited"},
        "sst-dropped": {"SKILL.md": "d"},
    }
    states = {status.name: status.state for status in skill_statuses(bundle, lock, disk, CORTEX)}
    assert states == {
        "sst-current": "current",
        "sst-dropped": "outdated",
        "sst-missing": "missing",
        "sst-modified": "modified",
        "sst-new": "not installed",
        "sst-outdated": "outdated",
    }
    new = next(status for status in skill_statuses(bundle, lock, disk, CORTEX) if status.name == "sst-new")
    assert new.outdated


def test_status_for_an_agent_with_nothing_installed_is_not_outdated() -> None:
    statuses = skill_statuses(KIT, KitLock(""), {}, CLAUDE)
    assert statuses == (SkillStatus("sst-core", installed=False), SkillStatus("sst-fix", installed=False))


def test_status_for_a_named_selection_lists_only_it() -> None:
    lock = _installed({"sst-core": KIT["sst-core"]}, ("sst-core",))
    assert [status.name for status in skill_statuses(KIT, lock, {"sst-core": KIT["sst-core"]}, CORTEX)] == ["sst-core"]


def test_the_lock_round_trips_through_its_document() -> None:
    lock = KitLock(
        "1.0.0", {CLAUDE: AgentInstall(("sst-core",), {"sst-core": {"SKILL.md": "c"}}), CORTEX: AgentInstall((), KIT)}
    )
    document = lock_document(lock)
    assert document["version"] == 1
    assert document["agents"]["claude"] == {  # type: ignore[index]
        "root": ".claude/skills",
        "selection": ["sst-core"],
        "skills": {"sst-core": {"SKILL.md": "c"}},
    }
    assert parse_lock(document) == lock


def test_a_lock_that_does_not_say_who_wrote_it_reads_as_empty() -> None:
    assert parse_lock({"version": 1, "agents": {}}) == KitLock("")


@pytest.mark.parametrize(
    ("document", "reason"),
    [
        ([], "it is not a version 1 agent-kit lock"),
        ({"version": 2, "agents": {}}, "it is not a version 1 agent-kit lock"),
        ({"version": 1}, "it needs an agents object"),
        ({"version": 1, "agents": {"vim": {}}}, "it names an agent the kit does not know: 'vim'"),
        ({"version": 1, "agents": {"cortex": []}}, "agent 'cortex' is not an object"),
        ({"version": 1, "agents": {"cortex": {"selection": "x", "skills": {}}}}, "agent 'cortex' needs a selection"),
        ({"version": 1, "agents": {"cortex": {"selection": [1], "skills": {}}}}, "agent 'cortex' needs a selection"),
        ({"version": 1, "agents": {"cortex": {"skills": []}}}, "agent 'cortex' needs a skills object"),
        ({"version": 1, "agents": {"cortex": {"skills": {"a": []}}}}, "skill 'a' of agent 'cortex' needs"),
        ({"version": 1, "agents": {"cortex": {"skills": {"a": {"x": 1}}}}}, "skill 'a' of agent 'cortex' needs"),
    ],
)
def test_a_malformed_lock_is_refused_with_its_reason(document: object, reason: str) -> None:
    with pytest.raises(LockFormatError, match=reason):
        parse_lock(document)
