"""Desktop profiles render content-addressed trees and one Desktop-shaped registry row."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.profile import (
    DESKTOP_REGISTRY,
    CommandFile,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
    StageTree,
    assemble_prompt,
    build_profile,
)
from snowflake_semantic_tools.domain.model.skill import BundleEntry, Plugin, Skill, SkillFile, build_plugin_bundle
from snowflake_semantic_tools.domain.validate.profile import (
    desktop_registry_diagnostics,
    unreached_skills,
    validate_profile_catalog,
)

ORIGIN = Origin("profiles/analyst/profile.yml", 1)


def skill(name: str) -> Skill:
    files = (SkillFile("SKILL.md", f"---\nname: {name}\n---\nbody\n".encode()), SkillFile("reference/a.md", b"a"))
    return Skill(name, f"skills/{name}", name, "d", "body", files, Origin(f"skills/{name}/SKILL.md"))


SKILLS = {name: skill(name) for name in ("common", "semantics", "operations")}


def profile(name: str = "analyst", **fields: object) -> DesktopProfile:
    values: dict[str, object] = {
        "name": name,
        "directory": f"profiles/{name}",
        "description": "Analyst.",
        "owner_team": "Data",
        "skills": ("semantics",),
        "mcp_servers": (),
        "hooks": (),
        "prompt": None,
        "origin": ORIGIN,
        "source_files": (f"profiles/{name}/profile.yml",),
    }
    values.update(fields)
    return DesktopProfile(**values)  # type: ignore[arg-type]


SHARED = SharedProfile(
    "Shared rules.\n",
    (("b.md", "Rule B."), ("a.md", "Rule A.")),
    ("common",),
    Origin("profiles/shared/profile.yml", 1),
    ("profiles/shared/AGENTS.md",),
)
HOOK = HookDefinition(
    "sql-safety",
    "hooks/sql-safety",
    "PreToolUse",
    "bash",
    SkillFile("check.sh", b"#!/bin/sh\n"),
    Origin("hooks/sql-safety/hook.yml", 1),
    matcher="snowflake_sql_execute",
    timeout=30,
    interactive=False,
)
START = HookDefinition("warm", "hooks/warm", "SessionStart", "bash", SkillFile("warm.sh", b"echo\n"), Origin("h"))
SECOND = HookDefinition(
    "audit",
    "hooks/audit",
    "PreToolUse",
    "bash",
    SkillFile("audit.sh", b"x"),
    Origin("h"),
    matcher="snowflake_sql_execute",
)
MCP = McpConfig(
    "dbt", "mcp-servers/dbt/mcp.json", {"dbt": {"command": "dbt-mcp", "env": {"TOKEN": "${DBT_TOKEN}"}}}, Origin("m")
)


def test_stage_tree_prefix_names_its_own_digest() -> None:
    tree = StageTree("skills", "analyst", (BundleEntry("a/SKILL.md", b"x"),))
    assert tree.prefix == f"skills/analyst/{tree.digest[:12]}/"
    assert tree.paths == ("a/SKILL.md",)


def test_prompt_assembles_shared_then_sorted_rules_then_profile() -> None:
    assert assemble_prompt(SHARED, profile(prompt="Profile.\n")) == "Shared rules.\n\nRule A.\n\nRule B.\n\nProfile.\n"
    assert assemble_prompt(None, profile()) is None
    assert assemble_prompt(None, profile(prompt="Only.")) == "Only.\n"


def test_build_profile_renders_trees_pointers_and_a_content_version() -> None:
    catalog = ProfileCatalog((), SHARED, (HOOK, START, SECOND), (MCP,))
    release = build_profile(
        profile(
            skills=("semantics", "common"), mcp_servers=("dbt",), hooks=("sql-safety", "warm", "audit"), prompt="P"
        ),
        catalog,
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
    )
    kinds = [(tree.kind, tree.scope, tree.paths) for tree in release.trees]
    assert kinds == [
        ("skills", "shared", ("common/SKILL.md", "common/reference/a.md")),
        ("skills", "analyst", ("semantics/SKILL.md", "semantics/reference/a.md")),
        ("prompts", "analyst", ("AGENTS.md",)),
        ("mcp", "analyst", ("mcp.json",)),
        ("hooks", "analyst", ("audit/audit.sh", "sql-safety/check.sh", "warm/warm.sh")),
    ]
    row = release.row
    assert row["SKILL_REPOS"] == [
        {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[0].prefix}"},
        {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[1].prefix}"},
    ]
    assert row["SYSTEM_PROMPT_REPO"] == {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[2].prefix}AGENTS.md"}
    assert row["MCP_SERVERS"] == {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[3].prefix}mcp.json"}
    hooks = row["HOOKS"]
    assert isinstance(hooks, dict)
    pre = hooks["PreToolUse"]
    assert len(pre) == 1 and pre[0]["matcher"] == "snowflake_sql_execute"
    assert [entry["source"]["snowflake_stage"].rsplit("/", 2)[-2] for entry in pre[0]["hooks"]] == [
        "sql-safety",
        "audit",
    ]
    assert pre[0]["hooks"][0]["timeout"] == 30 and pre[0]["hooks"][0]["interactive"] is False
    assert hooks["SessionStart"] == [
        {
            "hooks": [
                {
                    "type": "command",
                    "command": "bash",
                    "source": {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[4].prefix}warm/warm.sh"},
                }
            ]
        }
    ]
    assert (row["PLUGINS"], row["COMMAND_REPOS"], row["ENV_VARS"], row["SETTINGS_OVERRIDES"]) == ([], [], {}, {})
    assert release.version == row["VERSION"] and release.version.startswith("SST_")
    assert release.key == "profile:analyst"
    assert "mcp-servers/dbt/mcp.json" in release.source_files
    document = json.loads(release.document())
    assert document["row"]["CONFIG_NAME"] == "analyst" and len(document["trees"]) == 5

    bare = build_profile(
        profile(skills=(), mcp_servers=("missing",)),
        ProfileCatalog(),
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
    )
    assert bare.trees == ()
    assert (bare.row["SKILL_REPOS"], bare.row["SYSTEM_PROMPT_REPO"], bare.row["MCP_SERVERS"], bare.row["HOOKS"]) == (
        [],
        None,
        {},
        {},
    )
    changed = build_profile(
        profile(skills=(), mcp_servers=("missing",)),
        ProfileCatalog(),
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
    )
    assert changed.version == bare.version
    renamed = build_profile(
        profile(skills=(), mcp_servers=("missing",), description="Other."),
        ProfileCatalog(),
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
    )
    assert renamed.version != bare.version


def test_validation_covers_members_hooks_mcp_shape_and_credentials() -> None:
    placeholder = McpConfig("amplitude", "mcp-servers/amplitude/mcp.json", {"amplitude": "TODO"}, Origin("a"))
    clash = McpConfig("dbt-copy", "mcp-servers/dbt-copy/mcp.json", {"dbt": {"command": "x"}}, Origin("c"))
    leaky = McpConfig(
        "leaky",
        "mcp-servers/leaky/mcp.json",
        {"svc": {"env": {"API_KEY": "abc123", "token_list": ["plain"], "SAFE": "x"}, "args": ["--password", "p"]}},
        Origin("l"),
    )
    catalog = ProfileCatalog(
        (
            profile(
                skills=("semantics", "common", "ghost"),
                hooks=("nope", "sql-safety"),
                mcp_servers=("amplitude", "dbt", "dbt-copy", "missing"),
            ),
            profile("shared"),
            profile("Bad Name"),
        ),
        SharedProfile(None, (), ("common", "vanished"), Origin("profiles/shared/profile.yml")),
        (HOOK,),
        (MCP, placeholder, clash, leaky),
    )
    found = [(item.code, item.subject, item.context.get("name")) for item in validate_profile_catalog(catalog, SKILLS)]
    assert found == [
        ("SST-VAL844", "profile:shared", "vanished"),
        ("SST-VAL845", "profile:analyst", "common"),
        ("SST-VAL844", "profile:analyst", "ghost"),
        ("SST-VAL846", "profile:analyst", "nope"),
        ("SST-VAL848", "profile:analyst", "amplitude"),
        ("SST-VAL849", "profile:analyst", "dbt"),
        ("SST-VAL847", "profile:analyst", "missing"),
        ("SST-VAL801", "profile:shared", None),
        ("SST-VAL801", "profile:Bad Name", None),
        ("SST-VAL850", "mcp:leaky", "svc"),
        ("SST-VAL850", "mcp:leaky", "svc"),
    ]
    assert validate_profile_catalog(ProfileCatalog((profile(),)), SKILLS) == ()


def test_hooks_and_mcp_configs_that_failed_to_load_are_not_reported_unknown() -> None:
    loader_errors = DiagnosticBag(
        (
            D(
                "SST-VAL852",
                origin=Origin("hooks/guard"),
                subject="hook:guard",
                artifact="guard",
                detail="has no script",
            ),
            D("SST-VAL853", origin=Origin("mcp-servers/docs"), subject="mcp:docs", artifact="docs", detail="bad"),
        )
    )
    catalog = ProfileCatalog((profile(hooks=("guard",), mcp_servers=("docs",)),), None, (), (), loader_errors)
    codes = [item.code for item in validate_profile_catalog(catalog, SKILLS)]
    assert codes == ["SST-VAL852", "SST-VAL853"]


def test_hook_scripts_a_stage_rejects_fail_validation_on_the_hook() -> None:
    unsafe = HookDefinition("guard", "hooks/guard", "PreToolUse", "bash", SkillFile("check me.sh", b"x"), Origin("g"))
    catalog = ProfileCatalog((profile(hooks=("guard",)),), None, (unsafe,), ())
    found = [(item.code, item.subject) for item in validate_profile_catalog(catalog, SKILLS)]
    assert found == [("SST-VAL857", "hook:guard")]


def command(path: str, content: str = "Run the check.\n") -> CommandFile:
    return CommandFile(path, content.encode(), f"commands/{path}", Origin(f"commands/{path}", 1))


KIT = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("operations",), Origin("p"))


def test_commands_and_plugins_publish_as_trees_named_by_the_row() -> None:
    shared = SharedProfile(None, (), (), Origin("s"), commands=("daily",))
    catalog = ProfileCatalog((), shared, commands=(command("daily.md"), command("sql/check.md")))
    release = build_profile(
        profile(commands=("sql/check",), plugins=("kit",)),
        catalog,
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
        plugins={"kit": KIT},
    )
    trees = {(tree.kind, tree.scope): tree for tree in release.trees}
    assert trees[("commands", "shared")].paths == ("daily.md",)
    assert trees[("commands", "analyst")].paths == ("sql/check.md",)
    plugin_tree = trees[("plugins", "analyst")]
    bundle, _ = build_plugin_bundle(KIT, SKILLS)
    assert bundle is not None
    # The profile copy is byte for byte the bundle the plugin's extension publishes.
    assert plugin_tree.entries == tuple(BundleEntry(f"kit/{entry.path}", entry.content) for entry in bundle.entries)
    assert release.row["COMMAND_REPOS"] == [
        {"snowflake_stage": f"@DB.S.PROFILES/{trees[('commands', 'shared')].prefix}"},
        {"snowflake_stage": f"@DB.S.PROFILES/{trees[('commands', 'analyst')].prefix}"},
    ]
    assert release.row["PLUGINS"] == [f"@DB.S.PROFILES/{plugin_tree.prefix}kit/"]
    assert {"commands/daily.md", "commands/sql/check.md", "plugins/kit/plugin.yml"} <= set(release.source_files)

    plain = build_profile(profile(), ProfileCatalog(), SKILLS, stage="DB.S.PROFILES", version_prefix="SST_")
    assert (plain.row["PLUGINS"], plain.row["COMMAND_REPOS"]) == ([], [])
    missing = build_profile(
        profile(plugins=("ghost",)), ProfileCatalog(), SKILLS, stage="DB.S.PROFILES", version_prefix="SST_"
    )
    assert missing.version == plain.version and missing.trees == plain.trees


def test_command_and_plugin_references_are_validated() -> None:
    shared = SharedProfile(None, (), (), Origin("s"), commands=("daily", "gone"))
    unsafe = command("bad name.md")
    loader_error = D("SST-VAL859", origin=Origin("c"), subject="command:broken", artifact="broken", detail="x")
    catalog = ProfileCatalog(
        (profile(commands=("daily", "nope", "broken", "own"), plugins=("kit", "ghost")),),
        shared,
        commands=(command("daily.md"), command("own.md"), unsafe),
        diagnostics=DiagnosticBag((loader_error,)),
    )
    found = [
        (item.code, item.subject, item.context.get("name"))
        for item in validate_profile_catalog(catalog, SKILLS, {"kit": KIT})
    ]
    assert found == [
        ("SST-VAL859", "command:broken", None),
        ("SST-VAL858", "profile:shared", "gone"),
        ("SST-VAL845", "profile:analyst", "daily"),
        ("SST-VAL858", "profile:analyst", "nope"),
        ("SST-VAL860", "profile:analyst", "ghost"),
        ("SST-VAL857", "command:bad name", None),
    ]


def test_a_skill_reached_only_through_a_profile_plugin_is_reached() -> None:
    catalog = ProfileCatalog((profile(skills=(), plugins=("kit",)),))
    unreached = unreached_skills(SKILLS, catalog, catalog_channel=False, plugins={"kit": KIT})
    assert [item.subject for item in unreached] == ["skill:common", "skill:semantics"]


def test_unreached_skills_only_when_the_catalog_channel_is_off() -> None:
    catalog = ProfileCatalog((profile(),), SHARED)
    assert unreached_skills(SKILLS, catalog, catalog_channel=True) == ()
    assert [item.subject for item in unreached_skills(SKILLS, catalog, catalog_channel=False)] == ["skill:operations"]
    assert [item.subject for item in unreached_skills(SKILLS, ProfileCatalog(), catalog_channel=False)] == [
        "skill:common",
        "skill:operations",
        "skill:semantics",
    ]


def test_a_hook_script_sits_under_the_hook_name_in_its_tree() -> None:
    assert HOOK.tree_path == "sql-safety/check.sh"


def test_each_list_a_profile_names_is_checked_in_order_and_only_a_loader_error_excuses_a_miss() -> None:
    loader_errors = DiagnosticBag(
        (
            D("SST-VAL852", origin=Origin("hooks/broken"), subject="hook:broken", artifact="broken", detail="x"),
            D("SST-VAL853", origin=Origin("mcp-servers/broken"), subject="mcp:broken", artifact="broken", detail="x"),
            D(
                "SST-VAL859",
                origin=Origin("commands/broken.md"),
                subject="command:broken",
                artifact="broken",
                detail="x",
            ),
            # A warning leaves its subject defined, so a profile naming it is still told it is unknown.
            D("SST-PRS004", origin=Origin("hooks/warned"), subject="hook:warned", artifact="hook:warned", field="x"),
        )
    )
    named = profile(
        skills=("ghost", "semantics", "lost"),
        commands=("broken", "ghost"),
        plugins=("ghost", "kit"),
        hooks=("broken", "warned", "ghost"),
        mcp_servers=("ghost", "broken"),
    )
    catalog = ProfileCatalog((named,), None, (), (), loader_errors)
    found = [
        (item.code, item.subject, item.context.get("name"))
        for item in validate_profile_catalog(catalog, SKILLS, {"kit": KIT})
    ]
    assert found == [
        ("SST-VAL852", "hook:broken", None),
        ("SST-VAL853", "mcp:broken", None),
        ("SST-VAL859", "command:broken", None),
        ("SST-PRS004", "hook:warned", None),
        ("SST-VAL844", "profile:analyst", "ghost"),
        ("SST-VAL844", "profile:analyst", "lost"),
        ("SST-VAL858", "profile:analyst", "ghost"),
        ("SST-VAL860", "profile:analyst", "ghost"),
        ("SST-VAL846", "profile:analyst", "warned"),
        ("SST-VAL846", "profile:analyst", "ghost"),
        ("SST-VAL847", "profile:analyst", "ghost"),
    ]
    reported = validate_profile_catalog(catalog, SKILLS, {"kit": KIT})[4]
    assert (reported.origin, list(reported.context.items())) == (ORIGIN, [("artifact", "analyst"), ("name", "ghost")])


def test_a_server_defined_again_is_reported_against_the_config_that_defined_it_last() -> None:
    first, second, third = (
        McpConfig(name, f"mcp-servers/{name}/mcp.json", {"dbt": {"command": name}}, Origin(name))
        for name in ("first", "second", "third")
    )
    catalog = ProfileCatalog(
        (profile(mcp_servers=("first", "second", "third", "third")),), mcp_configs=(first, second, third)
    )
    found = [
        (item.code, item.context["a"], item.context["b"], item.origin)
        for item in validate_profile_catalog(catalog, SKILLS)
    ]
    # A config named twice clashes with itself.
    assert found == [
        ("SST-VAL849", "first", "second", Origin("second")),
        ("SST-VAL849", "second", "third", Origin("third")),
        ("SST-VAL849", "third", "third", Origin("third")),
    ]


def test_profile_names_follow_one_rule_that_reserves_shared() -> None:
    names = ("data_analyst", "x-y_z", "shared", "a__b", "Upper", "-a", "a-")
    catalog = ProfileCatalog(tuple(profile(name) for name in names))
    found = [
        (item.subject, item.context["artifact"], item.context["detail"])
        for item in validate_profile_catalog(catalog, SKILLS)
    ]
    rule = "must be lowercase letters, digits, '-' or '_', and not 'shared'"
    assert found == [
        (f"profile:{name}", f"profile:{name}", f"profile name '{name}' {rule}")
        for name in ("shared", "a__b", "Upper", "-a", "a-")
    ]


def test_hooks_group_by_event_then_matcher_in_the_order_the_profile_first_names_them() -> None:
    def hook(name: str, event: str, matcher: str | None = None) -> HookDefinition:
        script = SkillFile(f"{name}.sh", name.encode())
        return HookDefinition(name, f"hooks/{name}", event, "bash", script, Origin(name), matcher=matcher)

    hooks = (
        hook("a", "PreToolUse", "sql"),
        hook("b", "PreToolUse"),
        hook("c", "Stop"),
        hook("d", "PreToolUse", "sql"),
        hook("e", "PreToolUse", "other"),
    )
    release = build_profile(
        profile(skills=(), hooks=("c", "b", "a", "e", "d", "b", "ghost")),
        ProfileCatalog(hooks=hooks),
        SKILLS,
        stage="DB.S.PROFILES",
        version_prefix="SST_",
    )
    events = release.row["HOOKS"]
    assert isinstance(events, dict)
    shape = {
        event: [
            (group.get("matcher"), [entry["source"]["snowflake_stage"].rsplit("/", 1)[-1] for entry in group["hooks"]])
            for group in groups
        ]
        for event, groups in events.items()
    }
    assert list(shape) == ["Stop", "PreToolUse"]
    assert shape == {
        "Stop": [(None, ["c.sh"])],
        "PreToolUse": [(None, ["b.sh"]), ("sql", ["a.sh", "d.sh"]), ("other", ["e.sh"])],
    }
    # The group of hooks without a matcher carries no matcher key at all.
    assert list(events["PreToolUse"][0]) == ["hooks"]
    assert [entry for group in events["PreToolUse"] for entry in group["hooks"]][0] == {
        "type": "command",
        "command": "bash",
        "source": {"snowflake_stage": f"@DB.S.PROFILES/{release.trees[0].prefix}b/b.sh"},
    }
    assert release.trees[0].paths == ("a/a.sh", "b/b.sh", "c/c.sh", "d/d.sh", "e/e.sh")
    assert [path for path in release.source_files if path.startswith("hooks/")] == [
        "hooks/c/c.sh",
        "hooks/b/b.sh",
        "hooks/a/a.sh",
        "hooks/e/e.sh",
        "hooks/d/d.sh",
    ]


def test_only_the_registry_desktop_reads_is_accepted() -> None:
    assert desktop_registry_diagnostics(QualifiedName.parse(DESKTOP_REGISTRY.lower())) == ()
    (found,) = desktop_registry_diagnostics(QualifiedName.parse("DB.SCH.OTHER"))
    assert found.code == "SST-VAL854"
