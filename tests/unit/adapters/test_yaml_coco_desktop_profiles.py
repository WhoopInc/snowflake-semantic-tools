"""Profiles, the shared folder, hooks, and MCP configs load with located diagnostics."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.coco_desktop_profiles import load_coco_desktop_profile_catalog
from snowflake_semantic_tools.domain.model.coco_desktop_profile import CocoDesktopProfileCatalog
from tests.helpers.file_trees import write_tree


def load(root: Path) -> CocoDesktopProfileCatalog:
    return load_coco_desktop_profile_catalog(
        root, coco_desktop_profiles_dir="coco_desktop_profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers"
    )


def test_profiles_shared_hooks_and_mcp_configs_load(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "coco_desktop_profiles/analyst/coco_desktop_profile.yml": (
                "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [\"{{ skill('semantics') }}\"]\n"
                "mcp_servers: [\"{{ mcp_server('dbt') }}\"]\nhooks: [\"{{ hook('guard') }}\"]\n"
            ),
            "coco_desktop_profiles/analyst/AGENTS.md": "Be careful.\n",
            "coco_desktop_profiles/shared/AGENTS.md": "Shared.\n",
            "coco_desktop_profiles/shared/rules/b.md": "B\n",
            "coco_desktop_profiles/shared/rules/a.md": "A\n",
            "coco_desktop_profiles/shared/coco_desktop_profile.yaml": "skills: [\"{{ skill('common') }}\"]\n",
            "hooks/guard/hook.yml": (
                "name: guard\nevent: PreToolUse\ntype: command\ncommand: bash\nmatcher: snowflake_sql_execute\n"
                "timeout: 30\ninteractive: false\ndescription: Blocks drops.\n"
            ),
            "hooks/guard/guard.sh": "#!/bin/sh\n",
            "hooks/multi/hook.yml": "event: SessionStart\ncommand: bash\nscript: \"{{ file('b.sh') }}\"\n",
            "hooks/multi/a.sh": "a\n",
            "hooks/multi/b.sh": "b\n",
            "mcp-servers/dbt/mcp.json": '{"mcpServers": {"dbt": {"command": "dbt-mcp"}}}',
        },
    )
    catalog = load(tmp_path)
    assert catalog.diagnostics == ()
    profile = catalog.profiles[0]
    assert (profile.name, profile.skills, profile.mcp_servers, profile.hooks, profile.prompt) == (
        "analyst",
        ("semantics",),
        ("dbt",),
        ("guard",),
        "Be careful.\n",
    )
    assert profile.source_files == (
        "coco_desktop_profiles/analyst/coco_desktop_profile.yml",
        "coco_desktop_profiles/analyst/AGENTS.md",
    )
    shared = catalog.shared
    assert (
        shared is not None and shared.skills == ("common",) and [name for name, _ in shared.rules] == ["a.md", "b.md"]
    )
    assert shared.source_files[0] == "coco_desktop_profiles/shared/coco_desktop_profile.yaml"
    guard, multi = catalog.hooks
    assert (guard.event, guard.matcher, guard.timeout, guard.interactive, guard.script.path) == (
        "PreToolUse",
        "snowflake_sql_execute",
        30,
        False,
        "guard.sh",
    )
    assert multi.script.path == "b.sh" and multi.matcher is None
    assert dict(catalog.mcp_configs[0].servers) == {"dbt": {"command": "dbt-mcp"}}


def test_pipeline_keys_and_malformed_inputs_are_diagnosed(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "coco_desktop_profiles/legacy/coco_desktop_profile.yaml": (
                "name: other\nactive: true\nallowed_roles: [ANALYST_ROLE]\ncolor: blue\ndescription: 3\nskills: nope\n"
            ),
            "coco_desktop_profiles/twice/coco_desktop_profile.yml": "name: twice\n",
            "coco_desktop_profiles/twice/coco_desktop_profile.yaml": "name: twice\n",
            "coco_desktop_profiles/none/README.md": "x",
            "coco_desktop_profiles/broken/coco_desktop_profile.yml": "name: [unclosed\n",
            "coco_desktop_profiles/shared/coco_desktop_profile.yml": "skills: [\"{{ skill('common') }}\"]\nextra: 1\n",
            "coco_desktop_profiles/shared/coco_desktop_profile.yaml": "skills: []\n",
            "hooks/none/hook.yml": "event: PreToolUse\n",
            "hooks/prompt/hook.yml": "event: Stop\ntype: prompt\ncommand: x\n",
            "hooks/many/hook.yml": "event: Stop\ncommand: bash\n",
            "hooks/many/a.sh": "a",
            "hooks/many/b.sh": "b",
            "hooks/empty/hook.yml": "event: Stop\ncommand: bash\nbogus: 1\n",
            "hooks/wrong/hook.yml": "event: Stop\ncommand: bash\nscript: \"{{ file('missing.sh') }}\"\n",
            "hooks/wrong/a.sh": "a",
            "hooks/nofile/readme.txt": "x",
            "mcp-servers/bad/mcp.json": "{not json",
            "mcp-servers/shape/mcp.json": '{"servers": {}}',
            "mcp-servers/nofile/README.md": "x",
        },
    )
    catalog = load(tmp_path)
    found = [
        (item.subject, item.code, item.context.get("key") or item.context.get("field")) for item in catalog.diagnostics
    ]
    assert found == [
        ("coco_desktop_profile:broken", "SST-LOD001", None),
        ("coco_desktop_profile:legacy", "SST-VAL851", "active"),
        ("coco_desktop_profile:legacy", "SST-VAL851", "allowed_roles"),
        ("coco_desktop_profile:legacy", "SST-PRS004", "color"),
        ("coco_desktop_profile:legacy", "SST-VAL801", None),
        ("coco_desktop_profile:legacy", "SST-PRS003", "description"),
        ("coco_desktop_profile:legacy", "SST-PRS003", "skills"),
        ("coco_desktop_profile:none", "SST-VAL801", None),
        ("coco_desktop_profile:shared", "SST-VAL801", None),
        ("coco_desktop_profile:twice", "SST-VAL801", None),
        ("hook:empty", "SST-PRS004", "bogus"),
        ("hook:empty", "SST-VAL852", None),
        ("hook:many", "SST-VAL852", None),
        ("hook:nofile", "SST-VAL852", None),
        ("hook:none", "SST-VAL852", None),
        ("hook:prompt", "SST-VAL852", None),
        ("hook:wrong", "SST-REF014", None),
        ("mcp:bad", "SST-VAL853", None),
        ("mcp:nofile", "SST-VAL853", None),
        ("mcp:shape", "SST-VAL853", None),
    ]
    assert [profile.name for profile in catalog.profiles] == ["legacy"]
    assert catalog.shared is not None and catalog.shared.skills == ()


def test_missing_directories_are_empty(tmp_path: Path) -> None:
    catalog = load(tmp_path)
    assert (catalog.profiles, catalog.shared, catalog.hooks, catalog.mcp_configs, catalog.diagnostics) == (
        (),
        None,
        (),
        (),
        (),
    )
    assert catalog.commands == ()


def test_commands_load_like_a_desktop_command_repository(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "coco_desktop_profiles/analyst/coco_desktop_profile.yml": (
                "name: analyst\n"
                "commands: [\"{{ command('review') }}\", \"{{ command('sql/check') }}\"]\n"
                "plugins: [\"{{ plugin('kit') }}\"]\n"
            ),
            "coco_desktop_profiles/shared/coco_desktop_profile.yml": (
                "skills: []\ncommands: [\"{{ command('daily') }}\"]\n"
            ),
            "commands/daily.md": "Summarise yesterday.\n",
            "commands/review.md": (
                "---\ndescription: Review a PR.\nallowed-tools: [Read, Grep]\nhidden: false\n---\nGo.\n"
            ),
            "commands/sql/check.md": "---\nskill: \"{{ skill('sql-author') }}\"\nallowed-tools: Bash\n---\nCheck it.\n",
            "commands/notes.txt": "not a command",
            "commands/.drafts/wip.md": "hidden",
            "commands/bad/unclosed.md": "---\ndescription: x\n",
            "commands/bad/listy.md": "---\n- a\n---\n",
            "commands/bad/types.md": (
                "---\ndescription: 3\nhidden: maybe\nallowed-tools: [1]\nskill: [x]\ncolour: red\n---\n"
            ),
            "commands/bad/yaml.md": "---\ndescription: [unclosed\n---\n",
            "commands/bad/empty.md": "---\n---\nBody only.\n",
        },
    )
    (tmp_path / "commands" / "bad" / "binary.md").write_bytes(b"\xff\xfe")
    catalog = load_coco_desktop_profile_catalog(
        tmp_path,
        coco_desktop_profiles_dir="coco_desktop_profiles",
        hooks_dir="hooks",
        mcp_servers_dir="mcp-servers",
        commands_dir="commands",
    )
    assert [command.name for command in catalog.commands] == [
        "bad/binary",
        "bad/empty",
        "bad/listy",
        "bad/types",
        "bad/unclosed",
        "bad/yaml",
        "daily",
        "review",
        "sql/check",
    ]
    found = [
        (item.subject, item.code, item.context.get("field") or item.context.get("detail"))
        for item in catalog.diagnostics
    ]
    assert found == [
        ("command:bad/binary", "SST-VAL859", "is not UTF-8"),
        ("command:bad/listy", "SST-VAL859", "frontmatter is a list, not a mapping"),
        ("command:bad/types", "SST-PRS004", "colour"),
        ("command:bad/types", "SST-VAL859", "'description' must be a string"),
        ("command:bad/types", "SST-VAL859", "'skill' must be a string"),
        ("command:bad/types", "SST-VAL859", "'hidden' must be true or false"),
        ("command:bad/types", "SST-VAL859", "'allowed-tools' must be a string or a list of strings"),
        ("command:bad/unclosed", "SST-VAL859", "frontmatter opens with --- but never closes"),
        (
            "command:bad/yaml",
            "SST-VAL859",
            "frontmatter is not valid YAML: expected ',' or ']', but got '<stream end>'",
        ),
    ]
    profile = catalog.profiles[0]
    assert (profile.commands, profile.plugins) == (("review", "sql/check"), ("kit",))
    assert catalog.shared is not None and catalog.shared.commands == ("daily",)
    review = next(command for command in catalog.commands if command.name == "review")
    assert review.file == "commands/review.md" and review.content.startswith(b"---\ndescription")


def test_a_prompt_or_a_rule_that_is_not_utf8_is_reported_and_left_out(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "coco_desktop_profiles/analyst/coco_desktop_profile.yml": "name: analyst\n",
            "coco_desktop_profiles/shared/rules/a.md": "A\n",
        },
    )
    (tmp_path / "coco_desktop_profiles/analyst/AGENTS.md").write_bytes(b"Be \xff careful.\n")
    (tmp_path / "coco_desktop_profiles/shared/rules/b.md").write_bytes(b"\xfeB\n")

    catalog = load(tmp_path)

    assert [(item.code, item.subject, dict(item.context)) for item in catalog.diagnostics] == [
        (
            "SST-LOD006",
            "coco_desktop_profile:analyst",
            {"file": "coco_desktop_profiles/analyst/AGENTS.md", "offset": 3},
        ),
        ("SST-LOD006", "coco_desktop_profile:shared", {"file": "coco_desktop_profiles/shared/rules/b.md", "offset": 0}),
    ]
    assert catalog.profiles[0].prompt is None
    assert catalog.shared is not None and [name for name, _ in catalog.shared.rules] == ["a.md"]


def test_nothing_reached_through_a_symbolic_link_is_read(tmp_path: Path) -> None:
    project = tmp_path / "project"
    outside = write_tree(
        tmp_path / "outside",
        {
            "profile/coco_desktop_profile.yml": "name: linked\n",
            "rule.md": "Outside.\n",
            "command.md": "Outside.\n",
            "run.sh": "#!/bin/sh\n",
            "mcp.json": '{"mcpServers": {}}',
            "hook.yml": "name: h\nevent: PreToolUse\ncommand: bash\n",
        },
    )
    write_tree(
        project,
        {
            "coco_desktop_profiles/shared/AGENTS.md": "Shared.\n",
            "coco_desktop_profiles/shared/rules/kept.md": "Kept.\n",
            "commands/kept.md": "Kept.\n",
            "hooks/guard/hook.yml": "name: guard\nevent: PreToolUse\ncommand: bash\nscript: \"{{ file('run.sh') }}\"\n",
            "hooks/linked/run.sh": "#!/bin/sh\n",
            "mcp-servers/dbt/README.md": "x",
        },
    )
    (project / "coco_desktop_profiles/linked").symlink_to(outside / "profile")
    (project / "coco_desktop_profiles/shared/rules/outside.md").symlink_to(outside / "rule.md")
    (project / "commands/outside.md").symlink_to(outside / "command.md")
    (project / "hooks/guard/run.sh").symlink_to(outside / "run.sh")
    (project / "hooks/linked/hook.yml").symlink_to(outside / "hook.yml")
    (project / "mcp-servers/dbt/mcp.json").symlink_to(outside / "mcp.json")
    catalog = load_coco_desktop_profile_catalog(
        project, coco_desktop_profiles_dir="coco_desktop_profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers"
    )
    refused = sorted(item.context["path"] for item in catalog.diagnostics if item.code == "SST-PRT009")
    assert refused == [
        "coco_desktop_profiles/linked",
        "coco_desktop_profiles/shared/rules/outside.md",
        "commands/outside.md",
        "hooks/guard/run.sh",
        "hooks/linked/hook.yml",
        "mcp-servers/dbt/mcp.json",
    ]
    assert catalog.profiles == ()
    assert catalog.shared is not None and [name for name, _ in catalog.shared.rules] == ["kept.md"]
    assert [command.path for command in catalog.commands] == ["kept.md"]
    assert catalog.hooks == () and catalog.mcp_configs == ()


def test_a_profile_records_its_own_settings_and_its_route_is_checked(tmp_path: Path) -> None:
    write_tree(
        tmp_path,
        {
            "coco_desktop_profiles/analyst/coco_desktop_profile.yml": (
                "name: analyst\ndescription: Analyst.\nconfig:\n  stage: TREES\n  registry_table: R\n"
            ),
            "coco_desktop_profiles/operator/coco_desktop_profile.yml": (
                "name: operator\ndescription: Operator.\nconfig:\n  bundle_stage: B\n"
            ),
            "coco_desktop_profiles/plain/coco_desktop_profile.yml": (
                "name: plain\ndescription: Plain.\nconfig: [stage]\n"
            ),
        },
    )
    config = {"coco_desktop_profiles": {"analyst": {"+schema": "A"}, "gone": {}}}
    catalog = load_coco_desktop_profile_catalog(
        tmp_path,
        coco_desktop_profiles_dir="coco_desktop_profiles",
        hooks_dir="hooks",
        mcp_servers_dir="mcp-servers",
        config=config,
    )
    assert [(item.name, dict(item.config)) for item in catalog.profiles] == [
        ("analyst", {"stage": "TREES", "registry_table": "R"}),
        ("operator", {"bundle_stage": "B"}),
        ("plain", {}),
    ]
    assert [(item.code, item.subject) for item in catalog.diagnostics] == [
        ("SST-CFG041", "config_route:gone"),
        ("SST-CFG053", "coco_desktop_profile:operator"),
        ("SST-CFG004", "coco_desktop_profile:plain"),
    ]
