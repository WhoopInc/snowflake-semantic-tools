"""Profiles, the shared folder, hooks, and MCP configs load with located diagnostics."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog


def write(root: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


def load(root: Path):
    return load_profile_catalog(root, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers")


def test_profiles_shared_hooks_and_mcp_configs_load(tmp_path: Path) -> None:
    write(
        tmp_path,
        {
            "profiles/analyst/profile.yml": (
                "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [semantics]\n"
                "mcp_servers: [dbt]\nhooks: [guard]\n"
            ),
            "profiles/analyst/AGENTS.md": "Be careful.\n",
            "profiles/shared/AGENTS.md": "Shared.\n",
            "profiles/shared/rules/b.md": "B\n",
            "profiles/shared/rules/a.md": "A\n",
            "profiles/shared/profile.yaml": "skills: [common]\n",
            "hooks/guard/hook.yml": (
                "name: guard\nevent: PreToolUse\ntype: command\ncommand: bash\nmatcher: snowflake_sql_execute\n"
                "timeout: 30\ninteractive: false\ndescription: Blocks drops.\n"
            ),
            "hooks/guard/guard.sh": "#!/bin/sh\n",
            "hooks/multi/hook.yml": "event: SessionStart\ncommand: bash\nscript: b.sh\n",
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
    assert profile.source_files == ("profiles/analyst/profile.yml", "profiles/analyst/AGENTS.md")
    shared = catalog.shared
    assert (
        shared is not None and shared.skills == ("common",) and [name for name, _ in shared.rules] == ["a.md", "b.md"]
    )
    assert shared.source_files[0] == "profiles/shared/profile.yaml"
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
    write(
        tmp_path,
        {
            "profiles/legacy/profile.yaml": (
                "name: other\nactive: true\nallowed_roles: [ANALYST_ROLE]\ncolor: blue\ndescription: 3\nskills: nope\n"
            ),
            "profiles/twice/profile.yml": "name: twice\n",
            "profiles/twice/profile.yaml": "name: twice\n",
            "profiles/none/README.md": "x",
            "profiles/broken/profile.yml": "name: [unclosed\n",
            "profiles/shared/profile.yml": "skills: [common]\nextra: 1\n",
            "profiles/shared/profile.yaml": "skills: []\n",
            "hooks/none/hook.yml": "event: PreToolUse\n",
            "hooks/prompt/hook.yml": "event: Stop\ntype: prompt\ncommand: x\n",
            "hooks/many/hook.yml": "event: Stop\ncommand: bash\n",
            "hooks/many/a.sh": "a",
            "hooks/many/b.sh": "b",
            "hooks/empty/hook.yml": "event: Stop\ncommand: bash\nbogus: 1\n",
            "hooks/wrong/hook.yml": "event: Stop\ncommand: bash\nscript: missing.sh\n",
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
        ("profile:broken", "SST-LOD001", None),
        ("profile:legacy", "SST-VAL851", "active"),
        ("profile:legacy", "SST-VAL851", "allowed_roles"),
        ("profile:legacy", "SST-PRS004", "color"),
        ("profile:legacy", "SST-VAL801", None),
        ("profile:legacy", "SST-PRS003", "description"),
        ("profile:legacy", "SST-PRS003", "skills"),
        ("profile:none", "SST-VAL801", None),
        ("profile:shared", "SST-VAL801", None),
        ("profile:twice", "SST-VAL801", None),
        ("hook:empty", "SST-PRS004", "bogus"),
        ("hook:empty", "SST-VAL852", None),
        ("hook:many", "SST-VAL852", None),
        ("hook:nofile", "SST-VAL852", None),
        ("hook:none", "SST-VAL852", None),
        ("hook:prompt", "SST-VAL852", None),
        ("hook:wrong", "SST-VAL852", None),
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
    write(
        tmp_path,
        {
            "profiles/analyst/profile.yml": "name: analyst\ncommands: [review, sql/check]\nplugins: [kit]\n",
            "profiles/shared/profile.yml": "skills: []\ncommands: [daily]\n",
            "commands/daily.md": "Summarise yesterday.\n",
            "commands/review.md": "---\ndescription: Review a PR.\nallowed-tools: [Read, Grep]\nhidden: false\n---\nGo.\n",
            "commands/sql/check.md": "---\nskill: sql-author\nallowed-tools: Bash\n---\nCheck it.\n",
            "commands/notes.txt": "not a command",
            "commands/.drafts/wip.md": "hidden",
            "commands/bad/unclosed.md": "---\ndescription: x\n",
            "commands/bad/listy.md": "---\n- a\n---\n",
            "commands/bad/types.md": "---\ndescription: 3\nhidden: maybe\nallowed-tools: [1]\nskill: [x]\ncolour: red\n---\n",
            "commands/bad/yaml.md": "---\ndescription: [unclosed\n---\n",
            "commands/bad/empty.md": "---\n---\nBody only.\n",
        },
    )
    (tmp_path / "commands" / "bad" / "binary.md").write_bytes(b"\xff\xfe")
    catalog = load_profile_catalog(
        tmp_path, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
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
