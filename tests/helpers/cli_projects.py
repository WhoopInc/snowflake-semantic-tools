"""What the CLI tests share: `sst` against the fake Snowflake, and small projects of one artifact type."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import DBT_MANIFEST
from tests.helpers.snowflake_fake import FakeSnowflake


def invoke_with_port(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, args: list[str]) -> Result:
    if "--project-dir" in args and args[0] in ("plan", "apply"):
        project = Path(args[args.index("--project-dir") + 1])
        if not (project / "target" / "sst" / "manifest.json").is_file():
            compile_project(project)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: None  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args)


def common(project: Path) -> list[str]:
    return ["--project-dir", str(project), "--manifest", str(DBT_MANIFEST)]


def compile_project(project: Path) -> None:
    result = CliRunner().invoke(cli, ["compile", *common(project)])
    assert result.exit_code == 0, result.output


def invoke_counting_closes(
    monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, args: list[str]
) -> tuple[Result, list[str]]:
    """Run `sst` against `port`, recording every close() so a test can prove the connection was released."""
    closes: list[str] = []
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: closes.append("closed")  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args), closes


PROFILES = """
skills:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: acct
      user: me
      database: DB
      schema: SCH
"""


# Every configuration states whether expressions are compiled against Snowflake (SST-CFG031).
OFFLINE_VALIDATION = "validation:\n  snowflake_syntax_check: false\n"


def skills_only_project(root: Path, config: str = "project:\n  target_profile: skills\n") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "profiles.yml").write_text(PROFILES, encoding="utf-8")
    (root / "sst_config.yml").write_text(OFFLINE_VALIDATION + config, encoding="utf-8")
    return root


SKILLS_CONFIG = """
project:
  target_profile: skills
skills:
  +version_prefix: "SST_"
  catalog:
    +bundle_stage: SKILL_BUNDLES
"""


def skill_project(root: Path) -> Path:
    project = skills_only_project(root, SKILLS_CONFIG)
    skill = project / "skills" / "month-close"
    (skill / "reference").mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\nname: month-close\ndescription: Close the month.\n---\nRead reference/steps.md.\n",
        encoding="utf-8",
    )
    (skill / "reference" / "steps.md").write_text("steps\n", encoding="utf-8")
    return project


PROFILE_CONFIG = """
project:
  target_profile: skills
skills:
  stage:
    +stage: PROFILES
"""


def profile_with_commands_and_plugin(root: Path) -> Path:
    project = skill_project(root)
    (project / "sst_config.yml").write_text(OFFLINE_VALIDATION + PROFILE_CONFIG, encoding="utf-8")
    files = {
        "profiles/analyst/profile.yml": (
            "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [month-close]\n"
            "commands: [sql/check]\nplugins: [kit]\n"
        ),
        "profiles/shared/profile.yml": "commands: [daily]\n",
        "commands/daily.md": "Summarise yesterday.\n",
        "commands/sql/check.md": "---\ndescription: Check SQL.\n---\nCheck it.\n",
        "plugins/kit/plugin.yml": "name: kit\ndescription: Kit.\nowner_team: Data\nskills: [month-close]\n",
    }
    for name, text in files.items():
        (project / name).parent.mkdir(parents=True, exist_ok=True)
        (project / name).write_text(text, encoding="utf-8")
    return project
