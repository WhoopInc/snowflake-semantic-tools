"""What the CLI tests share: `sst` against the fake Snowflake, and small projects of one artifact type."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_json import invoke_json
from tests.helpers.file_trees import write_tree
from tests.helpers.reference_project import DBT_MANIFEST
from tests.helpers.snowflake_fake import FakeSnowflake


def invoke_with_port(
    monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, args: list[str], *, input: str | None = None
) -> Result:
    if "--project-dir" in args and args[0] in ("plan", "apply"):
        project = Path(args[args.index("--project-dir") + 1])
        if not (project / "sst_target" / "manifest.json").is_file():
            compile_project(project)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: None  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args, input=input)


def common(project: Path) -> list[str]:
    return ["--project-dir", str(project), "--manifest", str(DBT_MANIFEST)]


def applied_port(project: Path) -> FakeSnowflake:
    """A fake Snowflake on which every artifact of `project`'s compiled manifest is applied.

    The state table records each one and its live marker matches, so `sst test` runs its
    probes, each of which answers.
    """
    manifest = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    artifacts = manifest["artifacts"]
    state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in artifacts.items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(manifest_id, value["fingerprint"])
        for value in artifacts.values()
    }
    return FakeSnowflake(state=state, markers=markers)


def on_a_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let `sst` ask its questions, as on a terminal; CliRunner's streams are never terminals."""
    monkeypatch.setattr("snowflake_semantic_tools.cli.confirm.can_ask", lambda: True)


def compile_project(project: Path) -> None:
    result = CliRunner().invoke(cli, ["compile", *common(project)])
    assert result.exit_code == 0, result.output


def compile_broken(project: Path) -> None:
    """Compile a project that fails to, with `--partial`, so its manifest records the broken files.

    A plan, apply or test then gets past the early stale check and reports the errors itself.
    """
    result = CliRunner().invoke(cli, ["compile", *common(project), "--partial"])
    assert result.exit_code in (0, 1), result.output
    assert (project / "sst_target" / "manifest.json").is_file(), result.output


def validate_json(project: Path, *flags: str) -> tuple[int, list[dict[str, Any]]]:
    """Run `sst validate --output json` on `project` with `flags`; return its exit code and diagnostics."""
    return invoke_json(["validate", *common(project), *flags])


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
coco_desktop_profiles:
  +stage: PROFILES
"""


def profile_with_commands_and_plugin(root: Path) -> Path:
    project = skill_project(root)
    (project / "sst_config.yml").write_text(OFFLINE_VALIDATION + PROFILE_CONFIG, encoding="utf-8")
    files = {
        "coco_desktop_profiles/analyst/coco_desktop_profile.yml": (
            "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [\"{{ skill('month-close') }}\"]\n"
            "commands: [\"{{ command('sql/check') }}\"]\nplugins: [\"{{ plugin('kit') }}\"]\n"
        ),
        "coco_desktop_profiles/shared/coco_desktop_profile.yml": "commands: [\"{{ command('daily') }}\"]\n",
        "commands/daily.md": "Summarize yesterday.\n",
        "commands/sql/check.md": "---\ndescription: Check SQL.\n---\nCheck it.\n",
        "plugins/kit/plugin.yml": (
            "name: kit\ndescription: Kit.\nowner_team: Data\nskills: [\"{{ skill('month-close') }}\"]\n"
        ),
    }
    return write_tree(project, files)
