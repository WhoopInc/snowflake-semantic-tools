"""`sst agent-kit`: install, upgrade, status and remove against a project on disk, never Snowflake."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.commands import agent_kit as command
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import project_copy

LOCK = Path(".sst") / "agent-kit.json"
KIT_V1 = {"sst-core": {"SKILL.md": b"core v1\n"}, "sst-fix": {"SKILL.md": b"fix v1\n", "reference/codes.md": b"c\n"}}
KIT_V2 = {"sst-core": {"SKILL.md": b"core v2\n"}, "sst-fix": {"SKILL.md": b"fix v1\n", "reference/codes.md": b"c\n"}}


@pytest.fixture
def kit(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, dict[str, bytes]]]:
    """Serve `current["kit"]` as the kit the installed SST ships; a test swaps it to upgrade."""
    current = {"kit": KIT_V1}
    monkeypatch.setattr(command, "read_kit", lambda version: current["kit"])
    return current


def _kit(project: Path, *args: str) -> Result:
    return CliRunner().invoke(cli, ["agent-kit", *args, "--project-dir", str(project)])


def _json(project: Path, *args: str) -> tuple[int, dict[str, object]]:
    result = CliRunner().invoke(cli, ["-o", "json", "agent-kit", *args, "--project-dir", str(project)])
    return result.exit_code, json.loads(result.stdout)


def _codes(envelope: dict[str, object]) -> list[str]:
    return [item["code"] for item in envelope["diagnostics"]]  # type: ignore[attr-defined]


def test_install_writes_each_agent_its_skills_and_one_lock(tmp_path: Path, kit: object) -> None:
    code, envelope = _json(tmp_path, "install", "--agent", "cortex", "--agent", "claude")
    assert code == 0
    for root in (".cortex/skills", ".claude/skills"):
        assert (tmp_path / root / "sst-fix" / "reference" / "codes.md").read_bytes() == b"c\n"
    lock = json.loads((tmp_path / LOCK).read_text(encoding="utf-8"))
    assert sorted(lock["agents"]) == ["claude", "cortex"]
    data = envelope["data"]
    assert [row["agent"] for row in data["agents"]] == ["cortex", "claude"]  # type: ignore[index]
    assert data["refused"] is False and data["dry_run"] is False  # type: ignore[index]


def test_install_picks_the_agent_whose_folder_the_project_has(tmp_path: Path, kit: object) -> None:
    (tmp_path / ".claude").mkdir()
    result = _kit(tmp_path, "install")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".claude/skills/sst-core/SKILL.md").is_file()
    assert not (tmp_path / ".cortex").exists()
    assert "review the change, .sst/agent-kit.json included" in result.output


@pytest.mark.parametrize("folders", [(), (".cortex", ".claude")])
def test_install_needs_an_agent_when_the_project_does_not_say(
    tmp_path: Path, kit: object, folders: tuple[str, ...]
) -> None:
    for folder in folders:
        (tmp_path / folder).mkdir()
    result = _kit(tmp_path, "install")
    assert result.exit_code == 3
    assert "SST-PRT100" in result.output and "--agent cortex or --agent claude" in result.output


def test_a_second_install_changes_nothing(tmp_path: Path, kit: object) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    before = (tmp_path / LOCK).stat().st_mtime_ns
    result = _kit(tmp_path, "install", "--agent", "cortex")
    assert result.exit_code == 0
    assert "install: nothing to change" in result.output and "skip     sst-core" in result.output
    assert (tmp_path / LOCK).stat().st_mtime_ns == before


def test_install_only_adds_the_skills_it_names(tmp_path: Path, kit: object) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex", "--only", "sst-fix").exit_code == 0
    assert not (tmp_path / ".cortex/skills/sst-core").exists()
    assert _kit(tmp_path, "install", "--agent", "cortex", "--only", "sst-core").exit_code == 0
    lock = json.loads((tmp_path / LOCK).read_text(encoding="utf-8"))
    assert lock["agents"]["cortex"]["selection"] == ["sst-core", "sst-fix"]


def test_install_refuses_a_skill_the_kit_does_not_ship(tmp_path: Path, kit: object) -> None:
    result = _kit(tmp_path, "install", "--agent", "cortex", "--only", "sst-nope")
    assert result.exit_code == 3
    assert "--only names no skill of the kit: sst-nope; it ships sst-core, sst-fix" in result.output


def test_a_dry_run_writes_nothing(tmp_path: Path, kit: object) -> None:
    code, envelope = _json(tmp_path, "install", "--agent", "cortex", "--dry-run")
    assert code == 0 and envelope["data"]["dry_run"] is True  # type: ignore[index]
    assert not (tmp_path / ".cortex").exists() and not (tmp_path / LOCK).exists()
    assert "would change 3 file(s)" in _kit(tmp_path, "install", "--agent", "cortex", "--dry-run").output


def test_upgrade_replaces_what_nobody_changed_and_keeps_what_you_did(tmp_path: Path, kit: dict[str, object]) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    kit["kit"] = KIT_V2
    code, envelope = _json(tmp_path, "upgrade")
    assert code == 0 and _codes(envelope) == []
    assert (tmp_path / ".cortex/skills/sst-core/SKILL.md").read_bytes() == b"core v2\n"
    (tmp_path / ".cortex/skills/sst-core/SKILL.md").write_bytes(b"my notes\n")
    kit["kit"] = KIT_V1
    code, envelope = _json(tmp_path, "upgrade")
    assert code == 0 and _codes(envelope) == ["SST-PRT014"]
    assert (tmp_path / ".cortex/skills/sst-core/SKILL.md").read_bytes() == b"my notes\n"
    assert _kit(tmp_path, "upgrade", "--force").exit_code == 0
    assert (tmp_path / ".cortex/skills/sst-core/SKILL.md").read_bytes() == b"core v1\n"


def test_upgrade_with_nothing_installed_says_so(tmp_path: Path, kit: object) -> None:
    result = _kit(tmp_path, "upgrade", "--agent", "claude")
    assert result.exit_code == 0
    assert "claude (nothing installed; run `sst agent-kit install --agent claude`)" in result.output
    assert "nothing installed; run `sst agent-kit install`" in result.output


def test_upgrade_with_nothing_to_change_rewrites_nothing(tmp_path: Path, kit: object) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    result = _kit(tmp_path, "upgrade", "--dry-run")
    assert result.exit_code == 0 and "would change 0 file(s)" in result.output
    assert "upgrade: nothing to change" in _kit(tmp_path, "upgrade").output


def test_status_compares_the_files_with_the_kit(tmp_path: Path, kit: dict[str, object]) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    assert _kit(tmp_path, "status", "--check").exit_code == 0
    kit["kit"] = KIT_V2
    code, envelope = _json(tmp_path, "status", "--check")
    assert code == 2 and envelope["data"]["up_to_date"] is False  # type: ignore[index]
    [agent] = envelope["data"]["agents"]  # type: ignore[index]
    assert {skill["name"]: skill["state"] for skill in agent["skills"]} == {
        "sst-core": "outdated",
        "sst-fix": "current",
    }
    result = _kit(tmp_path, "status")
    assert result.exit_code == 0
    assert "  outdated       sst-core" in result.output
    assert result.output.count("run `sst agent-kit upgrade`") == 1
    assert envelope["next_steps"] == ["run `sst agent-kit upgrade` to bring the kit up to date"]
    checked = _kit(tmp_path, "status", "--check")
    assert checked.exit_code == 2 and checked.output.count("run `sst agent-kit upgrade`") == 1


def test_status_tells_an_agent_which_skills_to_upgrade(tmp_path: Path, kit: dict[str, object]) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    (tmp_path / ".cortex/skills/sst-fix/SKILL.md").write_bytes(b"mine\n")
    kit["kit"] = KIT_V2
    result = CliRunner().invoke(cli, ["-o", "agent", "agent-kit", "status", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    lines = result.stdout.splitlines()
    assert lines[0].startswith("sst agent-kit status: PASS")
    assert "outdated cortex sst-core" in lines and "modified cortex sst-fix" in lines
    assert lines[-1] == "next: run `sst agent-kit upgrade` to bring the kit up to date"
    assert _kit(tmp_path, "upgrade").exit_code == 0
    current = CliRunner().invoke(cli, ["-o", "agent", "agent-kit", "status", "--project-dir", str(tmp_path)])
    assert current.stdout.splitlines()[1:] == ["modified cortex sst-fix", "next: nothing to do"]


def test_status_with_nothing_installed_lists_every_agent(tmp_path: Path, kit: object) -> None:
    code, envelope = _json(tmp_path, "status", "--check")
    assert code == 0
    assert [row["agent"] for row in envelope["data"]["agents"]] == ["cortex", "claude"]  # type: ignore[index]
    assert envelope["data"]["installed_by"] is None  # type: ignore[index]
    assert "nothing installed" in _kit(tmp_path, "status").output


def test_remove_takes_out_the_kit_and_the_lock_and_keeps_your_files(tmp_path: Path, kit: object) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    (tmp_path / ".cortex/skills/sst-fix/SKILL.md").write_bytes(b"mine\n")
    (tmp_path / ".cortex/skills/mine").mkdir()
    code, envelope = _json(tmp_path, "remove")
    assert code == 0 and _codes(envelope) == ["SST-PRT014"]
    assert not (tmp_path / ".cortex/skills/sst-core").exists()
    assert (tmp_path / ".cortex/skills/sst-fix/SKILL.md").read_bytes() == b"mine\n"
    assert (tmp_path / ".cortex/skills/mine").is_dir()
    assert _kit(tmp_path, "remove", "--force").exit_code == 0
    assert not (tmp_path / ".cortex/skills/sst-fix").exists() and not (tmp_path / LOCK).exists()


def test_remove_only_one_skill_keeps_the_rest_installed(tmp_path: Path, kit: object) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    assert _kit(tmp_path, "remove", "--only", "sst-fix").exit_code == 0
    lock = json.loads((tmp_path / LOCK).read_text(encoding="utf-8"))
    assert lock["agents"]["cortex"]["selection"] == ["sst-core"]


@pytest.mark.parametrize("subcommand", ["install", "upgrade"])
def test_a_kit_folder_inside_a_published_directory_is_refused(tmp_path: Path, kit: object, subcommand: str) -> None:
    assert _kit(tmp_path, "install", "--agent", "cortex").exit_code == 0
    (tmp_path / "sst_config.yml").write_text("project:\n  skills_dir: .cortex\n", encoding="utf-8")
    code, envelope = _json(tmp_path, subcommand, "--agent", "cortex")
    assert code == 4 and _codes(envelope) == ["SST-CFG052"]
    assert envelope["data"]["refused"] is True  # type: ignore[index]


def test_an_unreadable_lock_ends_the_run(tmp_path: Path, kit: object) -> None:
    (tmp_path / ".sst").mkdir()
    (tmp_path / LOCK).write_text("{", encoding="utf-8")
    code, envelope = _json(tmp_path, "status")
    assert code == 4 and _codes(envelope) == ["SST-PRT009"]


def test_a_failed_write_exits_1_naming_the_file(tmp_path: Path, kit: object, monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(root: Path, path: Path, data: bytes | str) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr("snowflake_semantic_tools.adapters.agent_kit.write_within", refuse)
    code, envelope = _json(tmp_path, "install", "--agent", "cortex")
    assert code == 1 and _codes(envelope) == ["SST-PRT008"]


def test_a_refusal_reports_that_nothing_was_written(tmp_path: Path, kit: object) -> None:
    (tmp_path / ".cortex/skills/sst-fix").mkdir(parents=True)
    result = _kit(tmp_path, "install", "--agent", "cortex")
    assert result.exit_code == 1
    assert "install refused: nothing was written" in result.output
    assert not (tmp_path / ".cortex/skills/sst-core").exists()


def test_the_real_kit_installs_into_the_reference_project(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = _kit(project, "install", "--agent", "cortex")
    assert result.exit_code == 0, result.output
    assert (project / ".cortex/skills/sst-core/SKILL.md").is_file()
    assert _kit(project, "status", "--check").exit_code == 0


def test_debug_reports_which_sst_installed_the_kit(tmp_path: Path, kit: object) -> None:
    project = project_copy(tmp_path)
    debug = ["-o", "json", "debug", "--no-connect", "--project-dir", str(project)]
    before = json.loads(CliRunner().invoke(cli, debug).stdout)["data"]["agent_kit"]
    assert before == {"installed_by": None, "agents": [], "matches_sst": None}
    assert _kit(project, "install", "--agent", "claude").exit_code == 0
    after = json.loads(CliRunner().invoke(cli, debug).stdout)["data"]["agent_kit"]
    assert after["agents"] == ["claude"] and after["matches_sst"] is True
    (project / LOCK).write_text("[]", encoding="utf-8")
    broken = json.loads(CliRunner().invoke(cli, debug).stdout)["data"]["agent_kit"]
    assert broken["matches_sst"] is None and "not a version 1 agent-kit lock" in broken["error"]
