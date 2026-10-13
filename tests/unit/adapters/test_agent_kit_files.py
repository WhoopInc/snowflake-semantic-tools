"""The agent kit's files: the shipped kit, the lock file, installed folders, and applying a plan."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.adapters import agent_kit
from snowflake_semantic_tools.adapters.agent_kit import (
    LOCK_FILE,
    KitWriteError,
    apply_kit_plans,
    kit_digests,
    read_installed,
    read_kit,
    read_lock,
)
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import UnsafeWrite, remove_empty_folder_within
from snowflake_semantic_tools.domain.agent_kit import (
    AgentInstall,
    AgentTarget,
    KitLock,
    content_digest,
    plan_install,
    plan_remove,
)
from snowflake_semantic_tools.domain.diagnostics.core import docs_url

CORTEX = AgentTarget.CORTEX


def _kit_root(tmp_path: Path) -> Path:
    root = tmp_path / "kit"
    (root / "sst-core" / "reference").mkdir(parents=True)
    (root / "sst-core" / "SKILL.md").write_text("docs @SST_DOCS@ for @SST_VERSION@\n", encoding="utf-8")
    (root / "sst-core" / "reference" / "tiers.md").write_text("tiers\n", encoding="utf-8")
    (root / "sst-core" / "script.sh").write_bytes(b"@SST_VERSION@")
    (root / "sst-core" / ".hidden").write_text("x", encoding="utf-8")
    (root / "sst-core" / "__pycache__").mkdir()
    (root / "sst-core" / "__pycache__" / "x.pyc").write_bytes(b"x")
    (root / "not-a-skill").mkdir()
    (root / "README.txt").write_text("x", encoding="utf-8")
    return root


def test_the_kit_reads_each_skill_folder_and_renders_its_markdown(tmp_path: Path) -> None:
    kit = read_kit("1.2.3", _kit_root(tmp_path))
    assert list(kit) == ["sst-core"]
    assert sorted(kit["sst-core"]) == ["SKILL.md", "reference/tiers.md", "script.sh"]
    assert kit["sst-core"]["SKILL.md"] == f"docs {docs_url('1.2.3')} for 1.2.3\n".encode()
    assert kit["sst-core"]["script.sh"] == b"@SST_VERSION@"
    assert kit_digests(kit)["sst-core"]["reference/tiers.md"] == content_digest(b"tiers\n")


def test_the_package_ships_a_kit() -> None:
    assert "sst-core" in read_kit(__version__)


def test_a_project_without_a_lock_has_an_empty_one(tmp_path: Path) -> None:
    assert read_lock(tmp_path) == KitLock("")


@pytest.mark.parametrize(
    ("text", "reason"),
    [("not json", "Expecting value"), ('{"version": 9}', "not a version 1 agent-kit lock")],
)
def test_an_unreadable_lock_is_refused(tmp_path: Path, text: str, reason: str) -> None:
    (tmp_path / ".sst").mkdir()
    (tmp_path / LOCK_FILE).write_text(text, encoding="utf-8")
    with pytest.raises(ProjectError, match=reason) as raised:
        read_lock(tmp_path)
    assert [diagnostic.code for diagnostic in raised.value.diagnostics] == ["SST-PRT009"]


def test_a_linked_lock_is_refused_not_followed(tmp_path: Path) -> None:
    (tmp_path / ".sst").mkdir()
    elsewhere = tmp_path / "elsewhere.json"
    elsewhere.write_text('{"version": 1, "agents": {}}', encoding="utf-8")
    os.symlink(elsewhere, tmp_path / LOCK_FILE)
    with pytest.raises(ProjectError, match="symbolic link"):
        read_lock(tmp_path)


def test_installed_folders_are_read_as_digests(tmp_path: Path) -> None:
    folder = tmp_path / ".cortex" / "skills" / "sst-core"
    (folder / "reference").mkdir(parents=True)
    (folder / "SKILL.md").write_bytes(b"core")
    (folder / "reference" / "a.md").write_bytes(b"a")
    (folder / ".DS_Store").write_bytes(b"noise")
    found = read_installed(tmp_path, CORTEX, ["sst-core", "sst-absent"])
    assert found == {"sst-core": {"SKILL.md": content_digest(b"core"), "reference/a.md": content_digest(b"a")}}


def test_nothing_installed_reads_as_nothing(tmp_path: Path) -> None:
    assert read_installed(tmp_path, CORTEX, ["sst-core"]) == {}


@pytest.mark.parametrize("linked", [".cortex", ".cortex/skills/sst-core", ".cortex/skills/sst-core/SKILL.md"])
def test_a_link_anywhere_on_the_way_is_refused(tmp_path: Path, linked: str) -> None:
    real = tmp_path / "real"
    (real / "skills" / "sst-core").mkdir(parents=True)
    (real / "skills" / "sst-core" / "SKILL.md").write_bytes(b"x")
    target = {
        ".cortex": real,
        ".cortex/skills/sst-core": real / "skills" / "sst-core",
        ".cortex/skills/sst-core/SKILL.md": real / "skills" / "sst-core" / "SKILL.md",
    }[linked]
    link = tmp_path / linked
    link.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(target, link)
    with pytest.raises(ProjectError, match="symbolic link") as raised:
        read_installed(tmp_path, CORTEX, ["sst-core"])
    assert raised.value.diagnostics[0].code == "SST-PRT009"


def _install(tmp_path: Path) -> tuple[dict[str, dict[str, bytes]], KitLock]:
    kit = {"sst-core": {"SKILL.md": b"core", "reference/a.md": b"a"}}
    plan = plan_install(kit_digests(kit), KitLock(""), {}, CORTEX, version="1.0.0")
    apply_kit_plans(tmp_path, [plan], kit, plan.lock)
    return kit, plan.lock


def test_applying_an_install_writes_the_files_and_the_lock(tmp_path: Path) -> None:
    _, lock = _install(tmp_path)
    assert (tmp_path / ".cortex/skills/sst-core/reference/a.md").read_bytes() == b"a"
    document = json.loads((tmp_path / LOCK_FILE).read_text(encoding="utf-8"))
    assert document["agents"]["cortex"]["skills"]["sst-core"]["SKILL.md"] == content_digest(b"core")
    assert read_lock(tmp_path) == lock


def test_applying_a_removal_prunes_emptied_folders_and_the_lock(tmp_path: Path) -> None:
    kit, lock = _install(tmp_path)
    (tmp_path / ".cortex/skills/mine").mkdir()
    disk = read_installed(tmp_path, CORTEX, ["sst-core"])
    plan = plan_remove(lock, disk, CORTEX, force=False, version="1.0.0")
    apply_kit_plans(tmp_path, [plan], kit, plan.lock)
    assert not (tmp_path / ".cortex/skills/sst-core").exists()
    assert (tmp_path / ".cortex/skills/mine").is_dir()
    assert not (tmp_path / LOCK_FILE).exists()


def test_a_removal_keeps_a_folder_that_still_holds_a_file(tmp_path: Path) -> None:
    kit, lock = _install(tmp_path)
    (tmp_path / ".cortex/skills/sst-core/notes.md").write_bytes(b"mine")
    plan = plan_remove(lock, read_installed(tmp_path, CORTEX, ["sst-core"]), CORTEX, force=False, version="1")
    apply_kit_plans(tmp_path, [plan], kit, plan.lock)
    assert sorted(path.name for path in (tmp_path / ".cortex/skills/sst-core").iterdir()) == ["notes.md"]


def test_a_failed_write_names_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kit = {"sst-core": {"SKILL.md": b"core"}}
    plan = plan_install(kit_digests(kit), KitLock(""), {}, CORTEX, version="1")

    def refuse(root: Path, path: Path, data: bytes | str) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr(agent_kit, "write_within", refuse)
    with pytest.raises(KitWriteError) as raised:
        apply_kit_plans(tmp_path, [plan], kit, plan.lock)
    assert raised.value.path == tmp_path / ".cortex/skills/sst-core/SKILL.md"
    assert str(raised.value.cause) == "read-only"


def test_a_failed_lock_write_names_the_lock(tmp_path: Path) -> None:
    (tmp_path / ".sst").write_text("a file where the folder goes", encoding="utf-8")
    lock = KitLock("1", {CORTEX: AgentInstall((), {"sst-core": {"SKILL.md": "d"}})})
    with pytest.raises(KitWriteError) as raised:
        apply_kit_plans(tmp_path, [], {}, lock)
    assert raised.value.path == tmp_path / LOCK_FILE


def test_an_empty_folder_is_removed_and_nothing_else_is(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    (tmp_path / "full").mkdir()
    (tmp_path / "full" / "x").write_text("x", encoding="utf-8")
    (tmp_path / "file").write_text("x", encoding="utf-8")
    os.symlink(tmp_path / "empty", tmp_path / "link")
    assert remove_empty_folder_within(tmp_path, tmp_path / "empty") is True
    assert remove_empty_folder_within(tmp_path, tmp_path / "full") is False
    assert remove_empty_folder_within(tmp_path, tmp_path / "absent") is False
    with pytest.raises(UnsafeWrite, match="symbolic link"):
        remove_empty_folder_within(tmp_path, tmp_path / "link")
    with pytest.raises(UnsafeWrite, match="not a folder"):
        remove_empty_folder_within(tmp_path, tmp_path / "file")


def test_a_folder_that_cannot_be_removed_for_another_reason_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "empty").mkdir()

    def busy(name: str, *, dir_fd: int) -> None:
        raise PermissionError("busy")

    monkeypatch.setattr("snowflake_semantic_tools.adapters.paths.os.rmdir", busy)
    with pytest.raises(PermissionError, match="busy"):
        remove_empty_folder_within(tmp_path, tmp_path / "empty")
