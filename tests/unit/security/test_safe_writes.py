"""The contained write: a file SST writes stays inside its root, is never reached through a link,
and is replaced atomically; a file named after an artifact is one segment inside its folder.
"""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.adapters import paths
from snowflake_semantic_tools.adapters.paths import (
    UnsafeWrite,
    append_within,
    create_within,
    make_folders_within,
    output_root,
    remove_tree_within,
    scratch_folder,
    write_within,
)
from snowflake_semantic_tools.domain.file_names import MAX_FILE_NAME, file_name


def _outside(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    return root, outside


def test_a_file_is_written_and_replaced_atomically_with_its_folders(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "c.txt"
    write_within(tmp_path, target, "first\n")
    write_within(tmp_path, target, b"second\n")
    assert target.read_text(encoding="utf-8") == "second\n"
    assert sorted(path.name for path in target.parent.iterdir()) == ["c.txt"]


def test_a_replaced_file_keeps_its_permissions(tmp_path: Path) -> None:
    target = tmp_path / "kept.yml"
    target.write_text("a: 1\n", encoding="utf-8")
    target.chmod(0o640)
    write_within(tmp_path, target, "a: 2\n")
    assert stat.S_IMODE(target.stat().st_mode) == 0o640


def test_a_missing_root_is_created(tmp_path: Path) -> None:
    root = tmp_path / "chosen" / "emit"
    write_within(root, root / "view.sql", "x")
    assert (root / "view.sql").read_text(encoding="utf-8") == "x"


@pytest.mark.parametrize("relative", ["../escaped.txt", "a/../../escaped.txt", "/etc/escaped.txt"])
def test_a_path_outside_the_root_is_refused(tmp_path: Path, relative: str) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(UnsafeWrite, match="it lies outside"):
        write_within(root, root / relative, "x")
    assert not (tmp_path / "escaped.txt").exists()


def test_a_path_naming_the_root_itself_is_refused(tmp_path: Path) -> None:
    with pytest.raises(UnsafeWrite, match="names the folder SST writes in"):
        write_within(tmp_path, tmp_path, "x")


def test_a_symlinked_file_is_refused_and_its_target_left_as_it_is(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    victim = outside / "victim.txt"
    victim.write_text("ORIGINAL\n", encoding="utf-8")
    os.symlink(victim, root / "link.txt")
    with pytest.raises(UnsafeWrite) as refused:
        write_within(root, root / "link.txt", "pwned")
    assert str(refused.value) == "it is a symbolic link, which SST does not write through"
    assert refused.value.filename == str(root / "link.txt")
    assert victim.read_text(encoding="utf-8") == "ORIGINAL\n"
    assert (root / "link.txt").is_symlink()


def test_a_symlinked_folder_on_the_way_is_refused(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    os.symlink(outside, root / "emitted")
    with pytest.raises(UnsafeWrite, match="emitted is a symbolic link"):
        write_within(root, root / "emitted" / "deep" / "view.sql", "pwned")
    assert list(outside.iterdir()) == []


def test_a_link_pointing_inside_the_root_is_refused_too(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    os.symlink(tmp_path / "real", tmp_path / "alias")
    with pytest.raises(UnsafeWrite, match="alias is a symbolic link"):
        write_within(tmp_path, tmp_path / "alias" / "a.txt", "x")


def test_a_root_reached_through_a_link_is_accepted(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    os.symlink(tmp_path / "real", tmp_path / "linked")
    write_within(tmp_path / "linked", tmp_path / "linked" / "a.txt", "x")
    assert (tmp_path / "real" / "a.txt").read_text(encoding="utf-8") == "x"


def test_a_file_or_a_folder_where_a_folder_or_file_belongs_is_refused(tmp_path: Path) -> None:
    (tmp_path / "blocker").write_text("a file", encoding="utf-8")
    with pytest.raises(UnsafeWrite, match="blocker is not a folder"):
        write_within(tmp_path, tmp_path / "blocker" / "a.txt", "x")
    (tmp_path / "folder").mkdir()
    with pytest.raises(UnsafeWrite, match="it is not a regular file"):
        write_within(tmp_path, tmp_path / "folder", "x")


def test_a_folder_swapped_for_a_link_while_checked_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "sub").mkdir()
    real_open = os.open

    def swapped(name: str, flags: int, *args: object, **kwargs: object) -> int:
        if name == "sub":
            raise OSError(62, "Too many levels of symbolic links")
        return real_open(name, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "open", swapped)
    with pytest.raises(UnsafeWrite, match="sub changed while SST was writing"):
        write_within(tmp_path, tmp_path / "sub" / "a.txt", "x")


def test_a_failed_write_leaves_no_temporary_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def full(descriptor: int, data: bytes) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(paths, "_write_all", full)
    with pytest.raises(OSError, match="No space left"):
        write_within(tmp_path, tmp_path / "a.txt", "x")
    assert list(tmp_path.iterdir()) == []


def test_append_adds_to_what_the_file_holds_and_refuses_a_link(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    log = root / "target" / "run_log.jsonl"
    append_within(root, log, "one\n")
    append_within(root, log, "two\n")
    assert log.read_text(encoding="utf-8") == "one\ntwo\n"
    os.symlink(outside / "victim.log", root / "linked.log")
    with pytest.raises(UnsafeWrite):
        append_within(root, root / "linked.log", "x")
    assert not (outside / "victim.log").exists()


def test_create_fails_on_anything_there_and_never_follows_a_link(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    lock = root / "target" / "state.json.lock"
    create_within(root, lock, b"held")
    assert lock.read_bytes() == b"held" and stat.S_IMODE(lock.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        create_within(root, lock, b"again")
    os.symlink(outside / "victim.lock", root / "dangling.lock")
    with pytest.raises(FileExistsError):
        create_within(root, root / "dangling.lock", b"x")
    assert not (outside / "victim.lock").exists()


def test_folders_are_made_inside_the_root_or_as_the_root(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    make_folders_within(root, root / "a" / "b")
    make_folders_within(root / "fresh", root / "fresh")
    assert (root / "a" / "b").is_dir() and (root / "fresh").is_dir()
    os.symlink(outside, root / "linked")
    with pytest.raises(UnsafeWrite):
        make_folders_within(root, root / "linked" / "c")
    assert list(outside.iterdir()) == []


def test_a_tree_is_removed_unless_a_link_leads_to_it(tmp_path: Path) -> None:
    root, outside = _outside(tmp_path)
    (root / "target" / "sst").mkdir(parents=True)
    (root / "target" / "sst" / "manifest.json").write_text("{}", encoding="utf-8")
    remove_tree_within(root, root / "target" / "sst")
    assert not (root / "target" / "sst").exists()
    remove_tree_within(root, root / "target" / "sst")
    (outside / "sst").mkdir()
    (outside / "sst" / "keep.txt").write_text("keep", encoding="utf-8")
    (root / "target").rmdir()
    os.symlink(outside, root / "target")
    with pytest.raises(UnsafeWrite):
        remove_tree_within(root, root / "target" / "sst")
    assert (outside / "sst" / "keep.txt").exists()


def test_a_scratch_folder_is_removed_after_use() -> None:
    with scratch_folder("sst-test-") as folder:
        write_within(folder, folder / "a.bin", b"x")
        assert (folder / "a.bin").exists()
    assert not folder.exists()


def test_the_output_root_is_the_project_for_a_folder_inside_it(tmp_path: Path) -> None:
    project = tmp_path / "project"
    assert output_root(project, project / "emitted") == project
    assert output_root(project, project / "a" / ".." / "emitted") == project
    assert output_root(project, tmp_path / "elsewhere") == tmp_path / "elsewhere"
    assert output_root(project, project / ".." / "elsewhere") == project / ".." / "elsewhere"


_NAMES = st.text(st.characters(codec="utf-8"), max_size=300) | st.sampled_from(
    ["", ".", "..", "../..", "/etc/passwd", "a/../../b", "x\x00y", ".hidden", "%2F", '"x/../../pwned"']
)
_SAFE = re.compile(r"[A-Za-z0-9._%-]+")


@given(name=_NAMES)
def test_a_file_name_is_always_one_safe_segment(name: str) -> None:
    segment = file_name(name)
    assert _SAFE.fullmatch(segment)
    assert segment not in (".", "..") and not segment.startswith(".")
    assert "/" not in segment and "\\" not in segment
    assert len(segment) <= MAX_FILE_NAME


@given(left=_NAMES, right=_NAMES)
def test_distinct_names_short_enough_get_distinct_file_names(left: str, right: str) -> None:
    if left != right and max(len(file_name(left)), len(file_name(right))) < MAX_FILE_NAME - 17:
        assert file_name(left) != file_name(right)


@settings(max_examples=60, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(name=_NAMES, suffix=st.sampled_from([".sql", ".json", ".yaml"]))
def test_no_artifact_name_writes_outside_the_folder(tmp_path: Path, name: str, suffix: str) -> None:
    folder = tmp_path / "project" / "emitted"
    target = folder / f"{file_name(name.casefold())}{suffix}"
    write_within(tmp_path / "project", target, "x")
    assert target.parent == folder
    assert os.path.abspath(target).startswith(os.path.abspath(folder) + os.sep)
    assert target.is_file() and not target.is_symlink()
    target.unlink()
