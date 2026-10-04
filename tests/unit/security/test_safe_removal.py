"""A removal stays inside its root: it goes through descriptors, so a link swapped in is refused."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.paths import UnsafeWrite, remove_tree_within, remove_within


def _layout(tmp_path: Path) -> tuple[Path, Path]:
    """A project with `target/sst/manifest.json`, and a folder outside it holding a file to keep."""
    root = tmp_path / "project"
    (root / "target" / "sst").mkdir(parents=True)
    (root / "target" / "sst" / "manifest.json").write_text("{}", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep", encoding="utf-8")
    return root, outside


def test_a_folder_swapped_for_a_link_after_its_check_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, outside = _layout(tmp_path)
    folder = root / "target" / "sst"
    real_rmtree = shutil.rmtree

    def swap_then_remove(name: str, *, dir_fd: int) -> None:
        folder.rename(root / "target" / "moved")
        os.symlink(outside, folder)
        real_rmtree(name, dir_fd=dir_fd)

    monkeypatch.setattr(shutil, "rmtree", swap_then_remove)
    with pytest.raises(UnsafeWrite, match="changed into a symbolic link"):
        remove_tree_within(root, folder)
    assert (outside / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_a_link_or_a_file_where_the_folder_should_be_is_refused(tmp_path: Path) -> None:
    root, outside = _layout(tmp_path)
    shutil.rmtree(root / "target" / "sst")
    os.symlink(outside, root / "target" / "sst")
    with pytest.raises(UnsafeWrite, match="symbolic link"):
        remove_tree_within(root, root / "target" / "sst")
    (root / "target" / "sst").unlink()
    (root / "target" / "sst").write_text("x", encoding="utf-8")
    with pytest.raises(UnsafeWrite, match="not a folder"):
        remove_tree_within(root, root / "target" / "sst")
    assert (outside / "keep.txt").exists()


def test_a_removal_that_fails_for_another_reason_reports_that_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = _layout(tmp_path)

    def refuse(name: str, *, dir_fd: int) -> None:
        raise PermissionError(13, "Permission denied", name)

    monkeypatch.setattr(shutil, "rmtree", refuse)
    with pytest.raises(PermissionError):
        remove_tree_within(root, root / "target" / "sst")


def test_an_absent_tree_or_folder_on_the_way_removes_nothing(tmp_path: Path) -> None:
    root, _ = _layout(tmp_path)
    remove_tree_within(root, root / "nope" / "sst")
    remove_tree_within(root, root / "target" / "nope")
    remove_tree_within(root, root / "target" / "sst")
    assert not (root / "target" / "sst").exists() and (root / "target").is_dir()


def test_a_file_is_removed_by_name_in_its_folder(tmp_path: Path) -> None:
    root, outside = _layout(tmp_path)
    lock = root / "target" / "sst" / "manifest.json"
    remove_within(root, lock)
    assert not lock.exists()
    remove_within(root, lock)
    remove_within(root, root / "missing" / "lock")
    # A link at the name is removed itself; what it points to is left.
    os.symlink(outside / "keep.txt", lock)
    remove_within(root, lock)
    assert not lock.is_symlink() and (outside / "keep.txt").exists()


def test_a_file_removal_never_goes_through_a_link_or_takes_a_folder(tmp_path: Path) -> None:
    root, outside = _layout(tmp_path)
    with pytest.raises(UnsafeWrite, match="a folder, not a file"):
        remove_within(root, root / "target" / "sst")
    os.symlink(outside, root / "linked")
    with pytest.raises(UnsafeWrite, match="symbolic link"):
        remove_within(root, root / "linked" / "keep.txt")
    with pytest.raises(UnsafeWrite, match="outside"):
        remove_within(root, outside / "keep.txt")
    assert (outside / "keep.txt").exists()
