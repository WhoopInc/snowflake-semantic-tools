"""A private copy of a project, kept in step with it, where a session reads edits not yet saved.

A session given unsaved edits must read them as it reads files on disk, through every loader,
without writing to the project. So it reads a copy: the project's files, then the edits over
them. The copy is made once, and each `sync` after brings it back in step with the project by
the size, modification time and kind of each file, so only what changed since is copied again.

Left out are what no compile reads: version control, virtual environments and caches, dbt's
`target-path` and `log-path`, and the manifest SST writes. A copied file keeps the time the
original was modified, so a check that compares times reads the copy as it reads the project.
A symbolic link is copied as the link it is.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator, Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.paths import (
    UnsafeWrite,
    make_folders_within,
    mirror_file_within,
    mirror_link_within,
    private_folder,
    remove_private_folder,
    remove_tree_within,
    remove_within,
)

# Folders no compile reads, anywhere in the project.
_SKIPPED_FOLDERS = frozenset((".git", ".venv", "venv", "node_modules", "__pycache__", ".mypy_cache", ".pytest_cache"))
# What a file is, as `sync` compares it: its kind, size and modification time, or a link's target.
_Seen = tuple[str, int, int] | tuple[str, str]


class WorkingCopy:
    """One project's files in a private folder, with a set of unsaved edits written over them.

    Args:
        project_dir: The project to copy; it is only ever read.
        skipped: Paths relative to the project that are not copied, with everything below them,
            such as dbt's `target-path`.
    """

    def __init__(self, project_dir: Path, *, skipped: frozenset[str] = frozenset()) -> None:
        self._source = project_dir.resolve()
        self._skipped = skipped
        self._folder: Path | None = None
        self._seen: dict[str, _Seen] = {}
        self._overlaid: frozenset[str] = frozenset()

    @property
    def path(self) -> Path:
        """Where the copy is: the project directory a session reads it as.

        Raises:
            RuntimeError: `sync` has not run, so there is no copy yet.
        """
        if self._folder is None:
            raise RuntimeError("the working copy is made by its first sync")
        return self._folder / self._source.name

    def sync(self, overlay: Mapping[str, bytes | None]) -> Path:
        """Make the copy the project's files as they are now, with `overlay` written over them.

        Each key is a path relative to the project; its bytes replace the file, or create it,
        and None deletes it. A file an earlier sync overlaid is the project's again unless this
        overlay names it too.

        Returns:
            Where the copy is.

        Raises:
            UnsafeWrite: an overlay path is absolute or leaves the project, or names a folder.
            OSError: a project file cannot be read, or the copy cannot be written.
        """
        if self._folder is None:
            self._folder = private_folder("sst-working-copy-")
        root = self.path
        make_folders_within(self._folder, root)
        current = dict(self._walk())
        for relative in sorted(set(self._seen) - set(current), key=len, reverse=True):
            self._remove(root, relative)
        for relative in sorted(self._overlaid - set(overlay)):
            if relative in current:
                self._copy(root, relative, current[relative])
            else:
                self._remove(root, relative)
        for relative, seen in current.items():
            if self._seen.get(relative) != seen and relative not in overlay:
                self._copy(root, relative, seen)
        self._seen = current
        for relative, data in overlay.items():
            target = _inside(root, relative)
            if data is None:
                remove_within(root, target)
            else:
                # Written now, so a check that digests file times reads the edit as a change.
                mirror_file_within(root, target, data, None)
        self._overlaid = frozenset(overlay)
        return root

    def close(self) -> None:
        """Remove the copy; the next `sync` makes a new one."""
        if self._folder is not None:
            remove_private_folder(self._folder)
        self._folder, self._seen, self._overlaid = None, {}, frozenset()

    def __del__(self) -> None:
        self.close()

    def _walk(self) -> Iterator[tuple[str, _Seen]]:
        """Yield each file, link and folder of the project that is copied, with what it is."""
        for folder, folders, files in os.walk(self._source):
            here = Path(folder).relative_to(self._source)
            kept = []
            for name in sorted(folders):
                relative = (here / name).as_posix()
                if name in _SKIPPED_FOLDERS or relative in self._skipped:
                    continue
                full = Path(folder) / name
                if full.is_symlink():
                    yield relative, ("link", os.readlink(full))
                    continue
                yield relative, ("folder", 0, 0)
                kept.append(name)
            folders[:] = kept
            for name in files:
                relative = (here / name).as_posix()
                if relative in self._skipped:
                    continue
                found = os.stat(Path(folder) / name, follow_symlinks=False)
                if stat.S_ISLNK(found.st_mode):
                    yield relative, ("link", os.readlink(Path(folder) / name))
                elif stat.S_ISREG(found.st_mode):
                    yield relative, ("file", found.st_size, found.st_mtime_ns)

    def _copy(self, root: Path, relative: str, seen: _Seen) -> None:
        """Copy one file, link or folder of the project into the copy, in place of what is there."""
        target = root / relative
        if seen[0] == "folder":
            if target.is_symlink() or (target.exists() and not target.is_dir()):
                remove_within(root, target)
            make_folders_within(root, target)
            return
        if target.is_dir() and not target.is_symlink():
            remove_tree_within(root, target)
        if seen[0] == "link":
            mirror_link_within(root, target, str(seen[1]))
        else:
            source = self._source / relative
            mirror_file_within(root, target, source.read_bytes(), source.stat().st_mtime_ns)

    def _remove(self, root: Path, relative: str) -> None:
        """Remove what is at `relative` in the copy, file, link or folder; nothing when absent."""
        target = root / relative
        if target.is_dir() and not target.is_symlink():
            remove_tree_within(root, target)
        else:
            remove_within(root, target)


def _inside(root: Path, relative: str) -> Path:
    """Return where `relative` is in the copy.

    Raises:
        UnsafeWrite: `relative` is absolute, or a `..` takes it out of the project.
    """
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise UnsafeWrite(root / relative, "an overlay path must be relative to the project and stay inside it")
    return root / path
