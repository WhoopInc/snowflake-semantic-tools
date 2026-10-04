"""Path containment: what SST reads stays inside the project, and what it writes inside a root.

Every loader that reads a file a project names, or one it finds by walking a project folder,
asks here first, so neither a symbolic link nor a `..` can make SST read, or publish, a file
from outside the project.

Every file SST writes -- a manifest, rendered DDL, formatted or enriched YAML, a baseline, a
lock -- is written here, and nowhere else (a test fails on a write anywhere else in the
package). A write names the root it must stay inside: the project, or the output directory the
user chose. The path is walked from that root one folder at a time, each opened without
following a link, so a `..` or a symbolic link -- at the file or at any folder between the root
and it -- refuses the write instead of reaching outside. A file is replaced in one rename, from
a temporary file beside it, so a reader sees the old bytes or the new ones.
"""

from __future__ import annotations

import contextlib
import errno
import os
import secrets
import shutil
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

# A folder on the way to a written file is opened, never followed, and must be a folder.
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW


def resolve_within(root: Path, path: Path) -> Path | None:
    """Return `path` with every link and `..` resolved; None when that lies outside `root`."""
    resolved = path.resolve()
    return resolved if resolved.is_relative_to(root.resolve()) else None


def walk_refusal(project_dir: Path, path: Path) -> str | None:
    """Say why SST refuses to read a file or folder found by walking the project; None when it may.

    A walk never follows a symbolic link: `path` is refused when it, or any folder between
    `project_dir` and it, is a link, even one that points inside the project, or when it
    resolves outside the project. `project_dir` itself may be reached through a link.

    Returns:
        The reason, worded to follow "could not read <path>: " in SST-PRT009.
    """
    current = project_dir
    for part in path.relative_to(project_dir).parts:
        current = current / part
        if current.is_symlink():
            linked = "it is" if current == path else f"{current.relative_to(project_dir).as_posix()} is"
            return f"{linked} a symbolic link, which SST does not follow"
    if resolve_within(project_dir, path) is None:
        return "it resolves outside the project"
    return None


class UnsafeWrite(OSError):
    """A write SST refuses: its target lies outside the root, or a link or a non-file is in the way.

    An `OSError`, so every caller that reports a failed write (SST-PRT008, SST-MAN007) reports
    this one too; its text is the reason alone, worded to follow "could not write <path>: ".
    """

    def __init__(self, path: Path, reason: str) -> None:
        super().__init__(errno.EPERM, reason, str(path))

    def __str__(self) -> str:
        return str(self.strerror)


def output_root(project_dir: Path, chosen: Path) -> Path:
    """Return the root a write under a directory the user chose must stay inside.

    That is the project when `chosen` lies in it, so a link inside the project is never written
    through; else `chosen` itself, which the user named.
    """
    target = Path(os.path.abspath(chosen))
    for root in (Path(os.path.abspath(project_dir)), project_dir.resolve()):
        if target.is_relative_to(root):
            return project_dir
    return chosen


def write_within(root: Path, path: Path, data: bytes | str) -> None:
    """Replace the file at `path`, inside `root`, with `data` (text as UTF-8), in one rename.

    Missing folders between `root` and the file are created, and `root` itself when it is
    missing. A file that exists keeps its permissions; a new one gets the umask's.

    Raises:
        UnsafeWrite: `path` lies outside `root`, names `root` itself, or has a symbolic link, or
            something that is not a folder or file, at it or on the way to it.
        OSError: The folder or the file cannot be written; the temporary file is removed.
    """
    payload = data.encode("utf-8") if isinstance(data, str) else data
    folders, name = _split(root, path)
    with _folder(root, folders, path, create=True) as folder:
        existing = _existing(folder, name, path)
        _replace(folder, name, payload, None if existing is None else stat.S_IMODE(existing.st_mode))


def append_within(root: Path, path: Path, text: str) -> None:
    """Append `text` to the file at `path`, inside `root`, by replacing it with what it holds and `text`.

    Raises:
        UnsafeWrite: As `write_within` raises it.
        OSError: The file cannot be read or written.
    """
    folders, name = _split(root, path)
    with _folder(root, folders, path, create=True) as folder:
        existing = _existing(folder, name, path)
        held = b"" if existing is None else _read(folder, name)
        _replace(
            folder, name, held + text.encode("utf-8"), None if existing is None else stat.S_IMODE(existing.st_mode)
        )


def create_within(root: Path, path: Path, data: bytes, mode: int = 0o600) -> None:
    """Create the file at `path`, inside `root`, holding `data`; fail when anything is there already.

    Two runs racing to create the same file cannot both succeed, which is what a lock needs.

    Raises:
        FileExistsError: Something, a symbolic link among them, is already at `path`.
        UnsafeWrite: `path` lies outside `root`, or a link is on the way to it.
        OSError: The folder or the file cannot be written.
    """
    folders, name = _split(root, path)
    with _folder(root, folders, path, create=True) as folder:
        descriptor = os.open(name, _CREATE_FLAGS, mode, dir_fd=folder)
        try:
            _write_all(descriptor, data)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def make_folders_within(root: Path, path: Path) -> None:
    """Create the folder at `path`, inside `root` or `root` itself, and every missing one on the way.

    Raises:
        UnsafeWrite: `path` lies outside `root`, or a link or a file is at it or on the way.
        OSError: A folder cannot be created.
    """
    with _folder(root, _parts(root, path), path, create=True):
        pass


def remove_tree_within(root: Path, path: Path) -> None:
    """Remove the folder at `path`, inside `root`, and everything below it; nothing when absent.

    The folder is removed through its parent's descriptor, reached without following a link,
    and the removal never follows one below it: a folder swapped for a link after it was
    checked is refused, not followed.

    Raises:
        UnsafeWrite: `path` lies outside `root`, or a link or a file is at it or on the way.
        OSError: Something below the folder cannot be removed.
    """
    folders, name = _split(root, path)
    with contextlib.ExitStack() as opened:
        try:
            parent = opened.enter_context(_folder(root, folders, path, create=False))
            found = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return
        if stat.S_ISLNK(found.st_mode):
            raise UnsafeWrite(path, "it is a symbolic link, which SST does not remove through")
        if not stat.S_ISDIR(found.st_mode):
            raise UnsafeWrite(path, "it is not a folder")
        try:
            shutil.rmtree(name, dir_fd=parent)
        except OSError as exc:
            # rmtree refuses a folder swapped for a link after the check above.
            if stat.S_ISLNK(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode):
                raise UnsafeWrite(path, "it changed into a symbolic link while SST was removing it") from exc
            raise


def remove_within(root: Path, path: Path) -> None:
    """Remove the file at `path`, inside `root`; nothing when absent.

    It is unlinked by name in its folder's descriptor, reached without following a link, so a
    folder on the way swapped for a link cannot redirect the removal outside `root`.

    Raises:
        UnsafeWrite: `path` lies outside `root`, names `root` itself, or a link or a file is on
            the way to it, or it is a folder.
        OSError: The file cannot be removed.
    """
    folders, name = _split(root, path)
    try:
        with _folder(root, folders, path, create=False) as parent:
            if stat.S_ISDIR(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode):
                raise UnsafeWrite(path, "it is a folder, not a file")
            os.unlink(name, dir_fd=parent)
    except FileNotFoundError:
        return


@contextmanager
def scratch_folder(prefix: str) -> Iterator[Path]:
    """Yield a new private temporary folder, removed with everything in it when the block ends."""
    folder = Path(tempfile.mkdtemp(prefix=prefix))
    try:
        yield folder
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _split(root: Path, path: Path) -> tuple[tuple[str, ...], str]:
    """Return the folders from `root` to the file at `path`, and the file's name.

    Raises:
        UnsafeWrite: `path` does not lie below `root`, or names `root` itself.
    """
    parts = _parts(root, path)
    if not parts:
        raise UnsafeWrite(path, "it names the folder SST writes in, not a file")
    return parts[:-1], parts[-1]


def _parts(root: Path, path: Path) -> tuple[str, ...]:
    """Return the names from `root` down to `path`, each `..` taken away as written.

    Raises:
        UnsafeWrite: `path`, its `..` taken away, lies outside `root`.
    """
    target = Path(os.path.abspath(path))
    for base in (Path(os.path.abspath(root)), root.resolve()):
        if target.is_relative_to(base):
            return target.relative_to(base).parts
    raise UnsafeWrite(path, f"it lies outside {root}")


@contextmanager
def _folder(root: Path, folders: tuple[str, ...], path: Path, *, create: bool) -> Iterator[int]:
    """Open each folder from `root` down, never following a link; yield the last one's descriptor.

    `root` itself may be reached through a link, and is created when `create` and it is missing.
    """
    if create:
        os.makedirs(root, exist_ok=True)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for index, folder in enumerate(folders):
            shown = Path(*folders[: index + 1]).as_posix()
            opened = _open_folder(descriptor, folder, shown, path, create=create)
            os.close(descriptor)
            descriptor = opened
        yield descriptor
    finally:
        os.close(descriptor)


def _open_folder(parent: int, name: str, shown: str, path: Path, *, create: bool) -> int:
    """Open the folder `name` in `parent` without following a link; create it when missing and asked."""
    try:
        found = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        if not create:
            raise
        os.mkdir(name, dir_fd=parent)
    else:
        if stat.S_ISLNK(found.st_mode):
            raise UnsafeWrite(path, f"{shown} is a symbolic link, which SST does not write through")
        if not stat.S_ISDIR(found.st_mode):
            raise UnsafeWrite(path, f"{shown} is not a folder")
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
    except OSError as exc:
        # Swapped for a link or a file after it was checked: refused all the same.
        raise UnsafeWrite(path, f"{shown} changed while SST was writing to it") from exc


def _existing(folder: int, name: str, path: Path) -> os.stat_result | None:
    """Return what is at `name` in `folder`; None when nothing is; refuse a link or a non-file."""
    try:
        found = os.stat(name, dir_fd=folder, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(found.st_mode):
        raise UnsafeWrite(path, "it is a symbolic link, which SST does not write through")
    if not stat.S_ISREG(found.st_mode):
        raise UnsafeWrite(path, "it is not a regular file")
    return found


def _read(folder: int, name: str) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=folder)
    chunks: list[bytes] = []
    try:
        while chunk := os.read(descriptor, 1 << 16):
            chunks.append(chunk)
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _replace(folder: int, name: str, payload: bytes, mode: int | None) -> None:
    """Write `payload` to a new temporary file in `folder`, then rename it over `name`."""
    temporary = f".sst-{secrets.token_hex(8)}.tmp"
    descriptor = os.open(temporary, _CREATE_FLAGS, 0o666, dir_fd=folder)
    try:
        try:
            if mode is not None:
                os.fchmod(descriptor, mode)
            _write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, name, src_dir_fd=folder, dst_dir_fd=folder)
        os.fsync(folder)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=folder)


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(descriptor, view) :]
