"""Path containment: whether a file SST reads stays inside the project it was found in.

Every loader that reads a file a project names, or one it finds by walking a project folder,
asks here first, so neither a symbolic link nor a `..` can make SST read, or publish, a file
from outside the project.
"""

from __future__ import annotations

from pathlib import Path


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
