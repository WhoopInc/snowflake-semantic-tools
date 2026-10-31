"""The project's git state: its commit, and the paths changed since it, as `git` reports them.

Each asks `git` once, with no shell; a project outside a work tree reads as an unknown commit
and no changed paths.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from snowflake_semantic_tools.adapters.locations import sst_target_dir
from snowflake_semantic_tools.domain.ports.project import UNKNOWN_GIT_SHA


def git_sha(project_dir: Path) -> str:
    """Return the project's commit as a 7-character sha, or `UNKNOWN_GIT_SHA` when git cannot say."""
    completed = subprocess.run(
        ["git", "-C", str(project_dir), "rev-parse", "--short=7", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and value else UNKNOWN_GIT_SHA


def git_dirty(project_dir: Path) -> tuple[str, ...]:
    """Return the project's uncommitted paths, sorted; none when it is clean or git cannot say.

    `git status` lists changed and untracked paths under the project, relative to the work
    tree's root. SST's build directory is left out, since every compile writes it.
    """
    excluded = f":(exclude){sst_target_dir(project_dir).relative_to(project_dir).as_posix()}"
    completed = subprocess.run(
        ["git", "-C", str(project_dir), "status", "--porcelain", "-z", "--", ".", excluded],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return ()
    paths: set[str] = set()
    entries = iter(completed.stdout.split("\0"))
    for entry in entries:
        if len(entry) > 3:
            paths.add(entry[3:])
            # A rename or copy is followed by the path it came from, which is not a change.
            if entry[0] in "RC":
                next(entries, None)
    return tuple(sorted(paths))
