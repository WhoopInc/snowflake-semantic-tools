"""The agent kit on disk: the skills the package ships, the lock file, and the files installed.

The kit is the folder `skills/` beside this module, one folder per skill, read from the
installed package so a project always gets the kit of the SST it runs. Its Markdown is rendered
for that SST as it is read. Everything installed is read without following a link: a linked
skills root, skill folder, or file is refused (SST-PRT009) rather than read through. Every
write and removal goes through `adapters.paths`, inside the project.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterator, Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.adapters.paths import (
    remove_empty_folder_within,
    remove_within,
    walk_refusal,
    write_within,
)
from snowflake_semantic_tools.domain.agent_kit import (
    WRITES,
    ActionKind,
    AgentTarget,
    KitLock,
    KitPlan,
    LockFormatError,
    SkillDigests,
    content_digest,
    lock_document,
    parse_lock,
    render_kit_text,
)
from snowflake_semantic_tools.domain.diagnostics import D, Origin

KIT_ROOT = Path(__file__).parent / "skills"
LOCK_FILE = Path(".sst") / "agent-kit.json"
# Files rendered for the installing SST; anything else is copied byte for byte.
_RENDERED = frozenset((".md",))

# A skill's files as the kit ships them, rendered: path inside its folder to bytes.
KitFiles = Mapping[str, bytes]
Kit = Mapping[str, KitFiles]


class KitWriteError(Exception):
    """A kit file, folder, or the lock could not be written or removed.

    Attributes:
        path: The file or folder that failed.
        cause: Why, as the file system reported it.
    """

    def __init__(self, path: Path, cause: OSError) -> None:
        super().__init__(f"could not write {path}: {cause}")
        self.path = path
        self.cause = cause


def read_kit(version: str, root: Path = KIT_ROOT) -> Kit:
    """Return every skill the package ships, its Markdown rendered for SST `version`.

    A skill is a folder of `root` holding a `SKILL.md`. Hidden files and Python caches are not
    part of a skill.
    """
    kit: dict[str, dict[str, bytes]] = {}
    for folder in sorted(path for path in root.iterdir() if path.is_dir() and (path / "SKILL.md").is_file()):
        files: dict[str, bytes] = {}
        for path in _files(folder):
            data = path.read_bytes()
            if path.suffix in _RENDERED:
                data = render_kit_text(data.decode("utf-8"), version).encode("utf-8")
            files[path.relative_to(folder).as_posix()] = data
        kit[folder.name] = files
    return kit


def kit_digests(kit: Kit) -> SkillDigests:
    """Return the digest of every file of `kit`, as the planners compare them."""
    return {name: {path: content_digest(data) for path, data in files.items()} for name, files in kit.items()}


def read_lock(project_dir: Path) -> KitLock:
    """Return the project's agent-kit lock; an empty one when the project has none.

    Raises:
        ProjectError: the lock is reached through a link, cannot be read, or is not a version 1
            agent-kit lock (SST-PRT009).

    Diagnostics:
        SST-PRT009: the lock cannot be read or does not hold a lock; raised.
    """
    path = project_dir / LOCK_FILE
    if not path.exists() and not path.is_symlink():
        return KitLock("")
    reason = walk_refusal(project_dir, path)
    if reason is None:
        try:
            return parse_lock(read_json_file(path))
        except (OSError, JsonFileError, LockFormatError) as exc:
            reason = str(exc)
    shown = LOCK_FILE.as_posix()
    raise ProjectError(
        f"could not read {shown}: {reason}",
        diagnostics=(D("SST-PRT009", origin=Origin(shown), subject="cli", path=shown, detail=reason),),
    )


def read_installed(project_dir: Path, agent: AgentTarget, names: Collection[str]) -> SkillDigests:
    """Return each of the skill folders `names` under the agent's skills root, with its files' digests.

    A folder that is absent is left out. Hidden files and Python caches are skipped, as the
    kit skips them.

    Raises:
        ProjectError: the skills root, a skill folder, or a file in one is a symbolic link or is
            reached through one (SST-PRT009).

    Diagnostics:
        SST-PRT009: a path the kit would read is a link; raised.
    """
    root = project_dir / agent.skills_root
    _refuse_link(project_dir, root)
    found: dict[str, dict[str, str]] = {}
    for name in sorted(names):
        folder = root / name
        if not folder.exists() and not folder.is_symlink():
            continue
        _refuse_link(project_dir, folder)
        files: dict[str, str] = {}
        for path in _files(folder):
            _refuse_link(project_dir, path)
            files[path.relative_to(folder).as_posix()] = content_digest(path.read_bytes())
        found[name] = files
    return found


def apply_kit_plans(project_dir: Path, plans: Collection[KitPlan], kit: Kit, lock: KitLock) -> None:
    """Carry out every action of `plans`, then write `lock`, or remove the lock file when it is empty.

    A skill folder emptied by a removal is removed as well, up to the agent's skills root.

    Raises:
        KitWriteError: a file or folder could not be written or removed; the files written
            before it stay written, and the lock is not written.
    """
    for plan in plans:
        for action in plan.actions:
            path = project_dir / action.location
            try:
                if action.kind in WRITES:
                    write_within(project_dir, path, kit[action.skill][action.path])
                elif action.kind is ActionKind.REMOVE:
                    remove_within(project_dir, path)
                    _prune(project_dir, path.parent, project_dir / action.agent.skills_root)
            except OSError as exc:
                raise KitWriteError(path, exc) from exc
    path = project_dir / LOCK_FILE
    try:
        if lock.agents:
            write_within(project_dir, path, json.dumps(lock_document(lock), indent=2, ensure_ascii=False) + "\n")
        else:
            remove_within(project_dir, path)
    except OSError as exc:
        raise KitWriteError(path, exc) from exc


def _prune(project_dir: Path, folder: Path, root: Path) -> None:
    """Remove `folder` and each folder above it that is left empty, stopping below `root`."""
    while folder != root and folder.is_relative_to(root) and remove_empty_folder_within(project_dir, folder):
        folder = folder.parent


def _files(folder: Path) -> Iterator[Path]:
    """Yield every file below `folder`, in path order, skipping hidden entries and Python caches."""
    for path in sorted(folder.rglob("*")):
        parts = path.relative_to(folder).parts
        if any(part.startswith(".") or part == "__pycache__" for part in parts) or path.suffix == ".pyc":
            continue
        if path.is_file() or path.is_symlink():
            yield path


def _refuse_link(project_dir: Path, path: Path) -> None:
    """Refuse to read `path` when it, or a folder on the way to it, is a symbolic link.

    Raises:
        ProjectError: it is (SST-PRT009).

    Diagnostics:
        SST-PRT009: `path` is a link or is reached through one; raised.
    """
    if not path.exists() and not path.is_symlink():
        return
    reason = walk_refusal(project_dir, path)
    if reason is None:
        return
    shown = path.relative_to(project_dir).as_posix()
    raise ProjectError(
        f"could not read {shown}: {reason}",
        diagnostics=(D("SST-PRT009", origin=Origin(shown), subject="cli", path=shown, detail=reason),),
    )
