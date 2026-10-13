"""The agent kit: SST's own skills for a coding agent, and how installing them changes a project.

The kit ships inside the package, one folder per skill: a `SKILL.md` and the files it points at.
`sst agent-kit` copies it into the folder a coding agent reads project skills from, and records
in a lock file what each file it wrote held, so a later upgrade or removal touches only files
SST wrote and nobody has changed since. A changed file is the user's: it is kept, never
overwritten, unless the run is forced.

Nothing here reads a file. The adapter hands in the rendered kit, the lock, and what is on disk,
each as a digest per file, and the planners return the actions to take and the lock to write
after them. A conflict -- a folder or file in the way that SST did not write -- refuses the whole
run, so a plan writes everything it lists or nothing.
"""

from __future__ import annotations

import hashlib
import posixpath
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.core import REPOSITORY_URL, docs_url

# A skill's files: the path inside its folder, POSIX-style, to the digest of its bytes.
SkillFiles = Mapping[str, str]
# Skills by folder name, each with its files.
SkillDigests = Mapping[str, SkillFiles]

LOCK_VERSION = 1

# Placeholders a kit file may carry, replaced as it is installed. They are not `{{ }}`
# expressions, which the kit's own examples of SST templates are full of.
VERSION_TOKEN = "@SST_VERSION@"
DOCS_TOKEN = "@SST_DOCS@"
REPOSITORY_TOKEN = "@SST_REPOSITORY@"
TOKENS = (VERSION_TOKEN, DOCS_TOKEN, REPOSITORY_TOKEN)


class AgentTarget(Enum):
    """A coding agent the kit installs for, named as `--agent` takes it."""

    CORTEX = "cortex"
    CLAUDE = "claude"

    @property
    def folder(self) -> str:
        """Return the project folder the agent keeps its settings in, which marks it as in use."""
        return _AGENT_FOLDERS[self]

    @property
    def skills_root(self) -> str:
        """Return the project folder the agent reads project skills from, where the kit goes."""
        return f"{self.folder}/skills"


_AGENT_FOLDERS: Mapping[AgentTarget, str] = {AgentTarget.CORTEX: ".cortex", AgentTarget.CLAUDE: ".claude"}


@dataclass(frozen=True, slots=True)
class AgentInstall:
    """What the kit installed for one agent.

    Attributes:
        selection: The skills asked for by name, sorted; empty when the whole kit was, so an
            upgrade also installs a skill a later kit adds.
        skills: Each installed skill's files, at the digest of what SST last wrote. A file kept
            because the user changed it keeps the digest of what SST wrote before it.
    """

    selection: tuple[str, ...]
    skills: SkillDigests


@dataclass(frozen=True, slots=True)
class KitLock:
    """The lock file: which kit files SST wrote, for which agents.

    Attributes:
        sst_version: The SST that last wrote the lock; empty for a lock that does not say.
        agents: Only agents with at least one skill installed.
    """

    sst_version: str
    agents: Mapping[AgentTarget, AgentInstall] = field(default_factory=dict)


class LockFormatError(ValueError):
    """A lock file that does not hold a version 1 lock; the message says why."""


class ActionKind(Enum):
    """What a plan does to one file."""

    CREATE = "create"
    REPLACE = "replace"
    REMOVE = "remove"
    ADOPT = "adopt"
    KEEP = "keep"
    CONFLICT = "conflict"


# The kinds that write to, or delete from, the disk.
WRITES = frozenset((ActionKind.CREATE, ActionKind.REPLACE))


@dataclass(frozen=True, slots=True)
class KitAction:
    """One file a plan touches, or leaves on purpose.

    Attributes:
        path: The file inside the skill's folder; empty for a conflict on the folder itself.
        reason: Why a file is kept or in conflict; empty otherwise.
    """

    kind: ActionKind
    agent: AgentTarget
    skill: str
    path: str = ""
    reason: str = ""

    @property
    def location(self) -> str:
        """Return the project-relative path the action is about."""
        folder = f"{self.agent.skills_root}/{self.skill}"
        return f"{folder}/{self.path}" if self.path else folder


@dataclass(frozen=True, slots=True)
class KitPlan:
    """What one agent-kit command does to one agent's files, and the lock it leaves.

    Attributes:
        skipped: Skills `install` leaves alone because they are already installed.
        lock: The lock after the plan runs; the lock it started from when the plan is refused.
    """

    actions: tuple[KitAction, ...]
    lock: KitLock
    diagnostics: tuple[Diagnostic, ...] = ()
    skipped: tuple[str, ...] = ()

    @property
    def refused(self) -> bool:
        """Report whether a conflict stops the plan, so nothing it lists may be done."""
        return any(action.kind is ActionKind.CONFLICT for action in self.actions)


@dataclass(frozen=True, slots=True)
class SkillStatus:
    """Where one skill stands against the kit the installed SST ships.

    Attributes:
        installed: The lock records it for the agent.
        outdated: An upgrade would change it: the kit's files differ from what SST wrote, the
            kit no longer ships it, or the kit adds it to an agent that asked for every skill.
        modified: A file SST wrote has changed or gone since.
        missing: Its folder is gone entirely.
    """

    name: str
    installed: bool
    outdated: bool = False
    modified: bool = False
    missing: bool = False

    @property
    def state(self) -> str:
        """Return the one word `sst agent-kit status` shows, the most pressing that applies."""
        if not self.installed:
            return "not installed"
        if self.missing:
            return "missing"
        if self.outdated:
            return "outdated"
        return "modified" if self.modified else "current"


def content_digest(data: bytes) -> str:
    """Return the digest the lock records for a file's bytes."""
    return "sha256:" + hashlib.sha256(data).hexdigest()


def render_kit_text(text: str, version: str) -> str:
    """Replace each placeholder in a kit file with its value for the SST `version` installing it.

    Example:
        `see @SST_DOCS@/guides/agents.md` installed by 1.0.0 reads
        `see https://github.com/<owner>/<repo>/blob/v1.0.0/docs/guides/agents.md`.
    """
    values = {VERSION_TOKEN: version, DOCS_TOKEN: docs_url(version), REPOSITORY_TOKEN: REPOSITORY_URL}
    for token, value in values.items():
        text = text.replace(token, value)
    return text


def published_overlap(root: str, published: Mapping[str, str]) -> tuple[str, str] | None:
    """Return the configuration key and folder of a directory SST publishes that overlaps `root`.

    Folders overlap when one is the other or lies inside it, compared case-folded, since a
    case-insensitive file system makes `Skills` and `skills` one folder. An empty value is not
    a folder and is skipped.

    Returns:
        The first such key, in the mapping's order, with its configured value; None when the
        kit's folder overlaps none of them.
    """
    mine = _normal(root)
    for key, value in published.items():
        if not value.strip():
            continue
        theirs = _normal(value)
        if _within(mine, theirs) or _within(theirs, mine):
            return key, value
    return None


def overlap_diagnostic(root: str, key: str, value: str) -> Diagnostic:
    """Report that the kit's folder overlaps a directory SST publishes from.

    Diagnostics:
        SST-CFG052: `root` and the folder `key` names overlap.
    """
    return D("SST-CFG052", subject="config:project", key=key, value=value, path=root)


def _normal(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/")).strip("/").casefold() or "."


def _within(inner: str, outer: str) -> bool:
    return outer == "." or inner == outer or inner.startswith(outer + "/")


def plan_install(
    bundle: SkillDigests,
    lock: KitLock,
    disk: SkillDigests,
    agent: AgentTarget,
    *,
    wanted: Iterable[str] = (),
    version: str,
) -> KitPlan:
    """Plan installing the `wanted` skills, or the whole kit, for `agent`; never replacing a file.

    A skill already installed is skipped, whatever its state. A folder already there is adopted
    when every kit file in it is byte-identical to the kit's, as after a lost lock, and is
    otherwise a conflict. Asking for the whole kit, now or before, makes the selection the whole
    kit; asking by name adds those names.

    Args:
        disk: Every folder under the agent's skills root, with the files in it.

    Diagnostics:
        SST-PRT013: a skill's folder exists, and SST did not install it.
    """
    asked = sorted(set(wanted))
    previous = lock.agents.get(agent)
    installed = dict(previous.skills) if previous else {}
    actions: list[KitAction] = []
    diagnostics: list[Diagnostic] = []
    skipped: list[str] = []
    for name in asked or sorted(bundle):
        if name in installed:
            skipped.append(name)
            continue
        recorded = _new_skill(agent, name, bundle[name], disk.get(name), actions, diagnostics)
        if recorded is not None:
            installed[name] = recorded
    if previous is None:
        selection: tuple[str, ...] = tuple(asked)
    elif not previous.selection or not asked:
        selection = ()
    else:
        selection = tuple(sorted({*previous.selection, *asked}))
    return _plan(lock, agent, AgentInstall(selection, installed), actions, diagnostics, version, tuple(skipped))


def plan_upgrade(
    bundle: SkillDigests, lock: KitLock, disk: SkillDigests, agent: AgentTarget, *, force: bool, version: str
) -> KitPlan:
    """Plan bringing every skill installed for `agent` to the kit the installed SST ships.

    A file nobody changed is replaced, created, or removed to match the kit. A file the user
    changed or deleted is kept as it is, with SST-PRT014 when the kit changed it too, unless
    `force`. A skill the kit adds is installed when the agent asked for the whole kit, and a
    skill the kit no longer ships is removed as its files allow.

    Diagnostics:
        SST-PRT013: a file or folder the kit now ships is in the way, and SST did not write it.
        SST-PRT014: a file the user changed is kept although the kit changed or dropped it.
    """
    previous = lock.agents[agent]
    actions: list[KitAction] = []
    diagnostics: list[Diagnostic] = []
    installed: dict[str, SkillFiles] = {}
    for name in sorted(bundle) if not previous.selection else [n for n in previous.selection if n in bundle]:
        here = disk.get(name)
        if name in previous.skills:
            installed[name] = _upgrade_skill(
                agent, name, previous.skills[name], bundle[name], here or {}, force, actions, diagnostics
            )
            continue
        recorded = _new_skill(agent, name, bundle[name], here, actions, diagnostics)
        if recorded is not None:
            installed[name] = recorded
    for name in sorted(set(previous.skills) - set(bundle)):
        kept = _retire_skill(agent, name, previous.skills[name], disk.get(name) or {}, force, actions, diagnostics)
        if kept:
            installed[name] = kept
    selection = tuple(name for name in previous.selection if name in bundle or name in installed)
    return _plan(lock, agent, AgentInstall(selection, installed), actions, diagnostics, version)


def plan_remove(
    lock: KitLock, disk: SkillDigests, agent: AgentTarget, *, wanted: Iterable[str] = (), force: bool, version: str
) -> KitPlan:
    """Plan removing the `wanted` installed skills, or every one, from `agent`.

    A file nobody changed is removed; one the user changed is kept with SST-PRT014, unless
    `force`, and stays in the lock so a later run still knows it. The selection becomes the
    skills still installed, so an upgrade never brings back one that was removed.

    Diagnostics:
        SST-PRT014: a file the user changed is kept.
    """
    previous = lock.agents[agent]
    asked = sorted(set(wanted))
    names = [name for name in asked if name in previous.skills] if asked else sorted(previous.skills)
    actions: list[KitAction] = []
    diagnostics: list[Diagnostic] = []
    installed = {name: files for name, files in previous.skills.items() if name not in names}
    skipped = tuple(name for name in asked if name not in previous.skills)
    for name in names:
        kept = _retire_skill(agent, name, previous.skills[name], disk.get(name) or {}, force, actions, diagnostics)
        if kept:
            installed[name] = kept
    selection = tuple(sorted(installed))
    return _plan(lock, agent, AgentInstall(selection, installed), actions, diagnostics, version, skipped)


def skill_statuses(
    bundle: SkillDigests, lock: KitLock, disk: SkillDigests, agent: AgentTarget
) -> tuple[SkillStatus, ...]:
    """Return where each skill stands for `agent`: every kit skill, and any installed one it dropped.

    An agent that asked for skills by name lists those, and the ones installed.
    """
    previous = lock.agents.get(agent) or AgentInstall((), {})
    wanted = set(previous.selection) if previous.selection else set(bundle)
    statuses: list[SkillStatus] = []
    for name in sorted(wanted | set(previous.skills)):
        recorded = previous.skills.get(name)
        if recorded is None:
            statuses.append(SkillStatus(name, installed=False, outdated=agent in lock.agents and name in bundle))
            continue
        here = disk.get(name)
        statuses.append(
            SkillStatus(
                name,
                installed=True,
                outdated=bundle.get(name) != recorded,
                modified=here is not None and any(here.get(path) != digest for path, digest in recorded.items()),
                missing=here is None,
            )
        )
    return tuple(statuses)


def _plan(
    lock: KitLock,
    agent: AgentTarget,
    install: AgentInstall,
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
    version: str,
    skipped: tuple[str, ...] = (),
) -> KitPlan:
    """Return the plan, with the lock it leaves: the old lock when refused, else the agent's new entry.

    A plan that changes no file and leaves the agent's entry as it was leaves the lock exactly as
    it was, so a run with nothing to do rewrites nothing. An agent left with no skill installed
    leaves the lock.
    """
    plan = KitPlan(tuple(actions), lock, tuple(diagnostics), skipped)
    entry = install if install.skills else None
    if plan.refused or (not actions and lock.agents.get(agent) == entry):
        return plan
    agents = {key: value for key, value in lock.agents.items() if key is not agent}
    if entry is not None:
        agents[agent] = entry
    return KitPlan(
        plan.actions, KitLock(version, dict(sorted(agents.items(), key=_by_name))), plan.diagnostics, skipped
    )


def _by_name(item: tuple[AgentTarget, AgentInstall]) -> str:
    return item[0].value


def _new_skill(
    agent: AgentTarget,
    name: str,
    files: SkillFiles,
    here: SkillFiles | None,
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
) -> SkillFiles | None:
    """Create a skill the lock does not record, adopt an identical folder, or report a conflict.

    Returns:
        The files to record; None when the folder is in the way.

    Diagnostics:
        SST-PRT013: the folder exists and does not hold the kit's files exactly.
    """
    if here is None:
        actions.extend(KitAction(ActionKind.CREATE, agent, name, path) for path in sorted(files))
        return files
    if all(here.get(path) == digest for path, digest in files.items()):
        actions.extend(KitAction(ActionKind.ADOPT, agent, name, path) for path in sorted(files))
        return files
    conflict = KitAction(ActionKind.CONFLICT, agent, name, reason="the folder exists and SST did not write it")
    actions.append(conflict)
    diagnostics.append(D("SST-PRT013", subject="cli", path=conflict.location, detail=conflict.reason))
    return None


def _upgrade_skill(
    agent: AgentTarget,
    name: str,
    recorded: SkillFiles,
    files: SkillFiles,
    here: SkillFiles,
    force: bool,
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
) -> SkillFiles:
    """Bring one installed skill's files to the kit's, keeping what the user changed unless `force`.

    Files the kit adds come first, then files it drops, then the files both hold, each group
    in path order.

    Returns:
        The files to record: the kit's digest for each file now as the kit ships it, the old
        digest for a kept file, and none for a removed one.

    Diagnostics:
        SST-PRT013: a file the kit adds is in the way, and SST did not write it.
        SST-PRT014: a changed file is kept although the kit changed or dropped it.
    """
    kept: dict[str, str] = {}
    for path in sorted(set(files) - set(recorded)):
        _add_file(agent, name, path, files[path], here.get(path), kept, actions, diagnostics)
    for path in sorted(set(recorded) - set(files)):
        _retire_file(agent, name, path, recorded[path], here.get(path), force, kept, actions, diagnostics)
    for path in sorted(set(recorded) & set(files)):
        old, new, found = recorded[path], files[path], here.get(path)
        if found == old and new == old:
            kept[path] = old
        elif found == old:
            actions.append(KitAction(ActionKind.REPLACE, agent, name, path))
            kept[path] = new
        elif found == new:
            actions.append(KitAction(ActionKind.ADOPT, agent, name, path))
            kept[path] = new
        elif force:
            actions.append(KitAction(ActionKind.CREATE if found is None else ActionKind.REPLACE, agent, name, path))
            kept[path] = new
        elif new == old:
            actions.append(KitAction(ActionKind.KEEP, agent, name, path, "you changed it"))
            kept[path] = old
        else:
            _keep(agent, name, path, "the kit changed it too", actions, diagnostics)
            kept[path] = old
    return kept


def _add_file(
    agent: AgentTarget,
    name: str,
    path: str,
    digest: str,
    found: str | None,
    kept: dict[str, str],
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
) -> None:
    """Create a file the kit adds to an installed skill, adopt an identical one, or report one in the way.

    Diagnostics:
        SST-PRT013: a different file is already at the path.
    """
    if found is None or found == digest:
        actions.append(KitAction(ActionKind.CREATE if found is None else ActionKind.ADOPT, agent, name, path))
        kept[path] = digest
        return
    conflict = KitAction(ActionKind.CONFLICT, agent, name, path, "the file exists and SST did not write it")
    actions.append(conflict)
    diagnostics.append(D("SST-PRT013", subject="cli", path=conflict.location, detail=conflict.reason))


def _retire_skill(
    agent: AgentTarget,
    name: str,
    recorded: SkillFiles,
    here: SkillFiles,
    force: bool,
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
) -> dict[str, str]:
    """Remove a skill's files, keeping each the user changed unless `force`; return those kept.

    Diagnostics:
        SST-PRT014: a changed file is kept.
    """
    kept: dict[str, str] = {}
    for path in sorted(recorded):
        _retire_file(agent, name, path, recorded[path], here.get(path), force, kept, actions, diagnostics)
    return kept


def _retire_file(
    agent: AgentTarget,
    name: str,
    path: str,
    old: str,
    found: str | None,
    force: bool,
    kept: dict[str, str],
    actions: list[KitAction],
    diagnostics: list[Diagnostic],
) -> None:
    """Remove a file SST wrote, or keep it, still recorded, when the user changed it and not `force`.

    A file already gone is removed all the same, which only forgets it.

    Diagnostics:
        SST-PRT014: the file is kept.
    """
    if found is None or found == old or force:
        actions.append(KitAction(ActionKind.REMOVE, agent, name, path))
        return
    _keep(agent, name, path, "the kit would remove it", actions, diagnostics)
    kept[path] = old


def _keep(
    agent: AgentTarget, name: str, path: str, reason: str, actions: list[KitAction], diagnostics: list[Diagnostic]
) -> None:
    """Keep a file the user changed although the kit would change it, and say so.

    Diagnostics:
        SST-PRT014: the file is kept.
    """
    action = KitAction(ActionKind.KEEP, agent, name, path, reason)
    actions.append(action)
    diagnostics.append(D("SST-PRT014", subject="cli", path=action.location, detail=reason))


def lock_document(lock: KitLock) -> dict[str, object]:
    """Return the lock as the JSON document the lock file holds, every key sorted for review."""
    return {
        "version": LOCK_VERSION,
        "generated_by_sst": lock.sst_version,
        "agents": {
            agent.value: {
                "root": agent.skills_root,
                "selection": list(install.selection),
                "skills": {name: dict(sorted(files.items())) for name, files in sorted(install.skills.items())},
            }
            for agent, install in lock.agents.items()
        },
    }


def parse_lock(document: object) -> KitLock:
    """Read a lock file's parsed JSON into a lock.

    Raises:
        LockFormatError: it is not a version 1 lock, names an agent the kit does not know, or
            holds a selection, skill, or digest that is not text.
    """
    if not isinstance(document, dict) or document.get("version") != LOCK_VERSION:
        raise LockFormatError(f"it is not a version {LOCK_VERSION} agent-kit lock")
    agents = document.get("agents")
    if not isinstance(agents, dict):
        raise LockFormatError("it needs an agents object")
    known = {agent.value: agent for agent in AgentTarget}
    parsed: dict[AgentTarget, AgentInstall] = {}
    for key, entry in agents.items():
        if key not in known:
            raise LockFormatError(f"it names an agent the kit does not know: {key!r}")
        parsed[known[key]] = _install(key, entry)
    return KitLock(str(document.get("generated_by_sst") or ""), parsed)


def _install(key: str, entry: object) -> AgentInstall:
    """Read one agent's entry of the lock; refuse any value that is not of its documented shape.

    Raises:
        LockFormatError: the entry, its selection, or one of its skills is not of its shape.
    """
    if not isinstance(entry, dict):
        raise LockFormatError(f"agent {key!r} is not an object")
    selection = entry.get("selection", [])
    skills = entry.get("skills")
    if not isinstance(selection, list) or not all(isinstance(name, str) for name in selection):
        raise LockFormatError(f"agent {key!r} needs a selection that is a list of skill names")
    if not isinstance(skills, dict):
        raise LockFormatError(f"agent {key!r} needs a skills object")
    files: dict[str, SkillFiles] = {}
    for name, digests in skills.items():
        if not isinstance(digests, dict) or not all(
            isinstance(path, str) and isinstance(digest, str) for path, digest in digests.items()
        ):
            raise LockFormatError(f"skill {name!r} of agent {key!r} needs an object of file digests")
        files[name] = dict(digests)
    return AgentInstall(tuple(sorted(selection)), files)
