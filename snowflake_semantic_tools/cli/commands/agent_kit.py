"""`sst agent-kit`: install SST's skills for a coding agent, and keep them current.

The kit is a set of skills that teach a coding agent to use SST well: how to author, validate,
and publish, how to read a diagnostic, and how to report a bug. It ships inside the package, so
each SST installs its own kit, with links to its own documentation. `install` copies it into
the folder the agent reads project skills from -- `.cortex/skills` for Cortex Code, `.claude/skills`
for Claude Code -- and writes `.sst/agent-kit.json`, the lock that records what each file held.
`upgrade` brings the installed files to the kit of the SST now installed, `status` compares
them, and `remove` takes them out. A file the user has changed is never overwritten without
--force. The files land on disk as ordinary changes to review and commit; SST never runs git,
reads no `sst_config.yml` setting of its own, and never connects to Snowflake.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.agent_kit import (
    LOCK_FILE,
    Kit,
    KitWriteError,
    apply_kit_plans,
    kit_digests,
    read_installed,
    read_kit,
    read_lock,
)
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import Decorator
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, WriteFailure, command_body
from snowflake_semantic_tools.cli.settings import project_config
from snowflake_semantic_tools.domain.agent_kit import (
    WRITES,
    ActionKind,
    AgentTarget,
    KitLock,
    KitPlan,
    SkillDigests,
    SkillStatus,
    overlap_diagnostic,
    plan_install,
    plan_remove,
    plan_upgrade,
    published_overlap,
    skill_statuses,
)
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_KEYS, configured_dir

_SUBCOMMANDS = ("install", "upgrade", "status", "remove")
_AGENTS = tuple(agent.value for agent in AgentTarget)
# The kinds a human report counts as a change to the working tree.
_CHANGES = frozenset((*WRITES, ActionKind.REMOVE))

# One agent's plan; the plan alone cannot say whose it is when it holds no action.
AgentPlan = tuple[AgentTarget, KitPlan]


def _agent_option() -> Decorator:
    """`--agent`: repeatable; which coding agents to act for."""
    return click.option("--agent", "agents", type=click.Choice(_AGENTS), multiple=True)


def _only_option() -> Decorator:
    """`--only`: repeatable; act on just these skills."""
    return click.option("--only", "only", multiple=True, metavar="SKILL")


@click.group(name="agent-kit")
@click.pass_context
def agent_kit(ctx: click.Context) -> None:
    """Install SST's skills for a coding agent into this project, and keep them current.

    The kit ships inside the package, so each SST installs the skills written for it. Skills go
    to `.cortex/skills` (--agent cortex) or `.claude/skills` (--agent claude), and
    `.sst/agent-kit.json` records what SST wrote, so `upgrade` and `remove` touch only those
    files and never one you changed, unless --force. Nothing connects to Snowflake and nothing
    is committed: the files are ordinary changes in the working tree. The kit is for a coding
    agent; it is never published, and it may not sit inside a directory SST publishes from.
    """
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in _SUBCOMMANDS}


@agent_kit.command(cls=SstCommand, name="install")
@_agent_option()
@_only_option()
@click.option("--dry-run", is_flag=True)
@command_body("agent-kit install", config=ConfigNeed.OPTIONAL, applies_baseline=False)
def install_command(
    paths: ProjectPaths, agents: tuple[str, ...], only: tuple[str, ...], dry_run: bool
) -> CommandResult:
    """Install the kit's skills for each --agent, writing only files that are not there.

    Without --agent, the agent whose folder (`.cortex` or `.claude`) the project has; with both
    or neither, --agent is required. --only installs just the skills it names, and adds them to
    those installed before. A skill already installed is left as it is, even when out of date:
    `upgrade` updates it. A folder already holding the kit's files exactly is adopted; any
    other folder or file in the way refuses the whole run, and nothing is written.

    Exit 0 when the kit is installed, or with --dry-run would be; 1 when something is in the
    way or cannot be written; 3 when --only names no kit skill or the agent is ambiguous; 4
    when the kit's folder overlaps a directory SST publishes from.

    Diagnostics:
        SST-CFG052: the agent's skills folder overlaps a `project.*_dir` directory.
        SST-PRT013: a skill folder or file is in the way.
        SST-PRT100: --only names no kit skill, or no agent can be chosen; raised.
    """
    session = _Session.read(paths)
    unknown = sorted(set(only) - set(session.kit))
    if unknown:
        raise SstUsageError(f"--only names no skill of the kit: {', '.join(unknown)}; it ships {_names(session.kit)}")
    targets = _given(agents) or _detected(paths.project_dir)
    if (refusal := session.overlap(paths, targets)) is not None:
        return refusal
    lock = session.lock
    plans: list[AgentPlan] = []
    for agent in targets:
        plan = plan_install(session.digests, lock, session.installed(agent, lock), agent, wanted=only, version=VERSION)
        plans.append((agent, plan))
        lock = plan.lock
    return session.finish("install", plans, lock, dry_run=dry_run)


@agent_kit.command(cls=SstCommand, name="upgrade")
@_agent_option()
@click.option("--force", is_flag=True)
@click.option("--dry-run", is_flag=True)
@command_body("agent-kit upgrade", config=ConfigNeed.OPTIONAL, applies_baseline=False)
def upgrade_command(paths: ProjectPaths, agents: tuple[str, ...], force: bool, dry_run: bool) -> CommandResult:
    """Bring every installed skill to the kit of the SST now installed.

    Acts for each --agent, else for every agent the lock records. A file SST wrote and nobody
    changed is replaced, added, or removed to match the kit; one you changed or deleted is kept
    as it is (SST-PRT014 when the kit changed it too) unless --force, which makes every file
    match the kit. A skill the kit adds is installed for an agent that installed the whole kit,
    and a skill it drops is removed. A file in the way that SST did not write refuses the whole
    run, even with --force.

    Exit 0 when every installed skill is upgraded, kept files included, or with --dry-run would
    be; 1 when something is in the way or cannot be written; 4 when the kit's folder overlaps a
    directory SST publishes from.

    Diagnostics:
        SST-CFG052: the agent's skills folder overlaps a `project.*_dir` directory.
        SST-PRT013: a file or folder the kit now ships is in the way.
        SST-PRT014: a file you changed is kept although the kit changed or dropped it.
    """
    session = _Session.read(paths)
    targets = session.recorded(agents)
    if (refusal := session.overlap(paths, targets)) is not None:
        return refusal
    lock = session.lock
    plans: list[AgentPlan] = []
    for agent in targets:
        plan = plan_upgrade(session.digests, lock, session.installed(agent, lock), agent, force=force, version=VERSION)
        plans.append((agent, plan))
        lock = plan.lock
    return session.finish("upgrade", plans, lock, dry_run=dry_run, absent=session.absent(agents))


@agent_kit.command(cls=SstCommand, name="status")
@_agent_option()
@click.option("--check", is_flag=True)
@command_body("agent-kit status", config=ConfigNeed.OPTIONAL, applies_baseline=False)
def status_command(paths: ProjectPaths, agents: tuple[str, ...], check: bool) -> CommandResult:
    """Show each skill's state against the kit of the SST now installed; change nothing.

    Lists, for each --agent, else each agent the lock records, else each agent whose folder the
    project has, else both: every kit skill, and any installed one the kit dropped, as
    `current`, `outdated` (an upgrade would change it), `modified` (you changed a file SST
    wrote), `missing` (its folder is gone), or `not installed`. With --check, exit 2 when any
    skill is outdated or missing, as `sst format --check` does for YAML.
    """
    session = _Session.read(paths)
    targets = _given(agents) or tuple(session.lock.agents) or _detected(paths.project_dir, required=False)
    report = [
        (agent, skill_statuses(session.digests, session.lock, session.installed(agent, session.lock), agent))
        for agent in targets or tuple(AgentTarget)
    ]
    stale = sum(status.outdated or status.missing for _, statuses in report for status in statuses)
    data = {
        "kit_version": VERSION,
        "installed_by": session.lock.sst_version or None,
        "lock": LOCK_FILE.as_posix(),
        "up_to_date": not stale,
        "agents": [
            {
                "agent": agent.value,
                "root": agent.skills_root,
                "installed": agent in session.lock.agents,
                "skills": [_status_row(status) for status in statuses],
            }
            for agent, statuses in report
        ],
    }
    exit_code = CHANGES if check and stale else OK
    return CommandResult(
        exit_code,
        data=data,
        human=lambda: _print_status(session.lock, report),
        headline=f"{stale} skill(s) out of date" if stale else None,
        next_steps=("run `sst agent-kit upgrade` to bring the kit up to date",) if stale else None,
    )


@agent_kit.command(cls=SstCommand, name="remove")
@_agent_option()
@_only_option()
@click.option("--force", is_flag=True)
@click.option("--dry-run", is_flag=True)
@command_body("agent-kit remove", config=ConfigNeed.OPTIONAL, applies_baseline=False)
def remove_command(
    paths: ProjectPaths, agents: tuple[str, ...], only: tuple[str, ...], force: bool, dry_run: bool
) -> CommandResult:
    """Remove the kit's skills, or those --only names, from each --agent, else every agent installed.

    A file SST wrote and nobody changed is removed, with any folder that leaves empty; one you
    changed is kept, and stays recorded, unless --force (SST-PRT014). A file SST did not write
    is never removed. The lock is removed with the last skill it records. After a partial
    removal `upgrade` maintains only the skills still installed; `install` adds others back.

    Exit 0 when the skills are removed, kept files included, or with --dry-run would be; 1 when
    a file cannot be removed.

    Diagnostics:
        SST-PRT014: a file you changed is kept.
    """
    session = _Session.read(paths)
    lock = session.lock
    plans: list[AgentPlan] = []
    for agent in session.recorded(agents):
        plan = plan_remove(lock, session.installed(agent, lock), agent, wanted=only, force=force, version=VERSION)
        plans.append((agent, plan))
        lock = plan.lock
    return session.finish("remove", plans, lock, dry_run=dry_run, absent=session.absent(agents))


@dataclass(frozen=True)
class _Session:
    """The kit of the installed SST, its digests, and the project's lock, read once per command."""

    project_dir: Path
    kit: Kit
    digests: SkillDigests
    lock: KitLock

    @classmethod
    def read(cls, paths: ProjectPaths) -> _Session:
        """Read the kit and the lock; an unreadable lock ends the run, exit 4 (SST-PRT009)."""
        kit = read_kit(VERSION)
        return cls(paths.project_dir, kit, kit_digests(kit), read_lock(paths.project_dir))

    def installed(self, agent: AgentTarget, lock: KitLock) -> SkillDigests:
        """Return what is on disk for every skill the kit ships or `lock` records for `agent`."""
        recorded = lock.agents[agent].skills if agent in lock.agents else {}
        return read_installed(self.project_dir, agent, {*self.kit, *recorded})

    def recorded(self, agents: Sequence[str]) -> tuple[AgentTarget, ...]:
        """Return the --agent agents the lock records, else every agent it records."""
        return tuple(agent for agent in _given(agents) or tuple(self.lock.agents) if agent in self.lock.agents)

    def absent(self, agents: Sequence[str]) -> list[str]:
        """Return each agent --agent names that has nothing installed, which a command skips."""
        return [agent.value for agent in _given(agents) if agent not in self.lock.agents]

    def overlap(self, paths: ProjectPaths, agents: Collection[AgentTarget]) -> CommandResult | None:
        """Refuse, exit 4, when an agent's skills folder overlaps a directory SST publishes from.

        Diagnostics:
            SST-CFG052: the folder overlaps a `project.*_dir` directory, configured or default.
        """
        published = _published_dirs(paths)
        found: list[Diagnostic] = []
        for agent in agents:
            if (overlap := published_overlap(agent.skills_root, published)) is not None:
                found.append(overlap_diagnostic(agent.skills_root, *overlap))
        if not found:
            return None
        return CommandResult(CONFIG, DiagnosticBag(tuple(found)), {"refused": True, "agents": []})

    def finish(
        self,
        command: str,
        plans: Sequence[AgentPlan],
        lock: KitLock,
        *,
        dry_run: bool,
        absent: Sequence[str] = (),
    ) -> CommandResult:
        """Carry the plans out unless one is refused or this is a dry run, and report them.

        Nothing is written when no plan changes a file and the lock is as it was.

        Raises:
            WriteFailure: a file or the lock could not be written (SST-PRT008, exit 1).
        """
        diagnostics = DiagnosticBag(tuple(item for _, plan in plans for item in plan.diagnostics))
        refused = any(plan.refused for _, plan in plans)
        changes = any(action.kind in _CHANGES for _, plan in plans for action in plan.actions)
        if not refused and not dry_run and (changes or lock != self.lock):
            try:
                apply_kit_plans(self.project_dir, [plan for _, plan in plans], self.kit, lock)
            except KitWriteError as exc:
                raise WriteFailure(exc.path, exc.cause) from exc
        data = {
            "kit_version": VERSION,
            "lock": LOCK_FILE.as_posix(),
            "dry_run": dry_run,
            "refused": refused,
            "not_installed": list(absent),
            "agents": [_plan_row(agent, plan) for agent, plan in plans],
        }
        verb = command.split()[-1]
        return CommandResult(
            ERROR if refused else OK,
            diagnostics,
            data,
            human=lambda: _print_plans(verb, plans, dry_run=dry_run, refused=refused, absent=absent),
        )


def _given(agents: Sequence[str]) -> tuple[AgentTarget, ...]:
    """Return the agents --agent names, each once, in the order given."""
    return tuple(AgentTarget(value) for value in dict.fromkeys(agents))


def _detected(project_dir: Path, *, required: bool = True) -> tuple[AgentTarget, ...]:
    """Return the one agent whose folder the project has; every one found when not `required`.

    Raises:
        SstUsageError: the project has both folders or neither, and `required` (SST-PRT100).

    Diagnostics:
        SST-PRT100: no agent can be chosen without --agent; raised.
    """
    present = tuple(agent for agent in AgentTarget if (project_dir / agent.folder).is_dir())
    if len(present) == 1 or not required:
        return present
    found = " and ".join(agent.folder for agent in present) if present else "neither .cortex nor .claude"
    raise SstUsageError(f"the project has {found}; name the agent with --agent {' or --agent '.join(_AGENTS)}")


def _published_dirs(paths: ProjectPaths) -> dict[str, str]:
    """Return every `project.*_dir` key, unprefixed, with the folder it names: configured, else default."""
    tree = project_config(paths) if paths.config_file is not None else {}
    names = [key.removeprefix("project.") for key in CONFIG_KEYS if key.startswith("project.") and key.endswith("_dir")]
    return {name: configured_dir(tree, name, CONFIG_KEYS[f"project.{name}"].default or "") for name in names}


def _names(kit: Kit) -> str:
    return ", ".join(sorted(kit))


def _plan_row(agent: AgentTarget, plan: KitPlan) -> dict[str, object]:
    """Return one agent's plan as the envelope reports it: each action, and the skills skipped."""
    return {
        "agent": agent.value,
        "root": agent.skills_root,
        "actions": [
            {"action": action.kind.value, "skill": action.skill, "path": action.location, "reason": action.reason}
            for action in plan.actions
        ],
        "skipped": list(plan.skipped),
    }


def _status_row(status: SkillStatus) -> dict[str, object]:
    return {
        "name": status.name,
        "state": status.state,
        "outdated": status.outdated,
        "modified": status.modified,
        "missing": status.missing,
    }


def _print_plans(verb: str, plans: Sequence[AgentPlan], *, dry_run: bool, refused: bool, absent: Sequence[str]) -> None:
    """Print each action, one per line, then one line that sums the run up."""
    changed = 0
    for _, plan in plans:
        for action in plan.actions:
            reason = f" ({action.reason})" if action.reason else ""
            click.echo(f"{action.kind.value:<8} {action.location}{reason}")
            changed += action.kind in _CHANGES
        for name in plan.skipped:
            click.echo(f"{'skip':<8} {name} (already installed; `sst agent-kit upgrade` updates it)")
    for agent in absent:
        click.echo(f"{'skip':<8} {agent} (nothing installed; run `sst agent-kit install --agent {agent}`)")
    if refused:
        click.echo(f"{verb} refused: nothing was written")
    elif not plans:
        click.echo("nothing installed; run `sst agent-kit install`")
    elif dry_run:
        click.echo(f"{verb} would change {changed} file(s); nothing was written")
    elif changed:
        click.echo(
            f"{verb} changed {changed} file(s); review the change, {LOCK_FILE.as_posix()} included, and commit it"
        )
    else:
        click.echo(f"{verb}: nothing to change")


def _print_status(lock: KitLock, report: Sequence[tuple[AgentTarget, Sequence[SkillStatus]]]) -> None:
    """Print each agent's skills and their states; the verdict says whether to upgrade."""
    installed = f"installed by SST {lock.sst_version}" if lock.sst_version else "nothing installed"
    click.echo(f"kit of SST {VERSION}; {installed}")
    for agent, statuses in report:
        click.echo(f"{agent.value} ({agent.skills_root}){'' if agent in lock.agents else ': not installed'}")
        for status in statuses:
            click.echo(f"  {status.state:<14} {status.name}")
