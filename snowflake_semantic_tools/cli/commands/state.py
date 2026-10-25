"""`sst state`: repair a target's state table: recover lost entries, convert its kind, or unlock it.

`sst state recover` plans the target as `sst plan` does, then records, under the run lock and
without writing any object, each object state does not record that its ownership marker, or
for a composite artifact its content, proves holds the compiled definition (`app/recover.py`).
`sst state adopt` records, the same way, each selected object SST did not publish whose live
definition matches the compiled one, member by member (`app/adopt.py`), so the next apply
writes SST's marker on it. `sst state convert` rebuilds the state and
lock tables as hybrid or standard tables, keeping the old ones as backups
(`adapters/snowflake/connector/convert.py`). Both ask before they write, and off a terminal
refuse without `--yes`. `sst state unlock` releases the run lock of one named run that will
never end, and always needs `--yes`.
"""

from __future__ import annotations

import socket
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.adopt import AdoptedArtifact, AdoptObjects
from snowflake_semantic_tools.app.observe import ObserveOptions
from snowflake_semantic_tools.app.plan_values import PlanRefused
from snowflake_semantic_tools.app.recover import RecoverResult, RecoverState
from snowflake_semantic_tools.cli.confirm import confirm, require_answerable
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.help_form import SstSubGroup
from snowflake_semantic_tools.cli.options import break_stale_lock_option, selection_options, target_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, run_baseline
from snowflake_semantic_tools.cli.settings import drift_check_setting, threads_setting
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest, plan_runtime
from snowflake_semantic_tools.cli.wiring.project import connect, git_sha, state_store
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import RECORDED
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import TABLE_KINDS, lock_table_for

_SUBCOMMANDS = ("recover", "adopt", "convert", "unlock")


@click.group(cls=SstSubGroup, name="state")
@click.pass_context
def state(ctx: click.Context) -> None:
    """Repair a target's state table: recover or adopt entries, convert its kind, or release a lock.

    State records what apply published to each target, in the state table `state:` names. These
    commands write that table and its lock table, and never a published object.
    """
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in _SUBCOMMANDS}


@state.command(cls=SstCommand, name="recover")
@target_option()
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@break_stale_lock_option()
@command_body("state recover")
def recover(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    assume_yes: bool,
    break_stale_lock: bool,
    options: GlobalOptions,
) -> CommandResult:
    """Record what state lost, from ownership markers and content digests on the target's live objects.

    Plans the target, then records, under the run lock and without writing any object, each
    artifact state does not record whose live objects prove they hold the compiled definition,
    so `sst plan` then shows it unchanged. A semantic view, tool or agent proves it by the
    marker in its comment; a skill or plugin by its aliased version's files and the bundle
    prefix they came from, read back byte for byte; a profile by its registry row as Desktop
    reads it; an eval by its dataset version's provenance and its staged config's bytes.
    Every other unrecorded artifact is listed with what the next apply will do: one SST cannot
    prove stays unmanaged (SST-PLN024), and one that holds another definition is published
    again. Eval baselines are not recovered.

    Exit 0 when recover ran, whatever it recorded; 1 when another run holds the lock or the
    plan is refused; 3 when it would have to ask and there is no terminal, without --yes.

    Diagnostics:
        SST-PRT109: there is no terminal to ask on, and --yes was not given; raised.
        SST-APL032: what recover recorded, as a summary.
        SST-APL033: an unrecorded artifact recover could not record, and what apply will do.
        SST-APL011: another run holds the lock.
        SST-APL010: an expired lock was taken over, with --break-stale-lock.
    """
    if not assume_yes:
        require_answerable("sst state recover")
    request = PlanRequest(
        paths,
        target_name,
        manifest_path,
        (),
        (),
        prune=False,
        partial=False,
        # Recover only needs the plan: warnings never stop it, and no statement is test-compiled.
        strict=False,
        connected=False,
        threads=threads_setting(paths, None),
        observe_options=ObserveOptions(grants=False, drift_check=drift_check_setting(paths)),
        baseline=run_baseline(options),
    )
    session = plan_runtime(request)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics, promoted=session.promoted)
    try:
        if not assume_yes:
            confirm(
                f"Record lost state entries for target {session.profile.target_name} "
                "from ownership markers and content digests?"
            )
        result = RecoverState(
            session.port,
            session.state_store,
            SystemClock(),
            state_table=session.profile.state_table,
            actor=session.profile.identity.role or "",
            host=socket.gethostname(),
            git_sha=git_sha(paths.project_dir),
        ).run(
            session.ready.changeset,
            session.ready.state,
            session.profile.target_name,
            break_stale=break_stale_lock,
            lifecycle_handlers=session.ready.lifecycle_handlers,
        )
    finally:
        session.port.close()
    data = {
        "target": session.profile.target_name,
        "state_table": session.profile.state_table.sql,
        "recorded": list(result.recorded),
        "artifacts": [
            {"key": item.key, "found": item.found, "object": item.qualified_name, "next_apply": item.next_apply}
            for item in result.artifacts
        ],
    }
    exit_code = ERROR if result.refused or result.diagnostics.has_errors else OK
    return CommandResult(exit_code, result.diagnostics, data, human=lambda: _print_recovered(result))


def _print_recovered(result: RecoverResult) -> None:
    if not result.artifacts:
        click.echo("state records every planned artifact; nothing to recover")
        return
    width = max(len(item.key) for item in result.artifacts)
    for item in result.artifacts:
        then = "" if item.found == RECORDED else f"  (next apply: {item.next_apply})"
        click.echo(f"{item.key:<{width}}  {item.found}{then}")


@state.command(cls=SstCommand, name="adopt")
@selection_options()
@target_option()
@click.option("--accept-differences", is_flag=True)
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@break_stale_lock_option()
@command_body("state adopt")
def adopt(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    accept_differences: bool,
    assume_yes: bool,
    break_stale_lock: bool,
    options: GlobalOptions,
) -> CommandResult:
    """Hand SST objects it did not publish, once the project compiles each to what is live.

    Plans the selected artifacts, as `sst plan --select` does, and for each one the plan
    blocks as an existing object SST does not manage (SST-PLN024), compares the live object
    with the compiled artifact member by member, as `sst plan` shows an update. Each that
    matches is recorded in state as adopted, under the run lock and without writing any
    object; the next `sst apply` publishes it, writing SST's ownership marker, unless it
    changed after it was adopted (SST-PLN014). One that differs is refused, naming what
    differs, unless --accept-differences records it anyway, so the next apply publishes the
    project's definition over the live one. Semantic views, tools, agents and CoWork profiles
    are adopted; `sst import` writes the files for one. --select is required.

    Exit 0 when every selected artifact was recorded; 1 when one differs, cannot be adopted,
    or another run holds the lock; 3 when there is no --select, or it would ask and there is no
    terminal without --yes.

    Diagnostics:
        SST-PRT100: no --select was given; raised.
        SST-PRT109: there is no terminal to ask on, and --yes was not given; raised.
        SST-IMP020: the live object differs from the compiled artifact.
        SST-IMP021: an artifact was recorded as adopted.
        SST-IMP022: a selected artifact is not an unmanaged object adopt can compare.
        SST-APL011: another run holds the lock.
        SST-APL010: an expired lock was taken over, with --break-stale-lock.
    """
    if not selected:
        detail = "sst state adopt requires --select, naming each artifact to adopt"
        raise SstUsageError(detail, diagnostic=D("SST-PRT100", subject="cli", detail=detail))
    if not assume_yes:
        require_answerable("sst state adopt")
    request = PlanRequest(
        paths,
        target_name,
        manifest_path,
        selected,
        excluded,
        prune=False,
        partial=False,
        strict=False,
        connected=False,
        threads=threads_setting(paths, None),
        observe_options=ObserveOptions(grants=False, drift_check=drift_check_setting(paths)),
        baseline=run_baseline(options),
    )
    session = plan_runtime(request)
    if isinstance(session, PlanRefused):
        return CommandResult(ERROR, session.diagnostics, promoted=session.promoted)
    try:
        adopter = AdoptObjects(
            session.port,
            session.state_store,
            SystemClock(),
            state_table=session.profile.state_table,
            actor=session.profile.identity.role or "",
            host=socket.gethostname(),
            git_sha=git_sha(paths.project_dir),
        )
        changeset = session.ready.changeset
        artifacts, problems = adopter.compare(changeset, session.ready.result, accept_differences=accept_differences)
        pending = [item for item in artifacts if item.recorded]
        if pending and not assume_yes:
            confirm(
                f"Record {len(pending)} object(s) on target {session.profile.target_name} as adopted, "
                "so the next apply writes SST's marker on them?"
            )
        result = adopter.record(artifacts, changeset, session.profile.target_name, break_stale=break_stale_lock)
    finally:
        session.port.close()
    diagnostics = DiagnosticBag((*problems, *result.diagnostics))
    data = {
        "target": session.profile.target_name,
        "state_table": session.profile.state_table.sql,
        "adopted": list(result.wrote),
        "artifacts": [
            {
                "key": item.key,
                "object": item.qualified_name,
                "found": item.found,
                "differences": list(item.differences),
                "recorded": item.key in result.wrote,
            }
            for item in result.artifacts
        ],
    }
    exit_code = ERROR if result.refused or diagnostics.has_errors else OK
    return CommandResult(exit_code, diagnostics, data, human=lambda: _print_adopted(result.artifacts, result.wrote))


def _print_adopted(artifacts: tuple[AdoptedArtifact, ...], wrote: tuple[str, ...]) -> None:
    if not artifacts:
        click.echo("the selection plans no artifact; nothing to adopt")
        return
    width = max(len(item.key) for item in artifacts)
    for item in artifacts:
        status = "adopted" if item.key in wrote else item.found
        click.echo(f"{item.key:<{width}}  {status}")
        for difference in item.differences:
            click.echo(f"{'':<{width}}    {difference}")


@state.command(cls=SstCommand, name="convert")
@target_option()
@click.option("--to", "to_kind", type=click.Choice(TABLE_KINDS), required=True)
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@command_body("state convert")
def convert(
    paths: ProjectPaths,
    target_name: str | None,
    to_kind: str,
    assume_yes: bool,
    options: GlobalOptions,
) -> CommandResult:
    """Rebuild the state and lock tables as hybrid or standard tables, keeping the old ones.

    The state table is copied into a new table of the kind --to names, then the old one is
    renamed to `<name>_<KIND>_BAK` and the new one into its place; the lock table is renamed
    to its backup the same way and created again, empty. Refused, with nothing changed, while
    any target's lock is live, or when the state table's rows change during the copy: run it
    while no apply does. Set `state.+table_kind` to the new kind afterwards, or every run
    refuses the converted tables (SST-APL031). Hybrid tables cannot sit in a transient
    database or schema (SST-APL030), nor on an account that cannot create them (SST-APL035).

    Exit 0 when the tables were converted; 1 when the conversion was refused or failed; 3 when
    it would have to ask and there is no terminal, without --yes.

    Diagnostics:
        SST-PRT109: there is no terminal to ask on, and --yes was not given; raised.
        SST-APL034: the conversion was refused; nothing was changed.
        SST-APL030: a hybrid table cannot be created in the transient database or schema.
        SST-APL035: the account cannot create hybrid tables.
    """
    del options
    if not assume_yes:
        require_answerable("sst state convert")
    profile, port = connect(paths, target_name)
    try:
        if not assume_yes:
            confirm(f"Rebuild {profile.state_table.sql} and its lock table as {to_kind} tables?")
        try:
            conversion = port.convert_state_tables(profile.state_table, to_kind)
        except SnowflakePortError as exc:
            if exc.diagnostic is None:
                raise
            return CommandResult(ERROR, DiagnosticBag((exc.diagnostic,)), {"state_table": profile.state_table.sql})
    finally:
        port.close()
    data = {
        "state_table": profile.state_table.sql,
        "from": conversion.from_kind,
        "to": conversion.to_kind,
        "rows": conversion.rows,
        "state_backup": conversion.state_backup.sql,
        "lock_backup": conversion.lock_backup.sql if conversion.lock_backup is not None else None,
        "configured": profile.state_table_kind,
    }

    def report() -> None:
        click.echo(
            f"converted {profile.state_table.sql} from {conversion.from_kind} to {to_kind}: {conversion.rows} rows"
        )
        click.echo(f"kept the old state table as {conversion.state_backup.sql}")
        if conversion.lock_backup is not None:
            click.echo(f"kept the old lock table as {conversion.lock_backup.sql}")
        if profile.state_table_kind != to_kind:
            click.echo(f"set state.+table_kind: {to_kind}, or every run refuses the converted tables (SST-APL031)")

    return CommandResult(OK, DiagnosticBag(), data, human=report)


@state.command(cls=SstCommand, name="unlock")
@target_option()
@click.option("--run", "run_id", required=True)
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@command_body("state unlock")
def unlock(
    paths: ProjectPaths,
    target_name: str | None,
    run_id: str,
    assume_yes: bool,
    options: GlobalOptions,
) -> CommandResult:
    """Release the target's run lock held by a run that will never end, live or expired.

    Releases the lock row only while the named run holds it, in one lock transaction, so a lock
    another run took meanwhile is kept; the local lock file is released too when it names the
    run. A run on this machine whose process is gone needs none of this: the next apply
    releases its locks itself (SST-APL010). There is no prompt: --yes is the confirmation.

    Exit 0 when the lock was released; 1 when the run does not hold it; 3 without --yes.

    Diagnostics:
        SST-PRT109: --yes was not given; raised.
        SST-APL040: the run's lock was released.
        SST-APL041: the run does not hold the lock, so nothing in Snowflake was released.
    """
    del options
    if not assume_yes:
        refusal = D("SST-PRT109", subject="cli", command="sst state unlock")
        raise SstUsageError(refusal.message, diagnostic=refusal)
    profile, port = connect(paths, target_name)
    try:
        released = port.force_release_run_lock(profile.state_table, profile.target_name, run_id)
    finally:
        port.close()
    state_store(paths, profile.target_name).release_lock(run_id)
    data = {
        "target": profile.target_name,
        "lock_table": lock_table_for(profile.state_table).sql,
        "run_id": run_id,
        "released": released is not None,
    }
    if released is None:
        refused = D("SST-APL041", value=run_id, target=profile.target_name)
        return CommandResult(ERROR, DiagnosticBag((refused,)), data)
    found = D("SST-APL040", value=released.describe())
    return CommandResult(OK, DiagnosticBag((found,)), data, headline=f"released the run lock of run {run_id}")
