"""`sst state`: repair a target's state table: recover lost entries, or convert its kind.

`sst state recover` plans the target as `sst plan` does, then records, under the run lock and
without writing any object, each object state does not record whose ownership marker proves
it holds the compiled definition (`app/recover.py`). `sst state convert` rebuilds the state and
lock tables as hybrid or standard tables, keeping the old ones as backups
(`adapters/snowflake/connector/convert.py`). Both ask before they write, and off a terminal
refuse without `--yes`.
"""

from __future__ import annotations

import socket
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.observe import ObserveOptions
from snowflake_semantic_tools.app.plan import PlanRefused
from snowflake_semantic_tools.app.recover import RECORDED, RecoverResult, RecoverState
from snowflake_semantic_tools.cli.confirm import confirm, require_answerable
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.options import break_stale_lock_option, target_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, run_baseline
from snowflake_semantic_tools.cli.settings import drift_check_setting, threads_setting
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest, plan_runtime
from snowflake_semantic_tools.cli.wiring.project import connect, git_sha
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import TABLE_KINDS

_SUBCOMMANDS = ("recover", "convert")


@click.group(name="state")
@click.pass_context
def state(ctx: click.Context) -> None:
    """Repair a target's state table: recover entries it lost, or convert it between table kinds.

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
    """Record what state lost, from the ownership markers on the target's live objects.

    Plans the target, then records, under the run lock and without writing any object, each
    object state does not record whose marker names the compiled definition under the compiled
    name, so `sst plan` then shows it unchanged. Every other unrecorded artifact is listed with
    what the next apply will do: one with no marker stays unmanaged (SST-PLN024), one with a
    marker for another definition is published again, and a kind with no marker on Snowflake --
    a profile, a skill, an eval -- is published as new.

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
            confirm(f"Record lost state entries for target {session.profile.target_name} from ownership markers?")
        result = RecoverState(
            session.port,
            session.state_store,
            SystemClock(),
            state_table=session.profile.state_table,
            actor=session.profile.identity.role or "",
            host=socket.gethostname(),
            git_sha=git_sha(paths.project_dir),
        ).run(session.ready.changeset, session.ready.state, session.profile.target_name, break_stale=break_stale_lock)
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
    database or schema.

    Exit 0 when the tables were converted; 1 when the conversion was refused or failed; 3 when
    it would have to ask and there is no terminal, without --yes.

    Diagnostics:
        SST-PRT109: there is no terminal to ask on, and --yes was not given; raised.
        SST-APL034: the conversion was refused; nothing was changed.
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
