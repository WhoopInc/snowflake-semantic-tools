"""`sst drop`: remove exactly one named object from one target, break-glass.

For the incident `apply --prune` cannot serve: the repository does not compile, and a bad object
has to come out now. So drop reads no project artifact -- no `sst_config.yml` requirement, no
manifest, no `dbt parse` -- and takes everything else from its command line: the object's full
name, its registered `--type`, which says which DROP statement to run, and `--target`, which has
no default and ignores `$SST_TARGET`. The profile is `dbt_project.yml`'s `profile:`, or
`--profile`. `--yes` is mandatory on every invocation; there is no prompt to fall back to.

Drop refuses an object without SST's ownership marker, holds the target's run lock while it
works, and deletes the object's entries from state. When `sst_config.yml` is present and reads,
its `state:` block locates the state table; otherwise the target's default `SST_STATE` is used,
so a broken configuration never blocks a drop. Every invocation that reaches Snowflake logs one
audit line before the statement and one after, at `warn`, which `--quiet` does not silence.
"""

from __future__ import annotations

import getpass
import os
import socket
import time
from collections.abc import Mapping

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.dbt.profiles import resolve_profile_name
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths, locate_project
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app.drop import DropObject, DropRequest, DropResult
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import break_stale_lock_option
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.cli.wiring.project import connect, state_store
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

# Characters that would make one name stand for many; drop takes exactly one object.
_WILDCARDS = frozenset("*?%[")


def droppable_types() -> Mapping[str, str]:
    """Return each registered type one DROP statement removes, with the object type it names."""
    return {
        name: artifact.object_type
        for name, artifact in sorted(SEMANTIC_REGISTRY.artifacts.items())
        if artifact.object_type
    }


def _refuse(objects: tuple[str, ...], artifact_type: str | None, target_name: str | None, assume_yes: bool) -> None:
    """Refuse a command line drop does not accept, before anything is resolved; exit 3.

    Diagnostics:
        SST-PRT100: not exactly one object, a missing or unregistered --type, or no --target;
            raised.
        SST-PRT110: the object is a pattern rather than one name; raised.
        SST-PRT108: the object is not a three-part name; raised.
        SST-PRT109: --yes was not given; raised.
    """
    if len(objects) != 1:
        _usage(D("SST-PRT100", subject="cli", detail=f"sst drop removes exactly one object; {len(objects)} given"))
    [name] = objects
    if _WILDCARDS & set(_unquoted(name)):
        _usage(D("SST-PRT110", subject="cli", command="sst drop", value=name))
    try:
        QualifiedName.parse(name)
    except ValueError:
        _usage(D("SST-PRT108", subject="cli", command="sst drop", value=name))
    types = ", ".join(droppable_types())
    if artifact_type is None or artifact_type not in droppable_types():
        found = "no --type" if artifact_type is None else f"--type '{artifact_type}' is not a droppable type"
        _usage(D("SST-PRT100", subject="cli", detail=f"sst drop requires --type; {found}; one of {types}"))
    if target_name is None:
        detail = "sst drop requires --target; it has no default and does not read $SST_TARGET"
        _usage(D("SST-PRT100", subject="cli", detail=detail))
    if not assume_yes:
        _usage(D("SST-PRT109", subject="cli", command="sst drop"))


@click.command(cls=SstCommand)
@click.argument("objects", metavar="<DB>.<SCHEMA>.<NAME>", nargs=-1)
@click.option("--type", "artifact_type")
@click.option("--target", "-t", "target_name")
@click.option("--profile", "profile_name")
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@break_stale_lock_option()
@command_body("drop", config=ConfigNeed.NONE, refusals=_refuse)
def drop(
    paths: ProjectPaths,
    options: GlobalOptions,
    objects: tuple[str, ...],
    artifact_type: str,
    target_name: str,
    profile_name: str | None,
    assume_yes: bool,
    break_stale_lock: bool,
) -> CommandResult:
    """Break-glass: drop exactly one object SST owns from one target, and forget it in state.

    Exit 0 when the object was dropped; 1 when Snowflake refused, the object does not exist,
    it carries no SST ownership marker, or another run holds the lock; 3 for a command line drop
    refuses; 4 when the profile or target cannot be resolved; and 5 when Snowflake cannot be
    reached.
    """
    del assume_yes
    files = _state_files(paths)
    profile, port = connect(files, target_name, profile=profile_name or _project_profile(paths))
    request = DropRequest(
        QualifiedName.parse(objects[0]),
        artifact_type,
        droppable_types()[artifact_type],
        profile.target_name,
        break_stale_lock=break_stale_lock,
    )
    role = profile.identity.role or ""
    _audit(
        options,
        f"role={role} target={profile.target_name} profile={profile.profile_name} type={artifact_type} "
        f"object={request.qualified_name.sql} caller={_os_user()}@{socket.gethostname()} pid={os.getpid()}",
    )
    started = time.monotonic()
    try:
        result = DropObject(
            port,
            state_store(files, profile.target_name),
            SystemClock(),
            state_table=profile.state_table,
            actor=role,
            host=socket.gethostname(),
        ).run(request)
    finally:
        port.close()
    elapsed = round((time.monotonic() - started) * 1000)
    _audit(
        options,
        f"outcome={result.outcome} type={artifact_type} object={request.qualified_name.sql} elapsed={elapsed}ms",
    )
    diagnostics: tuple[Diagnostic, ...] = (*profile.connection_warnings, *result.diagnostics)
    data = {
        "object": request.qualified_name.sql,
        "type": artifact_type,
        "target": profile.target_name,
        "profile": profile.profile_name,
        "role": role,
        "outcome": result.outcome,
        "statement": request.statement,
        "state_table": profile.state_table.sql,
        "forgotten": list(result.forgotten),
    }
    exit_code = OK if result.dropped and not any(item.blocks for item in diagnostics) else ERROR
    return CommandResult(exit_code, DiagnosticBag(diagnostics), data, human=lambda: _print(request, result))


def _audit(options: GlobalOptions, record: str) -> None:
    """Write one audit line on stderr at `warn`: `--quiet` cannot silence it, `--log-level error` can."""
    if options.log_level != "error":
        click.echo(f"warn  drop: {record}", err=True)


def _usage(diagnostic: Diagnostic) -> None:
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


def _unquoted(name: str) -> str:
    """Return `name` without its double-quoted parts, where any character is part of a name."""
    return "".join(part for index, part in enumerate(name.split('"')) if index % 2 == 0)


def _state_files(paths: ProjectPaths) -> ProjectPaths:
    """Return the files the state table is located from: the configuration when it reads, else none.

    A configuration that is absent, ambiguous, or broken never stops a drop; the target's default
    state table is then used.
    """
    try:
        located = locate_project(paths.project_dir, None, profiles_dir=paths.profiles_dir, required=False)
        if located.config_file is not None:
            resolved_config(located)
    except (ProjectError, OSError, ValueError):
        return paths
    return located


def _project_profile(paths: ProjectPaths) -> str:
    """Return the profile `dbt_project.yml` names, which `--profile` overrides.

    Raises:
        ProjectError: there is no `dbt_project.yml`, or it names no profile.
    """
    try:
        return resolve_profile_name(ProjectPaths(paths.project_dir, None, profiles_dir=paths.profiles_dir))
    except (ValueError, OSError) as exc:
        detail = f"no --profile was given, and {paths.project_dir / 'dbt_project.yml'} names no profile"
        raise ProjectError(detail) from exc


def _os_user() -> str:
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"


def _print(request: DropRequest, result: DropResult) -> None:
    if result.dropped:
        click.echo(f"dropped {request.object_type} {request.qualified_name.sql}")
        for key in result.forgotten:
            click.echo(f"forgot {key} in state")
    else:
        click.echo(f"{result.outcome}: {request.object_type} {request.qualified_name.sql} was not dropped")
