"""`sst baseline`: write and maintain the baseline file, the record of warnings a project knows about.

`add` records every current instance of one code, or with `--all-warnings` of every warning,
and never removes an entry. `prune` removes only the entries no current diagnostic matches.
`show` lists the entries. `renew` re-dates the file and records why. The current diagnostics are
those an offline `sst validate` reports; with `--target`, those the connected `sst validate
--target` reports, and each entry `add` writes then records that target. An offline run cannot
see what only a connected one finds, so an offline `prune` keeps every connected entry, and
`prune --target` judges that target's connected entries as well. An error, or any non-demotable
code, cannot be baselined: a baseline suppresses and never demotes. The file is the global
`--baseline`, else `.sst/baseline.json`.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, baseline_text, read_baseline
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.app.baseline import clock_stamp, clock_today, days_after
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.cli.exit_codes import ERROR
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import Decorator, selection_options
from snowflake_semantic_tools.cli.runner import CommandResult, command_body, project_path, write_text
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import (
    DEFAULT_EXPIRY_DAYS,
    MAX_EXPIRY_DAYS,
    Baseline,
    BaselineEntry,
    baseline_refusal,
    chosen_for_baseline,
    renewed,
    with_entries,
    without_stale,
)

_SUBCOMMANDS = ("add", "prune", "show", "renew")


def _connected_target() -> Decorator:
    """`--target`: run the connected validate against it.

    Never read from `$SST_TARGET`, so a baseline command stays offline unless asked by name.
    """
    return click.option("--target", "-t", "target_name")


@click.group()
@click.pass_context
def baseline(ctx: click.Context) -> None:
    """Write and maintain the baseline file of known, not yet fixed, diagnostics."""
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in _SUBCOMMANDS}


@baseline.command(cls=SstCommand, name="add")
@click.argument("code", metavar="[CODE]", required=False)
@_connected_target()
@selection_options()
@click.option("--all-warnings", is_flag=True)
@click.option("--expires-in", type=click.IntRange(1, MAX_EXPIRY_DAYS), default=DEFAULT_EXPIRY_DAYS)
@click.option("--note")
@click.option("--yes", "-y", "assume_yes", is_flag=True)
@command_body("baseline add", applies_baseline=False)
def add_command(
    paths: ProjectPaths,
    options: GlobalOptions,
    manifest_path: Path | None,
    code: str | None,
    target_name: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    all_warnings: bool,
    expires_in: int,
    note: str | None,
    assume_yes: bool,
) -> CommandResult:
    """Baseline every current instance of CODE, or with --all-warnings every current warning.

    Additive: an entry is never removed. A new file expires in --expires-in days; an existing
    one keeps its date, which only `renew` moves. With --target, what the connected validate
    against it reports is baselined, and each entry added records the target. Exit 1 when CODE
    is an error or non-demotable, and 3 when CODE is not registered, or --all-warnings has no
    --yes off a terminal.

    Diagnostics:
        SST-PRT100: no CODE and no --all-warnings, or CODE is not registered; raised. Also, at
            exit 1, CODE cannot be baselined.
        SST-PRT104: CODE and --all-warnings were both given; raised.
        SST-PRT109: --all-warnings off a terminal without --yes; raised.
    """
    wanted = _wanted_code(code, all_warnings)
    if wanted is not None and (refusal := baseline_refusal(wanted)) is not None:
        return CommandResult(ERROR, DiagnosticBag((D("SST-PRT100", subject="cli", detail=refusal),)))
    path, current = _read(paths, options)
    today = _today()
    start = current or Baseline(_name(options), _date(today, expires_in), (), _now(), VERSION)
    diagnostics = _current(paths, manifest_path, selected, excluded, target_name)
    chosen = chosen_for_baseline(diagnostics, wanted)
    if all_warnings:
        _confirm(options, len(chosen), assume_yes=assume_yes)
    written, added = with_entries(
        start, chosen, lambda found: note or f"pre-existing at adoption of {found}", target_name or ""
    )
    write_text(output_root(paths.project_dir, path.parent), path, baseline_text(written))
    data = _data(path, written, added=added)
    return CommandResult(data=data, human=lambda: click.echo(f"baselined {len(added)} diagnostic(s) in {path}"))


@baseline.command(cls=SstCommand, name="prune")
@_connected_target()
@selection_options()
@command_body("baseline prune", applies_baseline=False)
def prune_command(
    paths: ProjectPaths,
    options: GlobalOptions,
    manifest_path: Path | None,
    target_name: str | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
) -> CommandResult:
    """Remove the entries no current diagnostic matches; the only way an entry leaves the file.

    The offline entries are judged by an offline validate. A connected entry is judged only by
    `--target` naming its target, which runs the connected validate too; every other is kept.
    With --select or --exclude, only the entries of the artifacts chosen are considered.

    Diagnostics:
        SST-PRT009: there is no baseline file to prune.
    """
    path, current = _read(paths, options)
    if current is None:
        return _absent(path)
    in_scope = _scope(paths, manifest_path, selected, excluded)
    written = current
    pruned: tuple[BaselineEntry, ...] = ()
    for target in dict.fromkeys((None, target_name)):
        diagnostics = _current(paths, manifest_path, selected, excluded, target)
        judged = _judged_by(in_scope, target)
        written, dropped = without_stale(written, diagnostics, judged)
        pruned = (*pruned, *dropped)
    write_text(output_root(paths.project_dir, path.parent), path, baseline_text(written))
    data = _data(path, written, pruned=pruned)
    return CommandResult(data=data, human=lambda: click.echo(f"pruned {len(pruned)} entry(ies) from {path}"))


@baseline.command(cls=SstCommand, name="show")
@click.option("--code")
@click.option("--expired", is_flag=True)
@_connected_target()
@command_body("baseline show", applies_baseline=False)
def show_command(
    paths: ProjectPaths, options: GlobalOptions, code: str | None, expired: bool, target_name: str | None
) -> CommandResult:
    """List the baseline's entries: those of one --code, or with --expired only once it has expired.

    Each entry says whether a connected validate found it, and against which target. With
    --target, only the entries a validate against it can match are listed: the offline ones and
    that target's connected ones. Nothing connects.
    """
    path, current = _read(paths, options)
    if current is None:
        data = _data(path, Baseline(_name(options), "", ()))
        return CommandResult(data=data, human=lambda: click.echo(f"no baseline at {path}"))
    lapsed = current.expires_on < _today().isoformat()
    shown = tuple(
        entry
        for entry in current.entries
        if (code is None or entry.code == code.strip().upper())
        and (lapsed or not expired)
        and (target_name is None or entry.judged_by(None) or entry.judged_by(target_name))
    )
    data = _data(path, current, shown=shown)
    return CommandResult(data=data, human=lambda: _print_entries(current, shown, lapsed=lapsed))


def _require_reason(reason: str | None) -> None:
    """Refuse `renew` without `--reason`, before anything is resolved.

    Diagnostics:
        SST-PRT100: --reason was not given, or is blank; raised.
    """
    if not (reason or "").strip():
        diagnostic = D("SST-PRT100", subject="cli", detail="sst baseline renew requires --reason")
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


@baseline.command(cls=SstCommand, name="renew")
@click.option("--reason")
@click.option("--expires-in", type=click.IntRange(min=1), default=DEFAULT_EXPIRY_DAYS)
@command_body("baseline renew", applies_baseline=False, refusals=_require_reason)
def renew_command(paths: ProjectPaths, options: GlobalOptions, reason: str, expires_in: int) -> CommandResult:
    """Re-date the baseline --expires-in days from today, recording --reason as its audit record.

    Exit 1 when asked for more than 365 days, and 3 without --reason.

    Diagnostics:
        SST-PRT100: --expires-in is above 365 days.
        SST-PRT009: there is no baseline file to renew.
    """
    if expires_in > MAX_EXPIRY_DAYS:
        detail = f"--expires-in {expires_in} is more than {MAX_EXPIRY_DAYS} days; a baseline cannot last longer"
        return CommandResult(ERROR, DiagnosticBag((D("SST-PRT100", subject="cli", detail=detail),)))
    path, current = _read(paths, options)
    if current is None:
        return _absent(path)
    today = _today()
    written = renewed(current, today=today.isoformat(), expires_on=_date(today, expires_in), reason=reason)
    write_text(output_root(paths.project_dir, path.parent), path, baseline_text(written))
    data = _data(path, written)
    return CommandResult(data=data, human=lambda: click.echo(f"renewed {path}; expires on {written.expires_on}"))


def _wanted_code(code: str | None, all_warnings: bool) -> str | None:
    """Return the registered code `add` baselines; None for every warning.

    Diagnostics:
        SST-PRT104: CODE and --all-warnings were both given; raised.
        SST-PRT100: neither was given, or CODE is not registered; raised.
    """
    if code is not None and all_warnings:
        diagnostic = D("SST-PRT104", subject="cli", a="CODE", b="--all-warnings")
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
    if code is None:
        if all_warnings:
            return None
        diagnostic = D("SST-PRT100", subject="cli", detail="sst baseline add needs a CODE, or --all-warnings")
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
    wanted = code.strip().upper()
    if wanted not in ERROR_REGISTRY:
        diagnostic = D("SST-PRT100", subject="cli", detail=f"'{code}' is not a registered code")
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
    return wanted


def _confirm(options: GlobalOptions, count: int, *, assume_yes: bool) -> None:
    """Confirm baselining every warning: by `--yes`, else by a prompt on a terminal.

    Diagnostics:
        SST-PRT109: there is no terminal to ask on, or the output is JSON; raised.
    """
    if assume_yes:
        return
    if options.output == "json" or not _interactive():
        diagnostic = D("SST-PRT109", subject="cli", command="sst baseline add --all-warnings")
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
    click.confirm(f"baseline all {count} current warning(s)?", abort=True, err=True)


def _interactive() -> bool:
    return sys.stdin.isatty()


def _read(paths: ProjectPaths, options: GlobalOptions) -> tuple[Path, Baseline | None]:
    """Return the baseline file's path and what it holds; None when there is no file yet."""
    path = project_path(paths, options.baseline or BASELINE_FILE)
    return path, read_baseline(path, _name(options)) if path.is_file() else None


def _name(options: GlobalOptions) -> str:
    return (options.baseline or BASELINE_FILE).as_posix()


def _current(
    paths: ProjectPaths,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    target_name: str | None = None,
) -> tuple[Diagnostic, ...]:
    """Return what `sst validate` of the chosen artifacts reports now.

    Offline; or, with `target_name`, connected to that target as `sst validate --target` runs it.
    """
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if selected or excluded:
        compiled = compiling.selected_result(paths.project_dir, compiled, selected, excluded)
    if target_name is None:
        result = ValidateArtifacts(None, catalog=None, target="", clock=SystemClock()).run(
            compiled, strict=False, connected=False
        )
        return tuple(result.diagnostics)
    profile, port = connect(paths, target_name)
    try:
        result = ValidateArtifacts(port, catalog=port, target=profile.target_name, clock=SystemClock()).run(
            compiled, strict=False, connected=True
        )
    finally:
        port.close()
    return tuple(result.diagnostics)


def _judged_by(in_scope: Callable[[BaselineEntry], bool], target: str | None) -> Callable[[BaselineEntry], bool]:
    """Return which entries in scope a validate against `target`, or offline, may prune."""
    return lambda entry: in_scope(entry) and entry.judged_by(target)


def _scope(
    paths: ProjectPaths, manifest_path: Path | None, selected: tuple[str, ...], excluded: tuple[str, ...]
) -> Callable[[BaselineEntry], bool]:
    """Return which entries `prune` considers: all, or those of the artifacts the selectors choose."""
    if not (selected or excluded):
        return lambda entry: True
    compiled = compiling.selected_result(
        paths.project_dir, compiling.compile_result(paths, None, manifest_path), selected, excluded
    )
    keys = {item.artifact_key for item in compiled.compiled}
    files = {path for item in compiled.compiled for path in item.source_files}
    return lambda entry: entry.artifact in keys or entry.file in files


def _absent(path: Path) -> CommandResult:
    diagnostic = D("SST-PRT009", subject="cli", path=str(path), detail="there is no baseline file")
    return CommandResult(ERROR, DiagnosticBag((diagnostic,)))


def _data(
    path: Path,
    baseline_now: Baseline,
    *,
    added: tuple[BaselineEntry, ...] = (),
    pruned: tuple[BaselineEntry, ...] = (),
    shown: tuple[BaselineEntry, ...] | None = None,
) -> dict[str, object]:
    entries = baseline_now.entries if shown is None else shown
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.code] = counts.get(entry.code, 0) + 1
    return {
        "file": str(path),
        "entries": [_entry(entry) for entry in entries],
        "added": [_entry(entry) for entry in added],
        "pruned": [_entry(entry) for entry in pruned],
        "expires_on": baseline_now.expires_on or None,
        "counts": dict(sorted(counts.items())),
    }


def _entry(entry: BaselineEntry) -> dict[str, object]:
    return {
        "fingerprint": entry.fingerprint,
        "code": entry.code,
        "artifact": entry.artifact,
        "file": entry.file,
        "note": entry.note,
        "connected": entry.connected,
        "target": entry.target or None,
    }


def _print_entries(current: Baseline, shown: tuple[BaselineEntry, ...], *, lapsed: bool) -> None:
    state = "expired on" if lapsed else "expires on"
    click.echo(f"{current.path}: {len(current.entries)} entry(ies), {state} {current.expires_on}")
    for entry in shown:
        found = f"connected:{entry.target}" if entry.connected else "offline"
        click.echo(f"  {entry.code} {entry.artifact or '-'} {entry.file or '-'} {entry.fingerprint} {found}")


def _today() -> date:
    return clock_today(SystemClock())


def _now() -> str:
    return clock_stamp(SystemClock())


def _date(today: date, days: int) -> str:
    return days_after(today, days)
