"""Run each command body under one exception guard, and report what it returns in one place.

A command's click callback is its body decorated with `command_body(name)`, the innermost
decorator, under the click options. `command_body` adds every global option to the command,
resolves them once into `GlobalOptions`, finds the configuration file, reads the baseline, and
passes the body what its signature asks for by name: `paths` (the resolved `ProjectPaths`),
`options`, `project_dir`, and `manifest_path`. The body does the command's work and returns a
`CommandResult`; the runner marks what the baseline holds, prints one JSON envelope or human
text, then exits with its code. Every exception becomes an exit code the same way for every
command:

- `click.exceptions.Exit` and `click.UsageError` pass through, to click and `SstGroup`;
- an interrupt, end of input, or a declined prompt exits 130 (INTERRUPTED);
- `SnowflakePortError` reports its diagnostic and exits 5 (CONNECTION). One without a diagnostic is
  classified by the signature table: a statement Snowflake rejected -- an object that does not
  exist, a compilation error -- exits 1 (ERROR) under its SNO code; a refused privilege or a
  missed deadline exits 5 under its SNO code; a failed login, a dropped connection, or an error
  no signature recognises is SST-PRT001 and exits 5;
- `WriteFailure` exits 1 (ERROR) with SST-PRT008: a file SST writes could not be written;
- `ProjectError`, `ValueError`, `OSError`, and `JSONDecodeError` exit 4 (CONFIG);
- anything else is SST-INT001, an unhandled internal error, and exits 1 (ERROR). This is the
  one place an exception nothing expects is caught.

Every reported bag is `audit`ed first, so a diagnostic that breaks an emission invariant is
reported beside it as an internal error, and a run that would have succeeded exits 1.
"""

from __future__ import annotations

import csv
import dataclasses
import functools
import inspect
import io
import json
import logging
import sys
from collections.abc import Callable, Collection
from enum import Enum
from pathlib import Path
from typing import Any, NoReturn

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, read_baseline
from snowflake_semantic_tools.adapters.locations import ProjectPaths, locate_project
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.yaml.dump import dump_yaml
from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, CONNECTION, ERROR, OK
from snowflake_semantic_tools.cli.globals import DEFAULT_OUTPUTS, GLOBAL_NAMES, GlobalOptions, command_global_options
from snowflake_semantic_tools.cli.output import (
    RenderPolicy,
    captured,
    emit_json,
    interrupted,
    json_envelope,
    print_report,
    render_diagnostics,
    resolve_invocation,
    use_render_policy,
)
from snowflake_semantic_tools.cli.policy import ran_under_1_0, with_baseline, with_policy
from snowflake_semantic_tools.cli.run_log import append_run_log
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, audit
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline
from snowflake_semantic_tools.domain.diagnostics.signatures import UNRECOGNISED, match_signature, snowflake_diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import ErrorKind
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

_LOG_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}


class ConfigNeed(Enum):
    """Whether a command needs a configuration file: `init`, `debug`, and `docs` run without one.

    NONE never looks for one, nor for a baseline: `explain` and `drop` must work when the project
    itself is what is broken.
    """

    REQUIRED = "required"
    OPTIONAL = "optional"
    NONE = "none"


@dataclasses.dataclass(frozen=True)
class CommandResult:
    """What a command reports: its exit code, diagnostics, JSON data, and human report.

    Attributes:
        exit_code: The process's exit code, which the envelope reports as well.
        diagnostics: Listed in the envelope; human output renders them on stderr before
            `human` runs, unless `show_diagnostics` is false.
        data: The envelope's `data`, `{}` when None; a callable builds it as the envelope is
            built, after the baseline is matched.
        human: Prints the human report; it is not called for `--output json`.
        promoted: The envelope's `summary.promoted`: the warnings `--strict` made errors.
        show_diagnostics: False where human output leaves the diagnostics to `--output json`.
        gated: True when the exit code is 1 only because a diagnostic is an error, so a run
            whose every error is a baselined, promoted warning exits 0 instead.
        rows: The flat rows `--output csv` prints, one per item; None for a command without them.
    """

    exit_code: int = OK
    diagnostics: DiagnosticBag = DiagnosticBag()
    data: object | None = None
    human: Callable[[], None] | None = None
    promoted: int = 0
    show_diagnostics: bool = True
    gated: bool = False
    rows: list[dict[str, object]] | None = None


def command_body(
    name: str,
    *,
    outputs: Collection[str] = DEFAULT_OUTPUTS,
    config: ConfigNeed = ConfigNeed.REQUIRED,
    refusals: Callable[..., None] | None = None,
    applies_baseline: bool = True,
) -> Callable[[Callable[..., CommandResult]], Callable[..., None]]:
    """Make the click callback for the body of `sst <name>`, which returns a `CommandResult`.

    The callback keeps the body's name and docstring, which click takes as the command's name
    and help, and declares every global option on the command. It runs the body and then its
    report under `guarded`, so an exception raised while reporting -- writing `--emit-ddl`
    files, say -- fails the command the same way as one raised by the body.

    Args:
        outputs: The `--output` values the command supports; any other is SST-PRT106.
        config: Whether the command refuses to run without a configuration file.
        refusals: Checks the command line alone, before anything is resolved, so a usage error
            exits 3 even in a directory that is not a project. It takes the parameters it names,
            as the body does.
        applies_baseline: False for a command that reads and writes the baseline itself, as
            `sst baseline` does, so the runner neither reads it first nor marks what it holds.
    """

    def decorate(body: Callable[..., CommandResult]) -> Callable[..., None]:
        wanted = frozenset(inspect.signature(body).parameters)

        @functools.wraps(body)
        def callback(**params: Any) -> None:
            options = GlobalOptions.from_params(params)
            given = {key: value for key, value in params.items() if key not in GLOBAL_NAMES}
            run = _Run(name, body, wanted, options, given, frozenset(outputs), config, refusals, applies_baseline)
            guarded(run.execute, command=name, output=options.output)

        declared = command_global_options()(callback)
        return declared if "selected" in wanted else _no_selector_options(name)(declared)

    return decorate


def _no_selector_options(command: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Declare `--select` and `--exclude`, hidden, on a command that takes no selector, refusing both.

    A selector there is refused rather than unknown, so the refusal can say what to do instead.

    Diagnostics:
        SST-PRT110: a selector was passed to a command that takes none; raised.
    """

    def refuse(ctx: click.Context, param: click.Parameter, value: tuple[str, ...]) -> None:
        if value:
            from snowflake_semantic_tools.cli.group import SstUsageError

            diagnostic = D("SST-PRT110", subject="cli", command=f"sst {command}", value=value[0])
            raise SstUsageError(diagnostic.message, ctx, diagnostic=diagnostic)

    def apply(function: Callable[..., None]) -> Callable[..., None]:
        for flag in ("--exclude", "--select"):
            function = click.option(
                flag, f"refused_{flag[2:]}", multiple=True, hidden=True, expose_value=False, callback=refuse
            )(function)
        return function

    return apply


@dataclasses.dataclass(frozen=True)
class _Run:
    """One invocation of a command body, from its resolved global options to its report."""

    name: str
    body: Callable[..., CommandResult]
    wanted: frozenset[str]
    options: GlobalOptions
    given: dict[str, Any]
    outputs: frozenset[str]
    config: ConfigNeed
    refusals: Callable[..., None] | None
    applies_baseline: bool = True

    def execute(self) -> None:
        """Check the command line, resolve the project, run the body, and report it."""
        options = self.options
        if options.output not in self.outputs:
            _refuse_output(self.name, options.output, self.outputs)
        if options.verbose and options.quiet:
            _refuse_pair("--verbose", "--quiet")
        if self.refusals is not None:
            names = frozenset(inspect.signature(self.refusals).parameters)
            supplied = {"options": options, "project_dir": options.project_dir}
            self.refusals(**{key: value for key, value in {**self.given, **supplied}.items() if key in names})
        logging.basicConfig(level=_LOG_LEVELS[options.log_level], stream=sys.stderr)
        use_render_policy(_render_policy(options))
        files = self._files()
        resolve_invocation(
            project_dir=options.project_dir,
            config_file=files.config_file,
            target=self.given.get("target_name"),
            overrides=options.overrides,
        )
        reads = self.applies_baseline and self.config is not ConfigNeed.NONE
        baseline = run_baseline(options) if reads else None
        # Read before the body, which may write the manifest this asks about.
        first_run = not ran_under_1_0(files.project_dir)
        result = self.body(**self._arguments(files))
        diagnostics, exit_code = with_policy(
            self.name,
            result.diagnostics,
            result.exit_code,
            gated=result.gated,
            promoted=result.promoted,
            paths=files,
            baselined=baseline is not None,
            strict=self.given.get("strict"),
            first_run=first_run,
        )
        result = dataclasses.replace(result, diagnostics=diagnostics, exit_code=exit_code)
        _report(self.name, options, _with_baseline(result, baseline, files))

    def _files(self) -> ProjectPaths:
        """Resolve the project's files, the target its configuration resolves against, and its deferral.

        A command that needs no configuration looks for none.
        """
        options = self.options
        run: dict[str, Any] = {
            "target_name": self.given.get("target_name"),
            "defer_target": self.given.get("defer_target"),
            "defer_disabled": bool(self.given.get("no_defer")),
        }
        if self.config is ConfigNeed.NONE:
            return ProjectPaths(options.project_dir, None, profiles_dir=options.profiles_dir, **run)
        return dataclasses.replace(
            locate_project(
                options.project_dir,
                options.config,
                profiles_dir=options.profiles_dir,
                required=self.config is ConfigNeed.REQUIRED,
            ),
            allow_unsupported_manifest_schema=options.allow_unsupported_manifest_schema,
            **run,
        )

    def _arguments(self, files: ProjectPaths) -> dict[str, Any]:
        """Return the body's arguments: its own parameters, and the resolved values it names."""
        supplied = {
            "paths": files,
            "options": self.options,
            "project_dir": self.options.project_dir,
            "manifest_path": self.options.manifest,
        }
        arguments = {key: value for key, value in self.given.items() if key in self.wanted}
        arguments.update({key: value for key, value in supplied.items() if key in self.wanted})
        return arguments


def _render_policy(options: GlobalOptions) -> RenderPolicy:
    return RenderPolicy(
        output=options.output,
        no_color=options.no_color,
        verbose=options.verbose,
        quiet=options.quiet,
        show_info=options.show_info,
        show_baselined=options.show_baselined,
        show_cascade=options.show_cascade,
        show_all_occurrences=options.show_all_occurrences,
    )


def _refuse_output(command: str, found: str, supported: frozenset[str]) -> NoReturn:
    """Refuse an `--output` value the command does not support; there is no fallback format.

    Diagnostics:
        SST-PRT106: the command does not support the `--output` value; raised.
    """
    from snowflake_semantic_tools.cli.group import SstUsageError

    expected = ", ".join(value for value in ("table", "plain", "json", "yaml", "csv") if value in supported)
    diagnostic = D("SST-PRT106", subject="cli", found=found, command=f"sst {command}", expected=expected)
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


def _refuse_pair(first: str, second: str) -> NoReturn:
    """Refuse two flags that exclude each other.

    Diagnostics:
        SST-PRT104: both flags were given; raised.
    """
    from snowflake_semantic_tools.cli.group import SstUsageError

    diagnostic = D("SST-PRT104", subject="cli", a=first, b=second)
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)


def run_baseline(options: GlobalOptions) -> Baseline | None:
    """Read the run's baseline: `--baseline`, else `.sst/baseline.json` when it exists; None without one.

    Raises:
        ProjectError: the file `--baseline` names does not exist, or a baseline cannot be read.
    """
    if options.no_baseline:
        return None
    path = options.baseline or options.project_dir / BASELINE_FILE
    if options.baseline is None and not path.is_file():
        return None
    name = path.as_posix() if options.baseline is not None else BASELINE_FILE.as_posix()
    return read_baseline(path, name)


def _with_baseline(result: CommandResult, baseline: Baseline | None, files: ProjectPaths) -> CommandResult:
    """Mark what the baseline holds, add its expiry notices, and settle the exit code they decide.

    `app.baseline` decides, as of the system clock's today: a baselined diagnostic never blocks,
    and a baseline past its expiry is an error.
    """
    if baseline is None:
        return result
    diagnostics, exit_code, baselined = with_baseline(
        result.diagnostics, result.exit_code, baseline, gated=result.gated, clock=SystemClock()
    )
    resolve_invocation(
        project_dir=files.project_dir,
        config_file=files.config_file,
        target=None,
        baselined=baselined,
    )
    return dataclasses.replace(result, diagnostics=diagnostics, exit_code=exit_code)


def _report(command: str, options: GlobalOptions, result: CommandResult) -> None:
    """Print `result` as one envelope, YAML, CSV, or human text, and exit with its code when it is not 0.

    YAML is the envelope serialized as YAML; CSV is the result's rows with a header, and its
    diagnostics go to stderr. `plain` is `table` without colour. Output of any form that would
    carry a credential the run resolved is withheld for SST-PRT012, and the run exits 1.
    """
    audited = audit(result.diagnostics)
    if audited is not result.diagnostics:
        exit_code = ERROR if result.exit_code in (OK, CHANGES) else result.exit_code
        result = dataclasses.replace(result, diagnostics=audited, exit_code=exit_code)
    append_run_log(options.project_dir, target_dir(options.project_dir), result.diagnostics, command=command)
    if options.output in ("json", "yaml"):
        envelope = json_envelope(
            command, result.diagnostics, exit_code=result.exit_code, promoted=result.promoted, data=result.data
        )
        if options.output == "json":
            emit_json(envelope, result.exit_code)
        _exit(result.exit_code, withheld=print_report(dump_yaml(envelope), "YAML report"))
    if options.output == "csv":
        render_diagnostics(result.diagnostics)
        _exit(result.exit_code, withheld=print_report(_csv(result.rows or []), "CSV report"))
    if result.show_diagnostics:
        render_diagnostics(result.diagnostics)
    withheld = result.human is not None and print_report(captured(result.human), "human report")
    if result.exit_code or withheld:
        _exit(result.exit_code, withheld=withheld)


def _exit(exit_code: int, *, withheld: bool) -> NoReturn:
    """End the run with `exit_code`, or with 1 when its output was withheld for carrying a credential."""
    raise click.exceptions.Exit(ERROR if withheld else exit_code)


def _csv(rows: list[dict[str, object]]) -> str:
    """Return `rows` as CSV with a header row; a list or mapping value is written as JSON."""
    columns = list(dict.fromkeys(key for row in rows for key in row))
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(
            [
                json.dumps(row.get(key)) if isinstance(row.get(key), (list, dict)) else row.get(key, "")
                for key in columns
            ]
        )
    return buffer.getvalue()


def guarded(action: Callable[[], None], *, command: str, output: str) -> None:
    """Run `action`, ending the run with the exit code the module docstring gives each exception."""
    try:
        action()
    except click.exceptions.Exit:
        raise
    except click.UsageError:
        raise
    except (click.exceptions.Abort, KeyboardInterrupt, EOFError) as exc:
        interrupted(command, output, exc)
    except SnowflakePortError as exc:
        _snowflake_failed(command, output, exc)
    except WriteFailure as exc:
        _write_failed(command, output, exc)
    except (ProjectError, ValueError, OSError, json.JSONDecodeError) as exc:
        _unusable(command, output, exc)
    except Exception as exc:  # the one crash handler: an exception nothing above expects
        _internal_error(command, output, exc)


def _snowflake_failed(command: str, output: str, exc: SnowflakePortError) -> NoReturn:
    """Exit with the code `snowflake_failure` gives a port error, reporting its diagnostic."""
    diagnostic, exit_code = snowflake_failure(exc)
    diagnostics = DiagnosticBag((diagnostic,))
    if output == "json":
        emit_json(
            json_envelope(command, diagnostics, exit_code=exit_code, status="error", data={"error": str(exc)}),
            exit_code,
        )
    render_diagnostics(diagnostics)
    raise click.exceptions.Exit(exit_code) from exc


# The signatures of a session Snowflake would not open or keep: a rejected login, a dropped
# connection. A command reports them as the connection failing.
_CONNECTION_SIGNATURES = frozenset((UNRECOGNISED.code, "SST-SNO013", "SST-SNO014"))


def snowflake_failure(exc: SnowflakePortError) -> tuple[Diagnostic, int]:
    """Return what a command reports for a port error, and the code it exits with.

    The error's own diagnostic exits 5. Otherwise the signature table classifies its message,
    errno and SQLSTATE: a failed login, a dropped connection, or an unrecognised error is the
    connection failing; a refused privilege or a missed deadline is the warehouse not
    cooperating, exit 5; any other signature -- an object that does not exist, a compilation
    error -- is a statement Snowflake rejected, exit 1.

    Diagnostics:
        SST-PRT001: the connection failed, or no signature recognises the error.
        Any SNO code of the signature table, as `snowflake_diagnostic` reports it.
    """
    if exc.diagnostic is not None:
        return exc.diagnostic, CONNECTION
    message = str(exc)
    signature = match_signature(message, errno=exc.errno, sqlstate=exc.sqlstate)
    if signature.code in _CONNECTION_SIGNATURES:
        return D("SST-PRT001", subject="cli", value="Snowflake", detail=message), CONNECTION
    diagnostic = snowflake_diagnostic(signature.code, message, value="the object", subject="cli")
    uncooperative = signature.kind in {ErrorKind.PRIVILEGE, ErrorKind.TRANSIENT} or signature.code == "SST-SNO011"
    return diagnostic, CONNECTION if uncooperative else ERROR


class WriteFailure(Exception):
    """A file a command writes -- a manifest, rendered DDL, a page -- could not be written."""

    def __init__(self, path: Path, cause: OSError) -> None:
        super().__init__(f"could not write {path}: {cause}")
        self.path = path
        self.cause = cause


def write_text(root: Path, path: Path, text: str) -> None:
    """Write `text` to `path` inside `root`, creating its folder; a failure is a `WriteFailure`, exit 1.

    `root` is the project, or the output folder the user chose (`adapters.paths.output_root`);
    a path outside it, or one a symbolic link is on the way to, is refused, not written.

    Raises:
        WriteFailure: the folder or the file cannot be written, or the write is refused.
    """
    try:
        write_within(root, path, text)
    except OSError as exc:
        raise WriteFailure(path, exc) from exc


def _write_failed(command: str, output: str, exc: WriteFailure) -> NoReturn:
    """Exit 1 because a file the command writes could not be written.

    Diagnostics:
        SST-PRT008: the write failed.
    """
    diagnostic = D("SST-PRT008", subject="cli", path=str(exc.path), detail=str(exc.cause))
    diagnostics = DiagnosticBag((diagnostic,))
    if output == "json":
        emit_json(json_envelope(command, diagnostics, exit_code=ERROR, status="error", data={"error": str(exc)}), ERROR)
    render_diagnostics(diagnostics)
    raise click.exceptions.Exit(ERROR) from exc


def _unusable(command: str, output: str, exc: Exception) -> NoReturn:
    """Exit 4 because the project, its configuration, or a saved plan cannot be used."""
    diagnostics = audit(DiagnosticBag(getattr(exc, "diagnostics", ())))
    if output == "json":
        emit_json(
            json_envelope(command, diagnostics, exit_code=CONFIG, status="error", data={"error": str(exc)}), CONFIG
        )
    render_diagnostics(diagnostics)
    if not diagnostics:
        click.echo(f"error: {exc}", err=True)
    raise click.exceptions.Exit(CONFIG) from exc


def _internal_error(command: str, output: str, exc: Exception) -> NoReturn:
    """Exit 1 with SST-INT001: an exception nothing above expects means SST itself is broken.

    Diagnostics:
        SST-INT001: an exception no handler expects reached the command's guard.
    """
    detail = f"{type(exc).__name__}: {exc}"
    diagnostics = DiagnosticBag((D("SST-INT001", subject=command, detail=detail),))
    if output == "json":
        emit_json(json_envelope(command, diagnostics, exit_code=ERROR, status="error", data={"error": str(exc)}), ERROR)
    render_diagnostics(diagnostics)
    raise click.exceptions.Exit(ERROR) from exc


def project_path(files: ProjectPaths, path: Path) -> Path:
    """Return `path` taken from the project directory when it is relative."""
    return path if path.is_absolute() else files.project_dir / path
