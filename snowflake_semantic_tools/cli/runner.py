"""Run each command body under one exception guard, and report what it returns in one place.

A command's click callback is its body decorated with `command_body(name)`, the innermost
decorator, under the click options. The body does the command's work and returns a
`CommandResult`; the runner prints that as one JSON envelope or as human text, then exits
with its code. Every exception the body or its report raises becomes an exit code the
same way for every command:

- `click.exceptions.Exit` and `click.UsageError` pass through, to click and `SstGroup`;
- an interrupt, end of input, or a declined prompt exits 130 (INTERRUPTED);
- `SnowflakePortError` exits 5 (CONNECTION); in human output one that carries no
  diagnostic is reported by click instead, which exits 1;
- `ProjectError`, `ValueError`, `OSError`, and `JSONDecodeError` exit 4 (CONFIG);
- anything else is SST-INT902, an internal error, and exits 1 (ERROR).
"""

from __future__ import annotations

import dataclasses
import functools
import json
from collections.abc import Callable
from typing import Any, NoReturn

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, ERROR, OK
from snowflake_semantic_tools.cli.output import emit_json, interrupted, json_envelope, render_diagnostics
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError


@dataclasses.dataclass(frozen=True)
class CommandResult:
    """What a command reports: its exit code, diagnostics, JSON data, and human report.

    Attributes:
        exit_code: The process's exit code, which the envelope reports as well.
        diagnostics: Listed in the envelope; human output renders them on stderr before
            `human` runs, unless `show_diagnostics` is false.
        data: The envelope's `data`, `{}` when None.
        human: Prints the human report; it is not called for `--output json`.
        promoted: The envelope's `summary.promoted`: the warnings `--strict` made errors.
        show_diagnostics: False where human output leaves the diagnostics to `--output json`.
    """

    exit_code: int = OK
    diagnostics: DiagnosticBag = DiagnosticBag()
    data: object | None = None
    human: Callable[[], None] | None = None
    promoted: int = 0
    show_diagnostics: bool = True


def command_body(name: str) -> Callable[[Callable[..., CommandResult]], Callable[..., None]]:
    """Make the click callback for the body of `sst <name>`, which returns a `CommandResult`.

    The callback keeps the body's name and docstring, which click takes as the command's
    name and help, and passes it every parameter, `output` included. It runs the body and
    then its report under `guarded`, so an exception raised while reporting -- writing
    `--emit-ddl` files, say -- fails the command the same way as one raised by the body.
    """

    def decorate(body: Callable[..., CommandResult]) -> Callable[..., None]:
        @functools.wraps(body)
        def callback(**params: Any) -> None:
            output = params["output"]
            guarded(lambda: _report(name, output, body(**params)), command=name, output=output)

        return callback

    return decorate


def _report(command: str, output: str, result: CommandResult) -> None:
    """Print `result` as one envelope or as human text, and exit with its code when it is not 0."""
    if output == "json":
        envelope = json_envelope(
            command, result.diagnostics, exit_code=result.exit_code, promoted=result.promoted, data=result.data
        )
        emit_json(envelope, result.exit_code)
    if result.show_diagnostics:
        render_diagnostics(result.diagnostics)
    if result.human is not None:
        result.human()
    if result.exit_code:
        raise click.exceptions.Exit(result.exit_code)


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
        _connection_failed(command, output, exc)
    except (ProjectError, ValueError, OSError, json.JSONDecodeError) as exc:
        _unusable(command, output, exc)
    except Exception as exc:
        _internal_error(command, output, exc)


def _connection_failed(command: str, output: str, exc: SnowflakePortError) -> NoReturn:
    """Exit 5 because Snowflake could not be reached; without a diagnostic, click reports it."""
    diagnostics = DiagnosticBag((exc.diagnostic,) if exc.diagnostic is not None else ())
    if output == "json":
        emit_json(
            json_envelope(command, diagnostics, exit_code=CONNECTION, status="error", data={"error": str(exc)}),
            CONNECTION,
        )
    if diagnostics:
        render_diagnostics(diagnostics)
        raise click.exceptions.Exit(CONNECTION) from exc
    raise click.ClickException(str(exc)) from exc


def _unusable(command: str, output: str, exc: Exception) -> NoReturn:
    """Exit 4 because the project, its configuration, or a saved plan cannot be used."""
    diagnostics = DiagnosticBag(getattr(exc, "diagnostics", ()))
    if output == "json":
        emit_json(
            json_envelope(command, diagnostics, exit_code=CONFIG, status="error", data={"error": str(exc)}), CONFIG
        )
    render_diagnostics(diagnostics)
    if not diagnostics:
        click.echo(f"error: {exc}", err=True)
    raise click.exceptions.Exit(CONFIG) from exc


def _internal_error(command: str, output: str, exc: Exception) -> NoReturn:
    """Exit 1 with SST-INT902: an exception nothing above expects means SST broke an invariant."""
    diagnostics = DiagnosticBag((D("SST-INT902", subject=command, detail=str(exc)),))
    if output == "json":
        emit_json(json_envelope(command, diagnostics, exit_code=ERROR, status="error", data={"error": str(exc)}), ERROR)
    render_diagnostics(diagnostics)
    raise click.exceptions.Exit(ERROR) from exc
