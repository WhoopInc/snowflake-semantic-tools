"""The `sst` root group: usage errors exit 3, interrupts exit 130, and JSON mode stays JSON.

`SstGroup` reports a usage error as one JSON envelope when the command line asks for
`--output json`, and as text otherwise; either way it exits 3 (USAGE) and carries a diagnostic,
SST-PRT100 unless the refusal has a code of its own. A value read from an environment variable
that does not parse is a configuration error instead, exit 4 (CONFIG). The root `cli` group
declares every global option and passes the ones the user set on to the command that follows,
as that command's defaults. `cli.commands` registers every command on `cli`.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from typing import IO, Any

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.cli.exit_codes import CONFIG, INTERRUPTED, OK, USAGE
from snowflake_semantic_tools.cli.globals import forwarded, group_global_options
from snowflake_semantic_tools.cli.output import json_envelope, print_envelope, start_invocation
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, render_diagnostic


class SstUsageError(click.UsageError):
    """A command line `sst` refuses; it exits 3 like every usage error, and carries its diagnostic.

    Attributes:
        diagnostic: The refusal's diagnostic; SST-PRT100 with the message when none is given.

    Diagnostics:
        SST-PRT100: a flag, value, or combination is not accepted.
    """

    exit_code = USAGE

    def __init__(self, message: str, ctx: click.Context | None = None, *, diagnostic: Diagnostic | None = None) -> None:
        super().__init__(message, ctx)
        self.diagnostic = diagnostic or D("SST-PRT100", subject="cli", detail=message)

    def show(self, file: IO[Any] | None = None) -> None:
        """Print the usage line, then the refusal as a diagnostic, then any further explanation, on stderr."""
        if self.ctx is not None:
            click.echo(self.ctx.get_usage(), file=file, err=True)
        click.echo(render_diagnostic(self.diagnostic), file=file, err=True)
        if self.message != self.diagnostic.message:
            click.echo(f"  note: {self.message}", file=file, err=True)


class SstConfigError(click.ClickException):
    """An environment variable whose value does not parse; it exits 4, before anything runs.

    Diagnostics:
        SST-CFG004: the variable's value is not of the option's type.
    """

    exit_code = CONFIG

    def __init__(self, diagnostic: Diagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic

    def show(self, file: IO[Any] | None = None) -> None:
        """Print the refusal as a diagnostic, on stderr."""
        click.echo(render_diagnostic(self.diagnostic), file=file, err=True)


def usage_refusal(exc: click.UsageError) -> SstUsageError | SstConfigError:
    """Return the refusal `sst` reports for a click usage error.

    A bad value that came from an environment variable is a configuration error, exit 4: the
    command line was fine, and nothing ran. Every other usage error keeps exit 3.
    """
    if isinstance(exc, SstUsageError):
        return exc
    param = exc.param if isinstance(exc, click.BadParameter) else None
    ctx = exc.ctx
    from_environment = (
        param is not None
        and param.name is not None
        and ctx is not None
        and ctx.get_parameter_source(param.name) is click.core.ParameterSource.ENVIRONMENT
    )
    if from_environment and param is not None:
        envvar = param.envvar if isinstance(param.envvar, str) else str(param.name)
        return SstConfigError(
            D(
                "SST-CFG004",
                subject=f"config:{envvar}",
                key=envvar,
                expected=param.type.name,
                found=repr(os.environ.get(envvar, "")),
            )
        )
    return SstUsageError(exc.format_message(), ctx)


# `sst --help` lists commands in workflow order: setup, authoring, offline checks, publishing,
# verification, housekeeping, and break-glass last. A command missing here sorts after them.
COMMAND_ORDER = (
    "init",
    "debug",
    "enrich",
    "format",
    "compile",
    "validate",
    "baseline",
    "list",
    "plan",
    "apply",
    "diff",
    "test",
    "explain",
    "docs",
    "clean",
    "migrate",
    "drop",
)


class SstGroup(click.Group):
    """The root command group, which owns how a run starts, fails to parse, and is interrupted."""

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Return the commands in workflow order, then any `COMMAND_ORDER` does not name, by name."""
        rank = {name: index for index, name in enumerate(COMMAND_ORDER)}
        return sorted(self.commands, key=lambda name: (rank.get(name, len(rank)), name))

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Parse the group's own options, raising any usage error as `sst` reports it."""
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            raise usage_refusal(exc) from exc

    def invoke(self, ctx: click.Context) -> Any:
        """Run the command; an interrupt outside a guarded action still exits 130 rather than click's 1."""
        try:
            return super().invoke(ctx)
        except (SstUsageError, SstConfigError):
            raise
        except click.UsageError as exc:
            raise usage_refusal(exc) from exc
        except (click.exceptions.Abort, KeyboardInterrupt, EOFError) as exc:
            click.echo("Aborted.", err=True)
            raise click.exceptions.Exit(INTERRUPTED) from exc

    def main(
        self,
        args: Sequence[str] | None = None,
        prog_name: str | None = None,
        complete_var: str | None = None,
        standalone_mode: bool = True,
        **extra: Any,
    ) -> Any:
        """Run `sst` once: record the invocation, then report a refusal as JSON when asked to.

        With `--output json` anywhere on the command line, click runs in non-standalone mode,
        so a refusal reaches this method and becomes one envelope with its exit code, and a run
        in standalone mode still ends the process with the command's exit code.
        """
        arguments = list(args if args is not None else sys.argv[1:])
        start_invocation([prog_name or "sst", *arguments])
        json_requested = _json_requested(arguments)
        try:
            result = super().main(
                args=arguments,
                prog_name=prog_name,
                complete_var=complete_var,
                standalone_mode=False if json_requested else standalone_mode,
                **extra,
            )
        except (click.UsageError, SstConfigError) as exc:
            if not json_requested:
                raise
            refusal = usage_refusal(exc) if isinstance(exc, click.UsageError) else exc
            command = next((value for value in arguments if value in self.commands), "")
            print_envelope(
                json_envelope(
                    command,
                    DiagnosticBag((refusal.diagnostic,)),
                    exit_code=refusal.exit_code,
                    status="error",
                    data={"error": refusal.message},
                )
            )
            if standalone_mode:
                raise SystemExit(refusal.exit_code) from exc
            return refusal.exit_code
        if json_requested and standalone_mode:
            raise SystemExit(result if isinstance(result, int) else OK)
        return result


def _json_requested(arguments: list[str]) -> bool:
    """Return whether the command line asks for `--output json`, in any spelling."""
    return any(
        value in ("--output=json", "-ojson")
        or value in ("--output", "-o")
        and index + 1 < len(arguments)
        and arguments[index + 1] == "json"
        for index, value in enumerate(arguments)
    )


def forward_globals(ctx: click.Context, group: click.Group, values: dict[str, Any]) -> None:
    """Give each subcommand of `group` the global values set before it as its defaults."""
    defaults = dict(ctx.default_map or {})
    for command_name in group.commands:
        command_defaults = dict(defaults.get(command_name, {}))
        command_defaults.update(values)
        defaults[command_name] = command_defaults
    ctx.default_map = defaults


@click.group(
    cls=SstGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(version=VERSION, prog_name="sst")
@group_global_options()
@click.pass_context
def cli(ctx: click.Context, /, **params: Any) -> None:
    """Snowflake Semantic Tools 1.0."""
    forward_globals(ctx, cli, forwarded(ctx, params))


# click's usage errors exit 2, which `sst` reserves for a plan with changes pending.
for _click_error in (
    click.UsageError,
    click.BadParameter,
    click.NoSuchOption,
    click.MissingParameter,
):
    _click_error.exit_code = USAGE
