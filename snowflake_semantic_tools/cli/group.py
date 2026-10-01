"""The `sst` root group: usage errors exit 3, interrupts exit 130, and JSON mode stays JSON.

`SstGroup` reports a usage error as one JSON envelope when the command line asks for
`--output json`, and click's text otherwise; either way it exits 3 (USAGE). The root
`cli` group passes its own `--output` and `--project-dir` on to the command that follows
as that command's defaults. `cli.commands` registers every command on `cli`.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.cli.exit_codes import INTERRUPTED, OK, USAGE
from snowflake_semantic_tools.cli.output import json_envelope, print_envelope, start_invocation
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag


class SstUsageError(click.UsageError):
    """A command line `sst` refuses beyond what click checks; it exits 3 like every usage error."""

    exit_code = USAGE


class SstGroup(click.Group):
    """The root command group, which owns how a run starts, fails to parse, and is interrupted."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Parse the group's own options, raising any usage error as `SstUsageError`."""
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            raise SstUsageError(str(exc), ctx) from exc

    def invoke(self, ctx: click.Context) -> Any:
        """Run the command; an interrupt outside a guarded action still exits 130 rather than click's 1."""
        try:
            return super().invoke(ctx)
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
        """Run `sst` once: record the invocation, then report a usage error as JSON when asked to.

        With `--output json` anywhere on the command line, click runs in non-standalone mode,
        so a usage error reaches this method and becomes one envelope with exit 3, and a run
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
        except click.UsageError as exc:
            if json_requested:
                command = next((value for value in arguments if value in self.commands), "")
                print_envelope(
                    json_envelope(command, DiagnosticBag(), exit_code=USAGE, status="error", data={"error": str(exc)})
                )
                if standalone_mode:
                    raise SystemExit(USAGE) from exc
                return USAGE
            raise
        if json_requested and standalone_mode:
            raise SystemExit(result if isinstance(result, int) else OK)
        return result


def _json_requested(arguments: list[str]) -> bool:
    """Return whether the command line asks for `--output json`, in either spelling."""
    return any(
        value == "--output=json"
        or value == "--output"
        and index + 1 < len(arguments)
        and arguments[index + 1] == "json"
        for index, value in enumerate(arguments)
    )


@click.group(
    cls=SstGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(version=VERSION, prog_name="sst")
@click.option("--output", "global_output", type=click.Choice(["human", "json"]), default=None)
@click.option(
    "--project-dir",
    "global_project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
)
@click.pass_context
def cli(ctx: click.Context, global_output: str | None, global_project_dir: Path | None) -> None:
    """Snowflake Semantic Tools 1.0."""
    defaults = dict(ctx.default_map or {})
    for command_name in cli.commands:
        command_defaults = dict(defaults.get(command_name, {}))
        if global_output is not None:
            command_defaults["output"] = global_output
        if global_project_dir is not None:
            command_defaults["project_dir"] = global_project_dir
        defaults[command_name] = command_defaults
    ctx.default_map = defaults


# click's usage errors exit 2, which `sst` reserves for a plan with changes pending.
for _click_error in (
    click.UsageError,
    click.BadParameter,
    click.NoSuchOption,
    click.MissingParameter,
):
    _click_error.exit_code = USAGE
