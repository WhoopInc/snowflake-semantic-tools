"""The global options: accepted before the command, after it, or both, the later copy winning.

`sst` declares each one twice: on the root group, where its environment variable is read, and on
every command, whose help lists it under `Global options`. The group hands a value given on its
command line or read from the environment to each command as that command's default, so a value
written after the command wins, then one written before it, then the environment, then the
built-in default. A command body sees the result as `GlobalOptions`.

Five short options SST 0.3 bound are reserved rather than rebound: each is refused with what to
write instead. A boolean environment variable takes `1`, `true`, `yes`, `on` or their negations,
case-insensitively, and anything else is a configuration error rather than a silent false.
"""

from __future__ import annotations

import dataclasses
import inspect
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import click

from snowflake_semantic_tools.cli.options import Decorator, stacked
from snowflake_semantic_tools.cli.render.terminal import COLOR_CHOICES

# The `--output` values in the order help lists them; `human` is accepted as `table` for one release.
OUTPUT_FORMATS = ("table", "plain", "json", "yaml", "csv")
HIDDEN_OUTPUT_ALIASES: Mapping[str, str] = {"human": "table"}
# What every command but `list` supports; an unsupported value is a usage error, never a fallback.
DEFAULT_OUTPUTS = ("table", "plain", "json")
LOG_LEVELS = ("debug", "info", "warn", "error")
_TRUE = frozenset(("1", "true", "yes", "on"))
_FALSE = frozenset(("0", "false", "no", "off"))

# Each 0.3 short option that is reserved: what it meant, and what to write instead. `-s` and `-d`
# chose where views publish, now the `+schema` and `+database` keys; a command that takes the
# flag itself is told to write it, and `-s` also names `--select`, which it will become.
RESERVED_SHORT_OPTIONS: Mapping[str, tuple[str, str]] = {
    "-s": ("it meant --schema in SST 0.3 and becomes --select in 1.0.1", "--schema"),
    "-d": ("it meant --database in SST 0.3", "--database"),
    "-f": ("it meant --format in SST 0.3", "--output"),
    "-a": ("it meant --all in SST 0.3", "--select"),
    "-m": ("it meant --models in SST 0.3", "--select"),
}
# Where the publish location a reserved option chose is set now, for a command without the flag.
_LOCATION_KEYS: Mapping[str, str] = {"-s": "+schema", "-d": "+database"}
# Where the 0.3 migration guide tells each removed command and option's replacement.
MIGRATION_GUIDE = "docs/guides/migrating-from-0.3.md"
# Each SST 0.3 command 1.0 removed, and what replaces it (the migration guide's Commands table).
REMOVED_COMMANDS: Mapping[str, str] = {
    "deploy": "run sst compile, sst plan, then sst apply",
    "generate": "run sst compile, sst plan, then sst apply; sst plan --sql-out <dir> writes the statements",
    "extract": "nothing replaces it: 1.0 compiles from the dbt manifest and records what it published in state",
    "migrate-meta": "nothing replaces it: author column metadata in the dbt model YAML",
}
# Each SST 0.3 long option 1.0 removed, whether it took a value, and what replaces it. A command
# that defines the option itself keeps its own; `--prune` is refused on `drop` alone.
REMOVED_OPTIONS: tuple[tuple[str, bool, str], ...] = (
    ("--format", True, "write --output (-o) json"),
    ("--only-modified", False, "write --select state:modified --state <dir>"),
    ("--skip-validation", False, "write --no-validate"),
    ("--views", True, "write --select"),
    ("--view", True, "write --select"),
    ("--db", True, "set +database in sst_config.yml for where artifacts publish"),
    ("--models", True, "write --select, with --exclude"),
    ("--extract-to-snowflake", False, "nothing replaces it: 1.0 records what it published in its state table"),
)
_PRUNE_ON_DROP = ("--prune", False, "run sst plan --prune, then sst apply --prune")


class StrictBool(click.types.BoolParamType):
    """A boolean that accepts only `1`/`true`/`yes`/`on` and their negations, in any case."""

    name = "boolean"

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> Any:
        """Return the boolean `value` spells; anything else fails, naming the accepted spellings."""
        if isinstance(value, bool):
            return value
        text = str(value).strip().casefold()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        self.fail(f"{value!r} is not a boolean; use 1, true, yes, on, or 0, false, no, off", param, ctx)


class OutputFormat(click.Choice[str]):
    """The `--output` choice, which also accepts the hidden `human` alias as `table`."""

    def __init__(self) -> None:
        super().__init__(OUTPUT_FORMATS)

    def convert(self, value: Any, param: click.Parameter | None, ctx: click.Context | None) -> Any:
        """Return the format `value` names, reading a hidden alias as the format it stands for."""
        if isinstance(value, str) and value in HIDDEN_OUTPUT_ALIASES:
            return HIDDEN_OUTPUT_ALIASES[value]
        return super().convert(value, param, ctx)


STRICT_BOOL = StrictBool()


@dataclasses.dataclass(frozen=True)
class GlobalOptions:
    """What the global options resolved to for one run.

    Attributes:
        output: One of `OUTPUT_FORMATS`; a hidden alias is already read as its format.
        project_dir: `--project-dir`, else `$SST_PROJECT_DIR`, else the current directory.
        config: `--config`, else `$SST_CONFIG`; None discovers the file in the project root.
        profiles_dir: `--profiles-dir`, else `$SST_PROFILES_DIR`; None searches.
        manifest: The dbt manifest to read instead of running `dbt parse`; None runs it.
        baseline: The baseline file; None reads `.sst/baseline.json` when it exists.
        color: `auto`, `always` or `never`; `--no-color` wins over it, as `never`.
    """

    output: str = "table"
    verbose: bool = False
    quiet: bool = False
    log_level: str = "warn"
    no_color: bool = False
    color: str = "auto"
    project_dir: Path = Path(".")
    config: Path | None = None
    profiles_dir: Path | None = None
    manifest: Path | None = None
    allow_unsupported_manifest_schema: bool = False
    allow_stale_manifest: bool = False
    baseline: Path | None = None
    no_baseline: bool = False
    show_baselined: bool = False
    show_info: bool = False
    show_cascade: bool = False
    show_all_occurrences: bool = False

    @property
    def overrides(self) -> tuple[str, ...]:
        """Name every guarantee this run switched off, for the envelope's `invocation`."""
        switched = (
            ("--allow-unsupported-manifest-schema", self.allow_unsupported_manifest_schema),
            ("--allow-stale-manifest", self.allow_stale_manifest),
            ("--no-baseline", self.no_baseline),
        )
        return tuple(flag for flag, on in switched if on)

    @classmethod
    def from_params(cls, params: Mapping[str, Any]) -> GlobalOptions:
        """Build the run's options from a command's parameters, each unset one at its default."""
        defaults = cls()
        values = {
            field.name: params[field.name] if params.get(field.name) is not None else getattr(defaults, field.name)
            for field in dataclasses.fields(cls)
        }
        return cls(**values)


GLOBAL_NAMES = frozenset(field.name for field in dataclasses.fields(GlobalOptions))


Declaration = tuple[tuple[str, ...], Mapping[str, Any], "str | None"]


def _declarations() -> tuple[Declaration, ...]:
    """Return each global option's names, its click settings, and its environment variable."""
    path = click.Path(path_type=Path)
    declared: tuple[Declaration, ...] = (
        (("--output", "-o", "output"), {"type": OutputFormat()}, "SST_OUTPUT"),
        (("--verbose", "-v", "verbose"), {"is_flag": True, "default": None}, None),
        (("--quiet", "-q", "quiet"), {"is_flag": True, "default": None}, None),
        (("--log-level", "log_level"), {"type": click.Choice(LOG_LEVELS)}, "SST_LOG_LEVEL"),
        (("--no-color", "no_color"), {"is_flag": True, "default": None, "type": STRICT_BOOL}, "SST_NO_COLOR"),
        (("--color", "color"), {"type": click.Choice(COLOR_CHOICES)}, "SST_COLOR"),
        (("--project-dir", "project_dir"), {"type": click.Path(file_okay=False, path_type=Path)}, "SST_PROJECT_DIR"),
        (("--config", "config"), {"type": path}, "SST_CONFIG"),
        (("--profiles-dir", "profiles_dir"), {"type": click.Path(file_okay=False, path_type=Path)}, "SST_PROFILES_DIR"),
        (("--manifest", "manifest"), {"type": click.Path(exists=True, dir_okay=False, path_type=Path)}, None),
        (("--allow-unsupported-manifest-schema", "allow_unsupported_manifest_schema"), _FLAG, None),
        (("--allow-stale-manifest", "allow_stale_manifest"), _FLAG, None),
        (("--baseline", "baseline"), {"type": click.Path(dir_okay=False, path_type=Path)}, None),
        (("--no-baseline", "no_baseline"), _FLAG, None),
        (("--show-baselined", "show_baselined"), _FLAG, None),
        (("--show-info", "show_info"), _FLAG, None),
        (("--show-cascade", "show_cascade"), _FLAG, None),
        (("--show-all-occurrences", "show_all_occurrences"), _FLAG, None),
    )
    return declared


_FLAG: Mapping[str, Any] = {"is_flag": True, "default": None}


def group_global_options() -> Decorator:
    """Declare every global option on the root group, each with its environment variable."""
    return stacked(
        *(click.option(*names, envvar=envvar, **settings) for names, settings, envvar in _declarations()),
        reserved_short_options(),
    )


def command_global_options() -> Decorator:
    """Declare every global option on one command, so it is accepted after the command too.

    Each is a `GlobalOption`: a command's `--help` lists them under their own heading, as
    `SstCommand` renders it, so `sst <command> --help` names every option the command accepts.
    """
    return stacked(
        *(click.option(*names, cls=GlobalOption, **settings) for names, settings, _ in _declarations()),
        reserved_short_options(),
    )


class GlobalOption(click.Option):
    """A global option as one command declares it: parsed by the command, listed apart in its help."""


class SstCommand(click.Command):
    """An `sst` command, whose help lists its own options, then the global ones under their own heading."""

    def format_help_text(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Write the command's help, keeping its `Diagnostics:` list one code per line.

        click rewraps every paragraph as prose; `\\b` before the list is its marker to leave
        that paragraph's lines as written. The reference page reads `help` itself and is unchanged.
        """
        written = self.help
        if written is not None:
            self.help = re.sub(r"\n(\s*)Diagnostics:\n", "\n\\1\b\nDiagnostics:\n", inspect.cleandoc(written))
        try:
            super().format_help_text(ctx, formatter)
        finally:
            self.help = written

    def format_options(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Write the command's options, then `Global options`: each section only when it has a row."""
        sections: dict[bool, list[tuple[str, str]]] = {False: [], True: []}
        for param in self.get_params(ctx):
            record = param.get_help_record(ctx)
            if record is not None:
                sections[isinstance(param, GlobalOption)].append(record)
        for title, rows in (("Options", sections[False]), ("Global options", sections[True])):
            if rows:
                with formatter.section(title):
                    formatter.write_dl(rows)


def reserved_short_options() -> Decorator:
    """Declare each reserved 0.3 short option, hidden, refusing it with what to write instead."""
    return stacked(
        *(
            click.option(
                letter,
                f"reserved_{letter[1:]}",
                is_flag=True,
                hidden=True,
                expose_value=False,
                callback=_refuse_reserved,
            )
            for letter in RESERVED_SHORT_OPTIONS
        )
    )


def refuse_removed_options(command: click.Command, path: str = "sst") -> None:
    """Give each command, and each group's subcommands, a hidden refusal for every removed 0.3 option.

    Run once every command is registered, so an option a command defines itself -- such as
    `sst format --check`'s own options, or `sst plan --prune` -- is left to it.
    """
    if isinstance(command, click.Group):
        for name, child in command.commands.items():
            refuse_removed_options(child, f"{path} {name}")
        return
    removed = (*REMOVED_OPTIONS, *((_PRUNE_ON_DROP,) if path == "sst drop" else ()))
    taken = {name for param in command.params for name in param.opts}
    for flag, takes_value, replacement in removed:
        if flag not in taken:
            command.params.append(
                click.Option(
                    [flag],
                    is_flag=not takes_value,
                    hidden=True,
                    expose_value=False,
                    callback=_refuse_removed(replacement),
                )
            )


def _refuse_removed(replacement: str) -> Callable[[click.Context, click.Parameter, Any], None]:
    def callback(ctx: click.Context, param: click.Parameter, value: Any) -> None:
        if value:
            # Imported here: `cli.group` imports this module to declare the root group's options.
            from snowflake_semantic_tools.cli.group import SstUsageError

            message = f"{param.opts[0]} was removed in SST 1.0; {replacement} (see {MIGRATION_GUIDE})"
            raise SstUsageError(message, ctx)

    return callback


def reserved_message(letter: str, command: click.Command) -> str:
    """Say why a reserved short option is refused, naming only flags `command` takes."""
    meant, flag = RESERVED_SHORT_OPTIONS[letter]
    takes = {name for param in command.params for name in param.opts}
    flags = [name for name in dict.fromkeys((flag, "--select" if letter == "-s" else flag)) if name in takes]
    advice = [f"write {' or '.join(flags)}"] if flags else []
    if letter in _LOCATION_KEYS and flag not in takes:
        advice.append(f"set {_LOCATION_KEYS[letter]} in sst_config.yml for where artifacts publish")
    return f"{letter} is reserved: {meant}; {', or '.join(advice) or f'this command takes no {flag}'}"


def _refuse_reserved(ctx: click.Context, param: click.Parameter, value: Any) -> None:
    if value:
        # Imported here: `cli.group` imports this module to declare the root group's options.
        from snowflake_semantic_tools.cli.group import SstUsageError

        raise SstUsageError(reserved_message(param.opts[0], ctx.command), ctx)


def forwarded(ctx: click.Context, params: Mapping[str, Any]) -> dict[str, Any]:
    """Return the root group's global values the user set, by the command or the environment.

    A value left at its built-in default is not forwarded, so it cannot outrank a command's own.
    """
    explicit = (click.core.ParameterSource.COMMANDLINE, click.core.ParameterSource.ENVIRONMENT)
    return {
        name: value
        for name, value in params.items()
        if name in GLOBAL_NAMES and value is not None and ctx.get_parameter_source(name) in explicit
    }
