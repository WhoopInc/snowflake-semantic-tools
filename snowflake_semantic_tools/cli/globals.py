"""The global options: accepted before the command, after it, or both, the later copy winning.

`sst` declares each one twice: on the root group, where its environment variable is read, and,
hidden, on every command. The group hands a value given on its command line or read from the
environment to each command as that command's default, so a value written after the command
wins, then one written before it, then the environment, then the built-in default. A command
body sees the result as `GlobalOptions`.

Five short options SST 0.3 bound are reserved rather than rebound: each is refused with what to
write instead. A boolean environment variable takes `1`, `true`, `yes`, `on` or their negations,
case-insensitively, and anything else is a configuration error rather than a silent false.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import click

from snowflake_semantic_tools.cli.options import Decorator, stacked

# The `--output` values in the order help lists them; `human` is accepted as `table` for one release.
OUTPUT_FORMATS = ("table", "plain", "json", "yaml", "csv")
HIDDEN_OUTPUT_ALIASES: Mapping[str, str] = {"human": "table"}
# What every command but `list` supports; an unsupported value is a usage error, never a fallback.
DEFAULT_OUTPUTS = ("table", "plain", "json")
LOG_LEVELS = ("debug", "info", "warn", "error")
_TRUE = frozenset(("1", "true", "yes", "on"))
_FALSE = frozenset(("0", "false", "no", "off"))

# Each 0.3 short option that is reserved, and the usage error that names its replacement.
RESERVED_SHORT_OPTIONS: Mapping[str, str] = {
    "-s": "-s is reserved: it meant --schema in SST 0.3 and becomes --select in 1.0.1; write --schema or --select",
    "-d": "-d is reserved: it meant --database in SST 0.3; write --database",
    "-f": "-f is reserved: it meant --format in SST 0.3; use --output",
    "-a": "-a is reserved: it meant --all in SST 0.3; use --select",
    "-m": "-m is reserved: it meant --models in SST 0.3; use --select",
}


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
    """

    output: str = "table"
    verbose: bool = False
    quiet: bool = False
    log_level: str = "info"
    no_color: bool = False
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
    """Declare every global option, hidden, on one command, so it is accepted after the command too."""
    return stacked(
        *(click.option(*names, hidden=True, **settings) for names, settings, _ in _declarations()),
        reserved_short_options(),
    )


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
                callback=_refuse_reserved(message),
            )
            for letter, message in RESERVED_SHORT_OPTIONS.items()
        )
    )


def _refuse_reserved(message: str) -> Callable[[click.Context, click.Parameter, Any], None]:
    def callback(ctx: click.Context, param: click.Parameter, value: Any) -> None:
        if value:
            # Imported here: `cli.group` imports this module to declare the root group's options.
            from snowflake_semantic_tools.cli.group import SstUsageError

            raise SstUsageError(message, ctx)

    return callback


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
