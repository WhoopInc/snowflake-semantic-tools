"""The help text each option shows, and the command tree `sst docs` renders as the CLI reference.

Options are declared without help text. Once every command is registered,
`document_options` gives each flag its one shared description, so `--help` and the
generated docs/reference/cli.md say the same thing everywhere.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from pathlib import Path

import click

from snowflake_semantic_tools.domain.render.reference_docs import CommandDoc, OptionDoc

# One description per flag, shared by every command that takes it, so `--help`
# and the generated CLI reference say the same thing everywhere. A command whose
# flag means something narrower overrides it in `_COMMAND_OPTION_HELP`.
_OPTION_HELP: Mapping[str, str] = {
    "--project-dir": "Project root: the directory that holds `sst_config.yml`.",
    "--target": "Target from `profiles.yml`; defaults to the profile's own default target.",
    "--manifest": "Read this dbt `manifest.json` instead of running `dbt parse`.",
    "--output": "`human` for readable text, or `json` for one machine-readable envelope.",
    "--select": "Only these artifacts: a semantic view name, `type:<type>`, or `<type>:<name>`.",
    "--exclude": "Leave these artifacts out; same forms as `--select`.",
    "--strict": "Promote every warning to an error. Defaults to `validation.strict`.",
    "--snowflake-syntax-check": (
        "Compile expressions against Snowflake. Defaults to `validation.snowflake_syntax_check`."
    ),
    "--prune": (
        "Also act on managed artifacts whose source was deleted, as far as each type "
        "allows: drop, deactivate, or report."
    ),
    "--partial": (
        "Go ahead with every artifact that has no errors and depends on nothing that does; "
        "still exits 1 while errors remain. Cannot be combined with `--prune`."
    ),
    "--sql-out": "Also write the statements for each change into this directory.",
    "--fail-fast": "Stop at the first failure instead of continuing.",
}
_COMMAND_OPTION_HELP: Mapping[tuple[str, str], str] = {
    ("sst", "--output"): "Default `--output` for the command that follows.",
    ("sst", "--project-dir"): "Default `--project-dir` for the command that follows.",
    ("sst apply", "--plan"): "Apply this saved plan. It must still match the compiled project.",
    ("sst apply", "--yes"): "Apply without asking for confirmation.",
    ("sst apply", "--break-stale-lock"): "Take over a state lock left behind by a run that no longer exists.",
    ("sst compile", "--emit-ddl"): "Write each semantic view's rendered DDL into this directory.",
    ("sst compile", "--print-ddl"): "Print the rendered DDL to stdout.",
    ("sst compile", "--ddl-output-dir"): "Same as `--emit-ddl`.",
    ("sst compile", "--manifest-output"): "Also write the SST manifest here; with `--select`, only the selection.",
    ("sst compile", "--select"): "Only this artifact: a semantic view name, `type:<type>`, or `<type>:<name>`.",
    ("sst debug", "--test-connection"): "Also connect to Snowflake and report the session's role and account.",
    ("sst docs", "--check"): "Write nothing; exit 1 when a committed reference page is out of date.",
    ("sst enrich", "--select"): "Only these dbt models: `model:<name>` or a bare name; globs such as `fct_*` work.",
    ("sst enrich", "--exclude"): "Leave these dbt models out; same forms as `--select`.",
    ("sst enrich", "--include"): (
        "Components to fill, repeatable or comma-separated: column-types, data-types, sample-values, enums, "
        "column-synonyms, table-synonyms, synonyms, all. Defaults to column-types and data-types."
    ),
    ("sst enrich", "--force"): "Components to derive again over values already written; forcing one includes it.",
    ("sst enrich", "--database"): "Read every relation from this database instead of the manifest's.",
    ("sst enrich", "--schema"): "Read every relation from this schema instead of the manifest's.",
    ("sst enrich", "--check"): "Write nothing; exit 2 when a file would change.",
    ("sst enrich", "--dry-run"): "Write nothing; print each file's change as a diff.",
    ("sst enrich", "--no-detailed-exitcode"): "With `--check`, exit 0 when files would change, instead of 2.",
    ("sst enrich", "--fail-fast"): "Stop at the first model that fails, and write nothing.",
    ("sst plan", "--plan-out"): "Write the saved plan here instead of `target/sst/plan.json`.",
    ("sst plan", "--no-plan-out"): "Do not write a saved plan.",
    ("sst plan", "--no-detailed-exitcode"): "Exit 0 when changes are pending, instead of 2.",
    ("sst test", "--suite"): (
        "`golden` compares outputs with committed goldens offline; `smoke` probes deployed "
        "objects; `evals` runs agent evaluations."
    ),
    ("sst test", "--golden-dir"): "Directory of the semantic view DDL goldens; the other goldens sit beside it.",
    ("sst test", "--capture-baseline"): "Record this eval run as the new baseline. Requires `--reason`.",
    ("sst test", "--reason"): "Why the baseline is changing; stored with it.",
    ("sst test", "--fail-fast"): "Stop at the first failing golden, probe, or eval.",
    ("sst validate", "--show-info"): "Also report which registered type owns each semantic-model file.",
}


def document_options(command: click.Command, path: str = "sst") -> None:
    """Give every option declared without help text its shared description."""
    for param in command.params:
        if isinstance(param, click.Option) and not param.help:
            flag = param.opts[0]
            param.help = _COMMAND_OPTION_HELP.get((path, flag)) or _OPTION_HELP.get(flag)
    for name, child in getattr(command, "commands", {}).items():
        document_options(child, f"{path} {name}")


def command_docs(group: click.Group, prefix: str = "sst") -> tuple[CommandDoc, ...]:
    """The visible command tree, each group followed by its subcommands."""
    documented: list[CommandDoc] = []
    for name in sorted(group.commands):
        command = group.commands[name]
        if command.hidden:
            continue
        path = f"{prefix} {name}"
        description = inspect.cleandoc(command.help or "")
        if isinstance(command, click.Group):
            children = tuple(f"{path} {child}" for child in sorted(command.commands))
            documented.append(CommandDoc(path, description, subcommands=children))
            documented.extend(command_docs(command, path))
        else:
            documented.append(CommandDoc(path, description, option_docs(command)))
    return tuple(documented)


def option_docs(command: click.Command) -> tuple[OptionDoc, ...]:
    """Document each visible option of `command`, in declaration order, as the CLI reference lists it.

    A flag takes no value; a choice lists its choices, a path says `DIRECTORY`, `FILE`, or
    `PATH`, and any other type gives its name. Only a string, number, or path default is shown.
    """
    documented: list[OptionDoc] = []
    for param in command.params:
        if not isinstance(param, click.Option) or param.hidden:
            continue
        if param.is_flag:
            value = None
        elif isinstance(param.type, click.Choice):
            value = "|".join(str(choice) for choice in param.type.choices)
        elif isinstance(param.type, click.Path):
            value = "DIRECTORY" if not param.type.file_okay else "FILE" if not param.type.dir_okay else "PATH"
        else:
            value = param.type.name.upper()
        default = param.default
        shown = str(default) if isinstance(default, (str, int, Path)) and not isinstance(default, bool) else None
        documented.append(
            OptionDoc(
                " / ".join((*param.opts, *param.secondary_opts)),
                value,
                None if param.is_flag else shown,
                param.help or "",
                multiple=param.multiple,
                required=param.required,
            )
        )
    return tuple(documented)
