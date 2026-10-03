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
    "--project-dir": "Project root, else `$SST_PROJECT_DIR`: where `sst_config.yml` and `dbt_project.yml` are.",
    "--config": "Read this configuration file, else `$SST_CONFIG`, instead of discovering one in the project.",
    "--profiles-dir": "Directory of `profiles.yml`, else `$SST_PROFILES_DIR`, then `$DBT_PROFILES_DIR`.",
    "--target": "Target from `profiles.yml`, else `$SST_TARGET`; defaults to the profile's own default target.",
    "--manifest": "Read this dbt `manifest.json` instead of running `dbt parse`.",
    "--output": "`table` (default), `plain`, or `json`; `list` also takes `yaml` and `csv`. Else `$SST_OUTPUT`.",
    "--verbose": "Add each diagnostic's phase and fingerprint. Cannot be combined with `--quiet`.",
    "--quiet": "Show errors only.",
    "--log-level": "Log threshold, else `$SST_LOG_LEVEL`; independent of `--verbose`.",
    "--no-color": "No ANSI colour, as `$SST_NO_COLOR` or a non-empty `$NO_COLOR` also say.",
    "--allow-unsupported-manifest-schema": "Read a dbt manifest of an unsupported schema version, for this run only.",
    "--allow-stale-manifest": "Accept a `--manifest` older than the files it describes, for this run only.",
    "--baseline": "Baseline file of known warnings; `.sst/baseline.json` when it exists.",
    "--no-baseline": "Ignore the baseline for this run: every diagnostic is shown and blocks as declared.",
    "--show-baselined": "Show baselined diagnostics instead of counting them.",
    "--show-info": "Show info diagnostics instead of counting them.",
    "--show-cascade": "Show cascade diagnostics instead of counting them.",
    "--show-all-occurrences": "Show every occurrence of a code that repeats four or more times.",
    "--select": (
        "Only these artifacts: a name (globs allowed), `type:<type>`, `path:<glob>`, `state:<state>`, "
        "or `<type>:<name>`."
    ),
    "--exclude": "Leave these artifacts out; same forms as `--select`.",
    "--state": "Previous run's build directory, else `$SST_STATE_DIR`, for `state:` selectors.",
    "--defer-target": (
        "Resolve dbt objects to this `profiles.yml` target's relations while publishing to `--target`, "
        "else `$SST_DEFER_TARGET`, else `defer.target`."
    ),
    "--no-defer": "Defer to no target, whatever `defer.target` says.",
    "--threads": (
        "Snowflake sessions to work on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`, "
        "else 1. Output is the same for any count."
    ),
    "--database": "Read from this database instead of the target's; never where an artifact is published.",
    "--no-detailed-exitcode": "Exit 0 instead of 2 when there are differences.",
    "--strict": "Promote every warning to an error, else `$SST_STRICT`. Defaults to `validation.strict`.",
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
    "--no-validate": (
        "Skip validation: no cycle check, connected check, or strict promotion. Only when `sst validate` "
        "already ran on the same tree; compile errors still stop the run."
    ),
    "--dbt": "Read the dbt models from this directory instead of `dbt_project.yml`'s `model-paths`.",
    "--semantic": "Read the semantic models from this directory instead of `project.semantic_models_dir`.",
}
_COMMAND_OPTION_HELP: Mapping[tuple[str, str], str] = {
    ("sst apply", "--fail-fast"): "Stop at the first failure instead of continuing. Defaults to `apply.fail_fast`.",
    ("sst apply", "--threads"): (
        "Sessions to plan and apply on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`. "
        "Planning defaults to 1; applying to `skills.+threads`, else 4."
    ),
    ("sst plan", "--threads"): (
        "Sessions to validate and observe on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`, "
        "else 1. The plan is the same for any count."
    ),
    ("sst test", "--threads"): (
        "Smoke probes, and evals no `concurrency` setting paces, run at once, 1 to 16, else `$SST_THREADS`, "
        "else `generation.threads`, else 1."
    ),
    ("sst compile", "--database"): "Resolve refs against this database instead of the target's.",
    ("sst compile", "--emit-agent-spec"): "Write each agent's rendered specification into this directory.",
    ("sst debug", "--no-connect"): "Report everything but the connection, offline.",
    ("sst debug", "--snowflake-signatures"): "Report how often Snowflake refusals went unrecognised, from the run log.",
    ("sst init", "--skip-prompts"): "Accept every default without prompting.",
    ("sst init", "--check-only"): "Report whether the setup is complete; create nothing.",
    ("sst clean", "--dry-run"): "List what would be removed; remove nothing.",
    ("sst docs", "--output-dir"): "Write the pages here instead of `docs/reference`.",
    ("sst list", "--long"): "Every detail column, including each artifact's source files.",
    ("sst apply", "--plan"): "Apply this saved plan. It must still match the compiled project.",
    ("sst apply", "--yes"): "Apply without asking for confirmation.",
    ("sst apply", "--break-stale-lock"): "Take over a state lock left behind by a run that no longer exists.",
    ("sst apply", "--temporary"): (
        "Publish agents as session-scoped temporary agents; refused for a production-like target."
    ),
    ("sst compile", "--emit-ddl"): "Write each artifact's rendered payload into this directory, offline.",
    ("sst compile", "--select"): "Report and emit only these artifacts; the manifest still holds everything.",
    ("sst docs", "--check"): "Write nothing; exit 2 when a committed reference page is out of date.",
    ("sst docs", "--only"): (
        "Repeatable. Generate only these references: `errors`, `coverage` (the code-to-test matrix), "
        "`artifacts`, `cli`, `config`. Defaults to all of them."
    ),
    ("sst enrich", "--select"): "Only these dbt models: `model:<name>` or a bare name; globs such as `fct_*` work.",
    ("sst enrich", "--exclude"): "Leave these dbt models out; same forms as `--select`.",
    ("sst enrich", "--include"): (
        "Components to fill, repeatable or comma-separated: column-types, data-types, sample-values, enums, "
        "column-synonyms, table-synonyms, synonyms, all. Defaults to column-types and data-types."
    ),
    ("sst enrich", "--force"): "Components to derive again over values already written; forcing one includes it.",
    ("sst enrich", "--database"): "Read every relation from this database instead of the manifest's.",
    ("sst enrich", "--schema"): "Read every relation from this schema instead of the manifest's.",
    ("sst enrich", "--allow-non-prod"): (
        "Enrich from the manifest of a target that is not production-like: one whose name has no "
        "`prod`, `production` or `prd` part. Refused without it."
    ),
    ("sst enrich", "--check"): "Write nothing; exit 2 when a file would change.",
    ("sst enrich", "--dry-run"): "Write nothing; print each file's change as a diff.",
    ("sst enrich", "--no-detailed-exitcode"): "With `--check`, exit 0 when files would change, instead of 2.",
    ("sst enrich", "--fail-fast"): "Stop at the first model that fails, and write nothing.",
    ("sst plan", "--plan-out"): "Write the saved plan here instead of `target/sst/plan.json`.",
    ("sst plan", "--no-plan-out"): "Do not write a saved plan.",
    ("sst plan", "--grants"): (
        "Read the grants on each object an update replaces, one `SHOW GRANTS` each, to report what a "
        "replace would drop (SST-PLN013). On by default; `--no-grants` skips the reads."
    ),
    ("sst plan", "--capture-prior"): (
        "Read the current definition of each live object a planned artifact names, one `GET_DDL` "
        "each; needs REFERENCES or OWNERSHIP. Shown by `--full` and as `prior_definition` in JSON."
    ),
    ("sst plan", "--full"): "Also print, under each update, the properties it changes on the object.",
    ("sst plan", "--names-only"): "Print the name of each changed artifact, one per line, and nothing else.",
    ("sst plan", "--no-detailed-exitcode"): "Exit 0 when changes are pending, instead of 2.",
    ("sst plan", "--state"): (
        "Directory holding the previous run's `manifest.json`, which `--select state:modified` compares with."
    ),
    ("sst test", "--suite"): (
        "Repeatable. `golden` compares outputs with committed goldens offline; `smoke` probes deployed "
        "objects; `evals` runs agent evaluations. Defaults to every suite that applies."
    ),
    ("sst test", "--golden-dir"): "Directory of the semantic view DDL goldens; the other goldens sit beside it.",
    ("sst test", "--update-golden"): (
        "Rewrite each golden the current output no longer equals, and create missing ones; runs the "
        "golden suite only. Refused with `--suite smoke` or `evals`, and whenever `$CI` is set."
    ),
    ("sst test", "--capture-baseline"): "Record this eval run as the new baseline. Requires `--reason`.",
    ("sst test", "--reason"): "Why the baseline is changing; stored with it.",
    ("sst test", "--fail-fast"): "Stop at the first failing golden, probe, or eval.",
    ("sst validate", "--verify-schema"): "Connects: confirm each column a semantic view reads exists in the warehouse.",
    ("sst list", "--no-manifest"): "Compile the project's files in memory instead of reading the compiled manifest.",
    ("sst validate", "--show-info"): "Also report which registered type owns each semantic-model file.",
    ("sst baseline add", "--all-warnings"): "Baseline every current warning; prints the count and needs `--yes`.",
    ("sst baseline add", "--expires-in"): (
        "Days, 1 to 365, until a new baseline expires; an existing one keeps its date until `renew`."
    ),
    ("sst baseline add", "--note"): "Written into each entry; defaults to `pre-existing at adoption of <code>`.",
    ("sst baseline add", "--yes"): "Baseline every warning without asking; required off a terminal.",
    ("sst baseline add", "--select"): "Baseline only the diagnostics of these artifacts.",
    ("sst baseline add", "--exclude"): "Leave the diagnostics of these artifacts out.",
    ("sst baseline prune", "--select"): "Prune only the entries of these artifacts.",
    ("sst baseline prune", "--exclude"): "Leave the entries of these artifacts as they are.",
    ("sst baseline show", "--code"): "Show only the entries of this code.",
    ("sst baseline show", "--expired"): "Show the entries only once the baseline has expired.",
    ("sst baseline renew", "--reason"): "Required. Why the baseline is renewed; written into the file.",
    ("sst baseline renew", "--expires-in"): "Days until the renewed baseline expires, at most 365.",
    ("sst diff", "--from"): "The state compared from: `local` (default), a dbt target, or a saved plan's `.json` path.",
    (
        "sst diff",
        "--to",
    ): "The state compared with: `local`, a dbt target (default: the resolved one), or a saved plan.",
    (
        "sst diff",
        "--target",
    ): "Target from `profiles.yml` that `--to` defaults to, else `$SST_TARGET`, else the profile's.",
    ("sst diff", "--full"): "Also name which recorded fields of a modified artifact differ.",
    ("sst diff", "--names-only"): "Print the name of each differing artifact, one per line, and nothing else.",
    ("sst diff", "--no-detailed-exitcode"): "Exit 0 when the states differ, instead of 2.",
    (
        "sst drop",
        "--type",
    ): "Required. The registered type of the object, which picks the DROP: agent or semantic_view.",
    (
        "sst drop",
        "--target",
    ): "Required. Target from `profiles.yml`; there is no default, and `$SST_TARGET` is not read.",
    (
        "sst drop",
        "--profile",
    ): "Profile in `profiles.yml`; else `dbt_project.yml`'s `profile:`. Needed outside a project.",
    ("sst drop", "--yes"): "Required on every invocation: it is the confirmation, and there is no prompt.",
    ("sst explain", "--aliases"): "Also list the SST 0.3 codes that resolve to the code, and what each became.",
    ("sst format", "--check"): "Write nothing; exit 2 when a file would change.",
    ("sst format", "--dry-run"): "Write nothing; print each file's change as a diff.",
    ("sst format", "--force"): "Rewrite every file, even one already in canonical form.",
    ("sst format", "--sanitize"): (
        "Also repair apostrophes in synonyms and sample values, and Jinja delimiters in descriptions."
    ),
    ("sst format", "--no-detailed-exitcode"): "With `--check`, exit 0 when files would change, instead of 2.",
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
    with click.Context(group) as ctx:
        names = group.list_commands(ctx)
    for name in names:
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
