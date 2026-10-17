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

from snowflake_semantic_tools.cli.globals import GlobalOption
from snowflake_semantic_tools.domain.render.reference_docs import CommandDoc, OptionDoc

# One description per flag, shared by every command that takes it, so `--help`
# and the generated CLI reference say the same thing everywhere. A command whose
# flag means something narrower overrides it in `_COMMAND_OPTION_HELP`.
_OPTION_HELP: Mapping[str, str] = {
    "--project-dir": (
        "Project root, else `$SST_PROJECT_DIR`, else the current directory: where `sst_config.yml` and "
        "`dbt_project.yml` are."
    ),
    "--config": "Read this configuration file, else `$SST_CONFIG`, instead of discovering one in the project.",
    "--profiles-dir": (
        "Look for `profiles.yml` here first, else in `$SST_PROFILES_DIR`; then in `$DBT_PROFILES_DIR`, the "
        "project root, and `~/.dbt`. The first that has one wins."
    ),
    "--target": "Target from `profiles.yml`, else `$SST_TARGET`; defaults to the profile's own default target.",
    "--manifest": "Read this dbt `manifest.json` instead of running `dbt parse`.",
    "--break-stale-lock": (
        "Take over the run lock only once the run holding it has expired (SST-APL010); a live lock "
        "is refused all the same (SST-APL011)."
    ),
    "--output": (
        "`table` (default), `plain` (`table` without colour), or `json`, else `$SST_OUTPUT`; `list` also "
        "takes `yaml` and `csv`. A format the command does not take is refused (SST-PRT106)."
    ),
    "--verbose": (
        "Add each diagnostic's phase and fingerprint, and report each file the run's parse cache served "
        "(SST-LOD201). Cannot be combined with `--quiet`."
    ),
    "--quiet": "Show error diagnostics only, with no counts line or progress lines; the report still prints.",
    "--log-level": (
        "Threshold for log records on stderr, such as the Snowflake connector's, else `$SST_LOG_LEVEL`, "
        "else `info`. Independent of `--verbose`."
    ),
    "--no-color": (
        "No ANSI colour, else `$SST_NO_COLOR`; a non-empty `$NO_COLOR` does the same. Only `table` "
        "output on a terminal is ever coloured."
    ),
    "--allow-unsupported-manifest-schema": "Read a dbt manifest of an unsupported schema version, for this run only.",
    "--allow-stale-manifest": (
        "Accept a manifest SST did not write (`--manifest`, or `dbt.invoke: false`) that is older than "
        "the files it describes, for this run only."
    ),
    "--baseline": (
        "Read this baseline file of known warnings, which must exist, instead of the project's "
        "`.sst/baseline.json`, read when it exists."
    ),
    "--no-baseline": "Ignore the baseline for this run: every diagnostic is shown and blocks as declared.",
    "--show-baselined": "Show baselined diagnostics instead of counting them.",
    "--show-info": "Show info diagnostics instead of counting them; a cascade needs `--show-cascade`.",
    "--show-cascade": (
        "Show cascade diagnostics, follow-ons downgraded to info, instead of counting them. An "
        "override promoting a cascade's code reports it at that severity, as no longer a cascade."
    ),
    "--show-all-occurrences": "Show every occurrence of a code, not the first three of four or more.",
    "--select": (
        "Only these artifacts: a name (`*` and `?` globs), `type:<type>`, `path:<glob>`, `<type>:<name>`, "
        "or, where the command takes `--state`, `state:new`, `modified`, `unmodified` or `orphaned`."
    ),
    "--exclude": "Leave these artifacts out; same forms as `--select`.",
    "--state": "Previous run's build directory, else `$SST_STATE_DIR`, whose `manifest.json` `state:` selectors read.",
    "--defer-target": (
        "The `profiles.yml` target whose relations dbt objects resolve to, while publishing to `--target`; "
        "else `$SST_DEFER_TARGET`, else `defer.target`."
    ),
    "--no-defer": "Defer to no target, whatever `--defer-target`, `$SST_DEFER_TARGET` or `defer.target` say.",
    "--threads": (
        "Snowflake sessions to work on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`, "
        "else 4; 1 works serially. Output is the same for any count."
    ),
    "--database": "Read from this database instead of the target's; never where an artifact is published.",
    "--no-detailed-exitcode": "Exit 0 instead of 2 when there are differences.",
    "--strict": (
        "Promote every warning to an error; `--no-strict` promotes none. Else `$SST_STRICT`, else "
        "`validation.strict`, off when unset. A warning about an artifact the selection leaves out stays one."
    ),
    "--snowflake-syntax-check": (
        "Connects: compile expressions against Snowflake, and run each model `snowflake.orchestration_models` "
        "allows in a throwaway agent. Defaults to `validation.snowflake_syntax_check`, on when unset."
    ),
    "--prune": (
        "Also act on managed artifacts whose source was deleted, as far as each type "
        "allows: drop, deactivate, or report."
    ),
    "--partial": (
        "Go ahead with every artifact that has no errors and depends on nothing that does; "
        "still exits 1 while errors remain. Cannot be combined with `--prune`."
    ),
    "--sql-out": "Write each create's and update's statements into this directory instead of `target/sst/sql`.",
    "--fail-fast": "Stop at the first failure instead of continuing.",
    "--no-validate": (
        "Skip validation: no cycle check, connected check, agent reference lookup, or strict promotion. "
        "Only when `sst validate` "
        "already ran on the same tree; compile errors still stop the run. Refused with `--snowflake-syntax-check`."
    ),
    "--dbt": "Read the dbt models from this directory instead of `dbt_project.yml`'s `model-paths`.",
    "--semantic": "Read the semantic models from this directory instead of `project.semantic_models_dir`.",
}
_COMMAND_OPTION_HELP: Mapping[tuple[str, str], str] = {
    ("sst apply", "--fail-fast"): "Stop at the first failure instead of continuing. Defaults to `apply.fail_fast`.",
    ("sst apply", "--threads"): (
        "Sessions to plan and apply on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`. "
        "Planning defaults to 4; applying to `skills.+threads`, else 4."
    ),
    ("sst plan", "--threads"): (
        "Sessions to validate and observe on at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`, "
        "else 4; 1 reads serially. The plan is the same for any count."
    ),
    ("sst test", "--threads"): (
        "Smoke probes, and eval runs neither `evals.+concurrency` nor an eval's `concurrency` setting paces, "
        "run at once, 1 to 16, else `$SST_THREADS`, else `generation.threads`, else 1."
    ),
    ("sst compile", "--database"): "Resolve refs against this database instead of the target's.",
    ("sst compile", "--emit-agent-spec"): "Write each agent's rendered specification into this directory.",
    ("sst debug", "--no-connect"): "Report everything but the connection, offline.",
    ("sst debug", "--snowflake-signatures"): "Report how often Snowflake refusals went unrecognised, from the run log.",
    ("sst init", "--skip-prompts"): "Accept every default without prompting.",
    ("sst init", "--check-only"): "Report whether the setup is complete; create nothing.",
    ("sst clean", "--dry-run"): "List what would be removed; remove nothing.",
    ("sst docs", "--output-dir"): "Write the pages here instead of `docs/reference`.",
    ("sst list", "--long"): (
        "Also print each artifact's applied fingerprint and published version; `json`, `yaml` and `csv` "
        "also carry its source files."
    ),
    ("sst apply", "--plan"): "Apply this saved plan. It must still match the compiled project.",
    ("sst apply", "--yes"): "Apply without asking for confirmation.",
    ("sst apply", "--temporary"): (
        "Publish agents as temporary agents, dropped when the apply's session ends, and skip their "
        "evals; views, tools and skills still publish permanently. Refused for a production-like target."
    ),
    (
        "sst compile",
        "--emit-ddl",
    ): "Write each artifact's rendered payload into this directory, one folder per type, offline.",
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
        "replace would drop (SST-PLN013), and the tags on each semantic view it replaces, to report "
        "those the replace removes (SST-PLN039). On by default; `--no-grants` skips the reads."
    ),
    ("sst plan", "--capture-prior"): (
        "Read the current definition of each live object a planned artifact names, so a review can "
        "see what an update replaces and restore it: `GET_DDL` for a semantic view, search service, "
        "procedure or function (needs REFERENCES or OWNERSHIP); for an agent, which an update never "
        "replaces, the version it serves, its specification, and the `ALTER AGENT` statements that "
        "point back at it. A stage, skill, plugin, profile or eval has none. Shown by `--full` and as "
        "`prior_definition` in JSON; a read that fails is a warning (SST-PLN037), not a failed plan."
    ),
    ("sst plan", "--use-cached-state"): (
        "Plan from the observation an earlier `sst plan` recorded in `--state` instead of reading "
        "the target, without connecting; the plan says what the target held then, and its age is "
        "reported. Requires `--state`; refused with `--snowflake-syntax-check` or `--capture-prior`; composite "
        "artifacts are blocked."
    ),
    ("sst plan", "--full"): (
        "Also print, under each update, the properties it changes on the object, and the definition it "
        "replaces when `--capture-prior` read it."
    ),
    ("sst plan", "--names-only"): "Print the name of each changed artifact, one per line, and nothing else.",
    ("sst plan", "--no-detailed-exitcode"): "Exit 0 when changes are pending, instead of 2.",
    ("sst plan", "--state"): (
        "Previous run's build directory, else `$SST_STATE_DIR`: `state:` selectors compare with its "
        "`manifest.json`, and `--use-cached-state` plans from its `observation.<target>.json`."
    ),
    ("sst test", "--suite"): (
        "Repeatable. `golden` compares outputs with committed goldens offline; `smoke` probes deployed "
        "objects; `evals` runs agent evaluations. Defaults to every suite that applies."
    ),
    ("sst test", "--golden-dir"): (
        "Directory of the semantic view DDL goldens, taken from the project directory when relative; the "
        "other types' goldens sit in directories beside it."
    ),
    ("sst test", "--update-golden"): (
        "Rewrite each golden the current output no longer equals, and create missing ones; runs the "
        "golden suite only. Refused with `--suite smoke` or `evals`, and when `$CI` is set to anything but "
        "empty, `0`, `false` or `no`."
    ),
    ("sst test", "--capture-baseline"): (
        "Record this eval run as each eval's new baseline instead of gating it on the stored one. Requires `--reason`, "
        "and a run of the eval suite: refused with `--update-golden`, a `--suite` that does not name `evals`, or no "
        "`--suite` when the eval suite does not apply."
    ),
    ("sst test", "--reason"): "Why the baseline is changing; stored with it. Requires `--capture-baseline`.",
    ("sst test", "--fail-fast"): (
        "Stop the smoke or eval suite at its first failing probe or eval, running them one at a time, and "
        "run no suite after one that fails. The golden suite always compares every golden."
    ),
    ("sst validate", "--verify-schema"): "Connects: confirm each column a semantic view reads exists in the warehouse.",
    ("sst list", "--no-manifest"): "Compile the project's files in memory instead of reading the compiled manifest.",
    ("sst list", "--dbt"): (
        "With `--no-manifest`, read the dbt models from this directory instead of `dbt_project.yml`'s `model-paths`."
    ),
    ("sst list", "--semantic"): (
        "With `--no-manifest`, read the semantic models from this directory instead of `project.semantic_models_dir`."
    ),
    ("sst validate", "--show-info"): (
        "Show info diagnostics instead of counting them, among them which registered type owns each "
        "semantic-model file."
    ),
    ("sst baseline add", "--all-warnings"): "Baseline every current warning; prints the count and needs `--yes`.",
    ("sst baseline add", "--expires-in"): (
        "Days, 1 to 365, until a new baseline expires; an existing one keeps its date until `renew`."
    ),
    ("sst baseline add", "--note"): "Written into each entry; defaults to `pre-existing at adoption of <code>`.",
    ("sst baseline add", "--yes"): "Baseline every warning without asking; required off a terminal.",
    ("sst baseline add", "--select"): "Baseline only the diagnostics of these artifacts.",
    ("sst baseline add", "--exclude"): "Leave the diagnostics of these artifacts out.",
    ("sst baseline add", "--target"): (
        "Connects: baseline what `sst validate --target` finds there, recording the target in each "
        "entry. Never `$SST_TARGET`; offline without it."
    ),
    ("sst baseline prune", "--select"): "Prune only the entries of these artifacts.",
    ("sst baseline prune", "--exclude"): "Leave the entries of these artifacts as they are.",
    ("sst baseline prune", "--target"): (
        "Connects: also prune the entries a connected validate against this target recorded. "
        "Without it, connected entries are kept."
    ),
    ("sst baseline show", "--code"): "Show only the entries of this code.",
    ("sst baseline show", "--expired"): "Show the entries only once the baseline has expired.",
    ("sst baseline show", "--target"): "Show only the offline entries and those recorded against this target.",
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
        "Also remove apostrophes from synonyms and break apart Jinja delimiters in descriptions; "
        "sample values are kept as written."
    ),
    ("sst format", "--no-detailed-exitcode"): "With `--check`, exit 0 when files would change, instead of 2.",
    ("sst agent-kit install", "--agent"): (
        "Repeatable. Install for this coding agent: `cortex` (`.cortex/skills`) or `claude` "
        "(`.claude/skills`). Defaults to the one whose folder the project has."
    ),
    ("sst agent-kit install", "--only"): "Repeatable. Install only this kit skill; adds to those already installed.",
    ("sst agent-kit install", "--dry-run"): "List what would be written; write nothing.",
    (
        "sst agent-kit upgrade",
        "--agent",
    ): "Repeatable. Upgrade only this agent's skills; defaults to every agent installed.",
    ("sst agent-kit upgrade", "--force"): "Make every installed file match the kit, overwriting your changes.",
    ("sst agent-kit upgrade", "--dry-run"): "List what would change; write nothing.",
    ("sst agent-kit status", "--agent"): "Repeatable. Show only this agent; defaults to every agent installed.",
    ("sst agent-kit status", "--check"): "Exit 2 when a skill is outdated or missing.",
    (
        "sst agent-kit remove",
        "--agent",
    ): "Repeatable. Remove only this agent's skills; defaults to every agent installed.",
    ("sst agent-kit remove", "--only"): "Repeatable. Remove only this installed skill.",
    ("sst agent-kit remove", "--force"): "Also remove files you changed since SST wrote them.",
    ("sst agent-kit remove", "--dry-run"): "List what would be removed; remove nothing.",
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
            with click.Context(command) as command_ctx:
                arguments = tuple(
                    piece
                    for param in command.params
                    if isinstance(param, click.Argument)
                    for piece in param.get_usage_pieces(command_ctx)
                )
            documented.append(CommandDoc(path, description, option_docs(command), arguments=arguments))
    return tuple(documented)


def option_docs(command: click.Command) -> tuple[OptionDoc, ...]:
    """Document each visible option of `command`, in declaration order, as the CLI reference lists it.

    A flag takes no value; a choice lists its choices, a path says `DIRECTORY`, `FILE`, or
    `PATH`, and any other type gives its name. Only a string, number, or path default is shown.
    """
    documented: list[OptionDoc] = []
    for param in command.params:
        # The global options are documented once, in their own section, not under every command.
        if not isinstance(param, click.Option) or param.hidden or isinstance(param, GlobalOption):
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
