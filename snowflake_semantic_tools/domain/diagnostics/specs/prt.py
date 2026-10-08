"""External-system codes (PRT): a failure at the boundary with an external system.

The Snowflake connection, a transient failure, a refused privilege, a missing object, a file SST
cannot read, refuses to read, or cannot write, and a command-line invocation SST does not accept.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "External systems"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-PRT001",
        Severity.ERROR,
        "Snowflake connection failed",
        "could not connect to {value}: {detail}",
        "check the account, the network and the credential",
        condition=(
            "connecting to Snowflake fails for a reason other than a rejected credential, or a "
            "Snowflake error matches no more specific code"
        ),
    ),
    spec(
        "SST-PRT002",
        Severity.ERROR,
        "Authentication failed",
        "authentication failed for {value}",
        "refresh the credential",
        condition="Snowflake rejects the session's credential while connecting",
    ),
    spec(
        "SST-PRT003",
        Severity.ERROR,
        "Query timed out",
        "query timed out after {detail}",
        "retry; this is retryable",
        condition="a statement times out or is cancelled, or the connection drops while it runs",
    ),
    spec(
        "SST-PRT004",
        Severity.ERROR,
        "Insufficient privilege",
        "{value} lacks {detail}",
        "grant the privilege",
        condition="Snowflake refuses a statement for want of a privilege",
    ),
    spec(
        "SST-PRT005",
        Severity.ERROR,
        "Object not found",
        "Snowflake object {value} was not found: {detail}",
        "publish it, or correct the name",
        condition="an object a command reads or drops does not exist, such as a table a semantic view reads",
    ),
    spec(
        "SST-PRT006",
        Severity.ERROR,
        "dbt manifest not found",
        "no dbt manifest at {path}",
        "run dbt compile, or set the manifest path",
        condition="no dbt manifest exists at the path SST reads it from",
    ),
    spec(
        "SST-PRT008",
        Severity.ERROR,
        "Filesystem write failed",
        "could not write {path}: {detail}",
        "check the permissions and the free space",
        condition="a file SST writes cannot be written",
    ),
    spec(
        "SST-PRT009",
        Severity.ERROR,
        "Filesystem read failed",
        "could not read {path}: {detail}",
        "check the permissions",
        condition=(
            "a file SST reads cannot be read or does not hold what it should (a manifest, baseline, "
            "saved plan, or `--state` observation); a file or folder to be read is a symbolic link "
            "or is reached through one; or dbt's `target-path`, the semantic-models directory, a "
            "verified query's `sql_file`, or a tool's `body_file` resolves outside the project"
        ),
    ),
    spec(
        "SST-PRT010",
        Severity.ERROR,
        "Path could not be removed",
        "could not remove {path}: {detail}",
        "remove it by hand",
        condition="`sst clean` cannot remove the build directory, or it is reached through a link",
    ),
    spec(
        "SST-PRT011",
        Severity.ERROR,
        "Credential resolved to an empty value",
        "{value} resolved to an empty credential",
        "set the environment variable, or the profile field",
        condition="a credential field written for the selected `profiles.yml` target renders empty",
    ),
    spec(
        "SST-PRT012",
        Severity.ERROR,
        "Secret would be rendered in plain text",
        "{value} would be rendered verbatim",
        "the output was withheld; remove the credential's value from the project text it appears in, "
        "or report this as a bug",
        condition="a resolved credential reaches command output unredacted",
    ),
    spec(
        "SST-PRT100",
        Severity.ERROR,
        "Invalid invocation",
        "{detail}",
        "see sst <command> --help",
        condition=(
            "a flag, a value, or a combination is not accepted, such as an unknown code for "
            "`sst explain` or a PATH `sst format` finds no YAML file at"
        ),
    ),
    spec(
        "SST-PRT101",
        Severity.ERROR,
        "Comma in a selector",
        "selector '{value}' contains a comma",
        "use space-separated selectors; intersection is not supported",
        condition="a comma appears in --select or --exclude",
    ),
    spec(
        "SST-PRT102",
        Severity.ERROR,
        "Unknown selector kind",
        "selector '{value}' names an unknown kind",
        "use one of {expected}",
        condition="a selector's prefix is not a registered kind, or globs a kind whose values are a fixed set",
    ),
    spec(
        "SST-PRT103",
        Severity.ERROR,
        "Selector requires state that was not supplied",
        "selector '{value}' requires --state",
        "pass --state to sst plan or sst apply; no other command reads a state: selector",
        condition="a `state:` selector is given without `--state`, or to a command that takes no `--state`",
    ),
    spec(
        "SST-PRT104",
        Severity.ERROR,
        "Mutually exclusive flags",
        "{a} and {b} cannot be combined",
        "pass one of them",
        condition="two flags that exclude each other were both given",
    ),
    spec(
        "SST-PRT105",
        Severity.ERROR,
        "Invalid source or model selector",
        "'{value}' is not a valid {detail} selector",
        "use the documented form",
        condition="an `sst enrich` selector names a directory, a kind other than a model, or nothing",
    ),
    spec(
        "SST-PRT106",
        Severity.ERROR,
        "Output format is not supported by this command",
        "'{found}' is not a supported format for {command}; supported: {expected}",
        "pass one of {expected}",
        condition="`--output` names a format the command does not support",
    ),
    spec(
        "SST-PRT107",
        Severity.INFO,
        "Run interrupted",
        "interrupted after {detail}; {value}",
        "after apply, run sst plan to see what remains; an eval run already started keeps running in "
        "Snowflake until it ends or is cancelled there",
        condition=(
            "the user interrupts a run, or declines a confirmation prompt; the run exits 130. After "
            "`apply`, partial work may have been applied; after an eval run started, the message "
            "names each run, which goes on in Snowflake"
        ),
    ),
    spec(
        "SST-PRT108",
        Severity.ERROR,
        "Object name is not fully qualified",
        "{command} requires a fully-qualified name; '{value}' is not one",
        "pass <database>.<schema>.<object>",
        condition="the object `sst drop` names is not a three-part name",
    ),
    spec(
        "SST-PRT109",
        Severity.ERROR,
        "Mandatory confirmation flag absent",
        "{command} requires --yes",
        "re-run with --yes; only sst baseline add --all-warnings asks instead, on a terminal and without JSON output",
        condition=(
            "a command that needs confirmation runs without --yes: `sst drop`, an `apply` that needs "
            "it, or `baseline add --all-warnings` with no terminal to prompt on or under JSON output"
        ),
    ),
    spec(
        "SST-PRT110",
        Severity.ERROR,
        "Selector passed to a command that has none",
        "{command} takes no selector; '{value}' is not accepted",
        "name one object, or use apply --prune for set-based removal",
        condition=(
            "--select or --exclude is passed to a command that takes no selector, or `sst drop` is "
            "given a pattern rather than one name"
        ),
    ),
)
