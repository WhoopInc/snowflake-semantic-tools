"""External-system codes (PRT): a failure at the boundary with an external system.

The Snowflake connection, a transient failure, a refused privilege, a missing object, or a
file SST refuses to read.
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
    ),
    spec(
        "SST-PRT002",
        Severity.ERROR,
        "Authentication failed",
        "authentication failed for {value}",
        "refresh the credential",
    ),
    spec(
        "SST-PRT003",
        Severity.ERROR,
        "Query timed out",
        "query timed out after {detail}",
        "retry; this is retryable",
    ),
    spec(
        "SST-PRT004",
        Severity.ERROR,
        "Insufficient privilege",
        "{value} lacks {detail}",
        "grant the privilege",
    ),
    spec(
        "SST-PRT005",
        Severity.ERROR,
        "Object not found",
        "Snowflake object {value} was not found: {detail}",
        "publish it, or correct the name",
    ),
    spec(
        "SST-PRT006",
        Severity.ERROR,
        "dbt manifest not found",
        "no dbt manifest at {path}",
        "run dbt parse, or pass --manifest with the path of an existing manifest",
    ),
    spec(
        "SST-PRT008",
        Severity.ERROR,
        "Filesystem write failed",
        "could not write {path}: {detail}",
        "check the permissions and the free space",
    ),
    spec(
        "SST-PRT009",
        Severity.ERROR,
        "Filesystem read failed",
        "could not read {path}: {detail}",
        "SST reads only regular files inside the project: replace a symbolic link with the file or folder "
        "it points to, and keep dbt's target-path inside the project",
    ),
    spec(
        "SST-PRT010",
        Severity.ERROR,
        "Path could not be removed",
        "could not remove {path}: {detail}",
        "remove it by hand",
    ),
    spec(
        "SST-PRT011",
        Severity.ERROR,
        "Credential resolved to an empty value",
        "{value} resolved to an empty credential",
        "set the environment variable, or the profile field",
    ),
    spec(
        "SST-PRT012",
        Severity.ERROR,
        "Secret would be rendered in plain text",
        "{value} would be rendered verbatim",
        "the output was withheld; report this as a bug, since SST never prints a credential",
    ),
    spec(
        "SST-PRT100",
        Severity.ERROR,
        "Invalid invocation",
        "{detail}",
        "see sst --help, and the command's own --help",
    ),
    spec(
        "SST-PRT101",
        Severity.ERROR,
        "Comma in a selector",
        "selector '{value}' contains a comma",
        "use space-separated selectors; intersection is not supported",
    ),
    spec(
        "SST-PRT102",
        Severity.ERROR,
        "Unknown selector kind",
        "selector '{value}' names an unknown kind",
        "use a name, type:, path:, state:, or <type>:<name>; only a name takes * and ? globs",
    ),
    spec(
        "SST-PRT103",
        Severity.ERROR,
        "Selector requires state that was not supplied",
        "selector '{value}' requires --state",
        "pass --state",
    ),
    spec(
        "SST-PRT104",
        Severity.ERROR,
        "Mutually exclusive flags",
        "{a} and {b} cannot be combined",
        "pass one of them",
    ),
    spec(
        "SST-PRT105",
        Severity.ERROR,
        "Invalid source or model selector",
        "'{value}' is not a valid {detail} selector",
        "use the documented form: model:<name>, or a model name",
    ),
    spec(
        "SST-PRT106",
        Severity.ERROR,
        "Output format is not supported by this command",
        "'{found}' is not a supported format for {command}; supported: {expected}",
        "pass one of the supported formats",
    ),
    spec(
        "SST-PRT107",
        Severity.INFO,
        "Run interrupted",
        "interrupted after {detail}; {value}",
        None,
    ),
    spec(
        "SST-PRT109",
        Severity.ERROR,
        "Mandatory confirmation flag absent",
        "{command} requires --yes",
        "re-run with --yes; there is no interactive prompt",
    ),
    spec(
        "SST-PRT110",
        Severity.ERROR,
        "Selector passed to a command that has none",
        "{command} takes no selector; '{value}' is not accepted",
        "remove the selector; this command takes none, and set-based removal is apply --prune",
    ),
)
