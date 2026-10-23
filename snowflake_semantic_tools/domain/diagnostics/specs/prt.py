"""External-system codes (PRT): a failure at the boundary with an external system.

The Snowflake connection, a transient failure, a refused privilege, a missing object, a file SST
cannot read, refuses to read, or cannot write, and a command-line invocation SST does not accept.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

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
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT002",
        Severity.ERROR,
        "Authentication failed",
        "authentication failed for {value}: {detail}",
        "refresh the credential",
        condition="Snowflake rejects the session's credential, while connecting or later in the run",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT003",
        Severity.ERROR,
        "Query timed out",
        "query timed out after {detail}",
        "retry; this is retryable",
        condition="a statement times out or is canceled, or the connection drops while it runs",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT004",
        Severity.ERROR,
        "Insufficient privilege",
        "{value} lacks {detail}",
        "grant the privilege",
        condition="Snowflake refuses a statement for want of a privilege",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT005",
        Severity.ERROR,
        "Object not found",
        "Snowflake object {value} was not found: {detail}",
        "publish it, or correct the name",
        condition="an object a command reads or drops does not exist, such as a table a semantic view reads",
        precheck=Precheck.RUNTIME,
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
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT009",
        Severity.ERROR,
        "Filesystem read failed",
        "could not read {path}: {detail}",
        "remove the link, or use a path inside the project; otherwise check that the file exists and can be read",
        condition=(
            "a file SST reads cannot be read or does not hold what it should (a manifest, baseline, "
            "saved plan, or `--state` observation), such as a baseline whose `expires_on` is not a "
            "`YYYY-MM-DD` date; a file or folder to be read is a symbolic link "
            "or is reached through one, every `file()` sidecar among them; or dbt's "
            "`target-path` or the semantic-models directory resolves outside the project"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-PRT010",
        Severity.ERROR,
        "Path could not be removed",
        "could not remove {path}: {detail}",
        "remove it by hand",
        condition="`sst clean` cannot remove the build directory, or it is reached through a link",
        precheck=Precheck.RUNTIME,
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
        "SST-PRT013",
        Severity.ERROR,
        "Agent kit path not owned by SST",
        "{path}: {detail}",
        "move or rename it and run the command again; SST never overwrites a file it did not write",
        condition=(
            "`sst agent-kit` would write a skill folder or file that exists, that its lock does "
            "not record, and that does not hold the kit's bytes exactly"
        ),
        note="nothing is written: the whole run is refused",
    ),
    spec(
        "SST-PRT014",
        Severity.WARNING,
        "Edited agent kit file kept",
        "{path} changed since SST wrote it, so it is kept: {detail}",
        "compare it with the kit and keep your change, or run the command again with --force to take the kit's version",
        condition=(
            "`sst agent-kit upgrade` or `remove` would change or remove a file that differs from "
            "what SST last wrote, and --force is not given"
        ),
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
        "use {expected}",
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
        "Snowflake until it ends or is canceled there",
        condition=(
            "the user interrupts a run, with Ctrl-C, SIGTERM or SIGHUP, or declines a confirmation "
            "prompt; the run exits 130. An interrupted `apply` starts no further change, records in "
            "state the changes that finished, and releases its lock; after an eval run started, the "
            "message names each run, which goes on in Snowflake"
        ),
        note=(
            "Every signal is handled as Ctrl-C is, so a CI cancellation, `timeout`, or `docker stop` "
            "ends the run cleanly; a signal the process inherited as ignored, as `nohup` leaves "
            "SIGHUP, stays ignored. A further SIGTERM or SIGHUP is ignored while it cleans up, and a "
            "second Ctrl-C stops apply's running changes at their next statement. SIGKILL cannot be "
            "handled: an apply it ends leaves its lock to expire (`--break-stale-lock` then takes it "
            "over). An object it updated but did not record still carries SST's marker for the new "
            "definition, so the next apply updates it again, and records it; one it created, which "
            "state never recorded, is refused as SST-PLN024 until it is dropped by hand."
        ),
        precheck=Precheck.RUNTIME,
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
        "re-run with --yes; sst apply and sst baseline add --all-warnings ask instead only when stdin and "
        "stderr are both terminals and the output is not JSON or agent",
        condition=(
            "a command that needs confirmation runs without --yes: `sst drop`, `apply --prune`, "
            "`sst state unlock`, `--output json` or `agent` with `apply` or `baseline add --all-warnings`, "
            "`state convert` or `state recover` off a terminal, or either of those two "
            "when stdin or stderr is not a terminal, so no question could be seen and answered"
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
    spec(
        "SST-PRT111",
        Severity.ERROR,
        "Emitted files would collide",
        "{path} would hold the payload of both {a} and {b}",
        "rename one of the two artifacts; names that differ only in case name one file",
        condition=(
            "two artifacts of one type would write the same `sst compile --emit-ddl` or `--emit-agent-spec` "
            "file, as names that differ only in case do; nothing is emitted, though the manifest is written"
        ),
    ),
    spec(
        "SST-PRT112",
        Severity.ERROR,
        "Lineage names no node",
        "no lineage node is named '{value}'",
        "name an artifact, a member such as metric:<name>, model:<name> or source:<source>.<table>{detail}",
        condition=(
            "`sst lineage NODE` names nothing the lineage graph holds: no artifact, member, dbt model, "
            "source or exposure of that key or name"
        ),
    ),
)
