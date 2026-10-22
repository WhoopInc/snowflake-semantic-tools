"""Manifest and state codes (MAN): reading the SST manifest and the recorded state, and their schemas."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Manifest and state"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-MAN001",
        Severity.ERROR,
        "manifest.json not found where expected",
        "no SST manifest at {path}",
        "run sst compile",
        condition=(
            "there is no manifest where it is read: `sst_target/manifest.json`, which "
            "`sst compile` writes, or the `manifest.json` of the `--state` directory"
        ),
    ),
    spec(
        "SST-MAN002",
        Severity.ERROR,
        "manifest.json is not valid JSON",
        "{path} is not a readable SST manifest: {detail}",
        "delete it and re-run sst compile",
        condition="the manifest fails to parse",
    ),
    spec(
        "SST-MAN003",
        Severity.ERROR,
        "Required manifest key missing",
        "{path} omits required key '{key}'",
        "re-run sst compile",
        condition="the manifest is structurally incomplete",
    ),
    spec(
        "SST-MAN005",
        Severity.ERROR,
        "manifest_id does not match the recomputed hash",
        "manifest_id {found}, recomputed {expected}",
        "re-run sst compile; the manifest is tampered or truncated",
        condition="the manifest content hash disagrees with its recorded id",
    ),
    spec(
        "SST-MAN006",
        Severity.ERROR,
        "Manifest written for a different target",
        "manifest target '{found}', current target '{expected}'",
        "re-run sst compile for this target",
        condition="the manifest `sst compile` wrote records another target than the run's",
        note="`--manifest` names the dbt manifest, not this one, so dropping it does not help.",
    ),
    spec(
        "SST-MAN007",
        Severity.ERROR,
        "Manifest write failed",
        "could not write {path}: {detail}",
        "check the filesystem permissions",
        condition="the manifest could not be persisted",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-MAN008",
        Severity.ERROR,
        "Compile failed before a manifest could be written",
        "compile failed: {detail}",
        "fix the reported errors, then re-compile",
        condition="compilation did not reach the write step",
    ),
    spec(
        "SST-MAN009",
        Severity.ERROR,
        "Compiled manifest is stale",
        "compiled SST manifest is stale; run sst compile before {command}",
        "run sst compile, then re-run the command",
        condition=(
            "the project no longer compiles to the manifest `sst compile` wrote, so plan, apply, or a "
            "connected test suite refuses to start from it; the command exits 4"
        ),
    ),
    spec(
        "SST-MAN020",
        Severity.WARNING,
        "state.json absent",
        "no state file; treating every artifact as new",
        "check `state.+table` is readable on this target; a full plan is the safe fallback, not the fix",
        condition="read offline, the target has no local state cache, so every artifact reads as new",
    ),
    spec(
        "SST-MAN022",
        Severity.ERROR,
        "state.json present but unreadable",
        "{path} is present and unreadable: {detail}",
        "fix or delete the file explicitly",
        condition=(
            "the local state cache, `sst_target/state.<target>.json`, exists but cannot be read; "
            "`sst diff` also reports it for a state table that exists and cannot be read"
        ),
        note="SST refuses rather than discard the file, because discarding it is what makes a prune unsafe.",
    ),
    spec(
        "SST-MAN023",
        Severity.ERROR,
        "State file schema is unrecognised",
        "{path} declares schema {found}",
        "delete it and re-run plan",
        condition="the local state cache declares a schema version this release does not read or migrate",
    ),
    spec(
        "SST-MAN024",
        Severity.WARNING,
        "State written by a newer SST",
        "{path} was written by SST {found}; this is {expected}",
        "upgrade SST, or delete the state file",
        condition="the local state cache was written by a later SST release",
    ),
    spec(
        "SST-MAN025",
        Severity.ERROR,
        "Two writers produced one state filename",
        "{path} was written by {value}",
        "split the two files; one filename, one schema",
        condition=(
            "the local state cache under this target's file name records another database or "
            "schema, so it is set aside, neither read nor rewritten; or the observation "
            "`--use-cached-state` reads records another account, database or schema"
        ),
    ),
    spec(
        "SST-MAN026",
        Severity.INFO,
        "Change detection summary",
        "{value}",
        None,
        condition=("a plan reports, per artifact type, its counts of new, modified, unmodified and orphaned artifacts"),
    ),
    spec(
        "SST-MAN027",
        Severity.INFO,
        "Local state cache disagrees with the state table",
        "local state.json for target {value} disagrees with {detail}; the table wins",
        "none -- the run proceeds from the table",
        condition=(
            "both the state table and the local state file are readable and their `applied` maps "
            "differ -- a switched branch, or an `apply` from another checkout"
        ),
        note=(
            "The table is authoritative and the file is a cache, so the table wins and the file is "
            "rewritten. Info rather than a warning because nothing is wrong and nothing is to be done: "
            "strict mode promotes every warning, and would stop a deploy over a stale local file."
        ),
    ),
    spec(
        "SST-MAN030",
        Severity.WARNING,
        "Cached Snowflake observation past its TTL",
        "observation for {value} is {detail} old; ignored",
        "no action; the observation was refreshed",
        condition=("apply took up a saved plan whose observation is more than 1 hour old, and planned afresh instead"),
    ),
    spec(
        "SST-MAN031",
        Severity.ERROR,
        "dbt manifest digest changed mid-run",
        "dbt manifest digest changed during the run",
        "re-run; a concurrent dbt compile is in progress",
        condition="the dbt manifest `--manifest` names changed its models while the project compiled",
    ),
    spec(
        "SST-MAN032",
        Severity.ERROR,
        "Comparison report cannot be refreshed",
        "{path} cannot have its cost refreshed: {detail}",
        "pass a compare.json that `sst test --suite evals --compare-models` wrote, as it wrote it; "
        "compare again for a new one",
        condition=(
            "`sst test --refresh-cost` was given a file that is not there, is not JSON, or is not an "
            "`sst.compare/1` report with every key it writes"
        ),
    ),
    spec(
        "SST-MAN201",
        Severity.INFO,
        "Older manifest schema migrated in memory",
        "{path} schema {found} migrated to {expected} in memory",
        None,
        condition="a stored manifest of an older schema was read, migrated in memory without rewriting the file",
    ),
    spec(
        "SST-MAN202",
        Severity.WARNING,
        "Older manifest schema with no migration",
        "{path} schema {found} has no migration, so it cannot be read",
        "re-run sst compile",
        condition="a stored manifest declares an older schema that has no migration",
        note="The manifest cannot be read, so the command stops; `sst compile` writes a current one.",
    ),
    spec(
        "SST-MAN203",
        Severity.ERROR,
        "Manifest schema newer than this binary supports",
        "{path} schema {found}; this binary supports {expected}",
        "upgrade SST",
        condition="a newer manifest may express types this binary cannot render",
    ),
)
