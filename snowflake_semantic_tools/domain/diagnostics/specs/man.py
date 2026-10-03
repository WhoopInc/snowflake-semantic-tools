"""Manifest and state codes (MAN): reading the SST manifest and the recorded state, and their schemas."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Manifest and state"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-MAN001",
        Severity.ERROR,
        "manifest.json not found where expected",
        "no SST manifest at {path}",
        "run sst compile",
        condition="the compiled manifest is absent",
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
        "SST-MAN004",
        Severity.ERROR,
        "Impact index incomplete",
        "{artifact} has no reverse-index entry",
        "re-run sst compile",
        condition="an artifact is absent from the impact index",
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
        "drop --manifest and let SST parse for this target",
        condition="the manifest and the run disagree about the target",
    ),
    spec(
        "SST-MAN007",
        Severity.ERROR,
        "Manifest write failed",
        "could not write {path}: {detail}",
        "check the filesystem permissions",
        condition="the manifest could not be persisted",
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
        "SST-MAN020",
        Severity.WARNING,
        "state.json absent",
        "no state file; treating every artifact as new",
        "check `state.+table` is readable on this target; a full plan is the safe fallback, not the fix",
        condition="change detection has no prior state",
    ),
    spec(
        "SST-MAN021",
        Severity.WARNING,
        "state.manifest_id does not match the current manifest",
        "state was recorded against manifest {found}; current is {expected}",
        "re-run plan; NOOP detection is disabled for this run",
        condition="the state and the manifest are from different compiles",
    ),
    spec(
        "SST-MAN022",
        Severity.ERROR,
        "state.json present but unreadable",
        "{path} is present and unreadable: {detail}",
        "fix or delete the file explicitly",
        condition=("refusing rather than silently discarding, because discarding is what makes a prune unsafe"),
    ),
    spec(
        "SST-MAN023",
        Severity.ERROR,
        "State file schema is unrecognised",
        "{path} declares schema {found}",
        "delete it and re-run plan",
        condition="the state file version is outside the supported set",
    ),
    spec(
        "SST-MAN024",
        Severity.WARNING,
        "State written by a newer SST",
        "{path} was written by SST {found}; this is {expected}",
        "upgrade SST, or delete the state file",
        condition="the state file is from a later version",
    ),
    spec(
        "SST-MAN025",
        Severity.ERROR,
        "Two writers produced one state filename",
        "{path} was written by {value}",
        "split the two files; one filename, one schema",
        condition="the 0.3 two-schemas-one-filename clobber is detected",
    ),
    spec(
        "SST-MAN026",
        Severity.INFO,
        "Change detection summary",
        "{value}",
        None,
        condition="per-type counts of new, modified, unmodified and orphaned",
    ),
    spec(
        "SST-MAN027",
        Severity.WARNING,
        "Local state cache disagrees with the state table",
        "local state.json for target {value} disagrees with {detail}; the table wins",
        "none -- the run proceeds from the table",
        condition=(
            "both the state table and the local state file are readable and their `applied` maps "
            "differ -- a switched branch, or an `apply` from another checkout"
        ),
        note=(
            "The table is authoritative and the file is a cache, so the table wins. A warning rather "
            "than an error because a correct answer is available; an error would stop a deploy over a "
            "stale local file."
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
        condition="the dbt manifest was rewritten while SST was reading it",
    ),
    spec(
        "SST-MAN201",
        Severity.INFO,
        "Older manifest schema migrated in memory",
        "{path} schema {found} migrated to {expected} in memory",
        None,
        condition="a read command migrated without rewriting the file",
    ),
    spec(
        "SST-MAN202",
        Severity.WARNING,
        "Older manifest schema with no migration",
        "{path} schema {found} has no migration; full recompile",
        "re-run sst compile",
        condition="an old schema cannot be migrated",
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
