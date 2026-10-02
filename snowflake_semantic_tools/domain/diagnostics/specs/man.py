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
    ),
    spec(
        "SST-MAN002",
        Severity.ERROR,
        "manifest.json is not valid JSON",
        "{path} is not a readable SST manifest: {detail}",
        "delete it and re-run sst compile",
    ),
    spec(
        "SST-MAN003",
        Severity.ERROR,
        "Required manifest key missing",
        "{path} omits required key '{key}'",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN004",
        Severity.ERROR,
        "Impact index incomplete",
        "{artifact} has no reverse-index entry",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN005",
        Severity.ERROR,
        "manifest_id does not match the recomputed hash",
        "manifest_id {found}, recomputed {expected}",
        "re-run sst compile; the manifest is tampered or truncated",
    ),
    spec(
        "SST-MAN006",
        Severity.ERROR,
        "Manifest written for a different target",
        "manifest target '{found}', current target '{expected}'",
        "re-run sst compile for this target",
    ),
    spec(
        "SST-MAN007",
        Severity.ERROR,
        "Manifest write failed",
        "could not write {path}: {detail}",
        "check the filesystem permissions",
    ),
    spec(
        "SST-MAN008",
        Severity.ERROR,
        "Compile failed before a manifest could be written",
        "compile failed: {detail}",
        "fix the reported errors, then re-compile",
    ),
    spec(
        "SST-MAN020",
        Severity.WARNING,
        "state.json absent",
        "no state file; treating every artifact as new",
        "check that the state table is readable on this target; a full plan is the safe fallback, not the fix",
    ),
    spec(
        "SST-MAN021",
        Severity.WARNING,
        "state.manifest_id does not match the current manifest",
        "state was recorded against manifest {found}; current is {expected}",
        "re-run plan; NOOP detection is disabled for this run",
    ),
    spec(
        "SST-MAN022",
        Severity.ERROR,
        "state.json present but unreadable",
        "{path} is present and unreadable: {detail}",
        "fix or delete the file explicitly",
    ),
    spec(
        "SST-MAN023",
        Severity.ERROR,
        "State file schema is unrecognised",
        "{path} declares schema {found}",
        "delete it and re-run plan",
    ),
    spec(
        "SST-MAN024",
        Severity.WARNING,
        "State written by a newer SST",
        "{path} was written by SST {found}; this is {expected}",
        "upgrade SST, or delete the state file",
    ),
    spec(
        "SST-MAN025",
        Severity.ERROR,
        "Two writers produced one state filename",
        "{path} was written by {value}",
        "split the two files; one filename, one schema",
    ),
    spec(
        "SST-MAN026",
        Severity.INFO,
        "Change detection summary",
        "{value}",
        None,
    ),
    spec(
        "SST-MAN027",
        Severity.WARNING,
        "Local state cache disagrees with the state table",
        "local state.json for target {value} disagrees with {detail}; the table wins",
        "none; the run proceeds from the table",
    ),
    spec(
        "SST-MAN030",
        Severity.WARNING,
        "Cached Snowflake observation past its TTL",
        "observation for {value} is {detail} old; ignored",
        "no action; the observation was refreshed",
    ),
    spec(
        "SST-MAN031",
        Severity.ERROR,
        "dbt manifest digest changed mid-run",
        "dbt manifest digest changed during the run",
        "re-run; a concurrent dbt compile is in progress",
    ),
    spec(
        "SST-MAN201",
        Severity.INFO,
        "Older manifest schema migrated in memory",
        "{path} schema {found} migrated to {expected} in memory",
        None,
    ),
    spec(
        "SST-MAN202",
        Severity.WARNING,
        "Older manifest schema with no migration",
        "{path} schema {found} has no migration; full recompile",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN203",
        Severity.ERROR,
        "Manifest schema newer than this binary supports",
        "{path} schema {found}; this binary supports {expected}",
        "upgrade SST",
    ),
)
