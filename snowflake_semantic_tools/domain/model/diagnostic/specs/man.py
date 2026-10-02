"""Manifest and state codes (MAN): reading the SST manifest and the recorded state, and their schemas."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Manifest and state"

SPECS: tuple[ErrorSpec, ...] = (
    spec("SST-MAN001", Severity.ERROR, "Manifest missing", "no SST manifest at {path}", "run sst compile"),
    spec(
        "SST-MAN002",
        Severity.ERROR,
        "Manifest is unreadable",
        "{path} is not a readable SST manifest: {detail}",
        "delete it and re-run sst compile",
    ),
    spec(
        "SST-MAN003",
        Severity.ERROR,
        "Manifest key missing",
        "{path} omits required key '{key}'",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN005",
        Severity.ERROR,
        "Manifest content hash mismatch",
        "manifest_id {found}, recomputed {expected}",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN020",
        Severity.WARNING,
        "State is absent",
        "no state file; treating every artifact as new",
        "check that the state table is readable",
    ),
    spec(
        "SST-MAN021",
        Severity.WARNING,
        "State and manifest differ",
        "state was recorded against manifest {found}; current is {expected}",
        "re-run plan; NOOP detection is disabled",
    ),
    spec(
        "SST-MAN022",
        Severity.ERROR,
        "State is unreadable",
        "{path} is present and unreadable: {detail}",
        "fix or explicitly delete the file",
    ),
    spec(
        "SST-MAN023",
        Severity.ERROR,
        "State schema unsupported",
        "{path} declares schema {found}",
        "upgrade SST or explicitly clear the state",
    ),
    spec(
        "SST-MAN027",
        Severity.WARNING,
        "State cache disagrees with remote state",
        "local state.json for target {value} disagrees with {detail}; the table wins",
        "the run proceeds from authoritative remote state",
    ),
    spec(
        "SST-MAN202",
        Severity.ERROR,
        "Manifest schema has no migration",
        "{path} schema {found} has no migration",
        "re-run sst compile",
    ),
    spec(
        "SST-MAN203",
        Severity.ERROR,
        "Manifest schema is newer",
        "{path} schema {found}; this binary supports {expected}",
        "upgrade SST",
    ),
)
