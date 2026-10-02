"""Snowflake codes (SNO): an error Snowflake returned for a statement, classified."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Snowflake"

SPECS: tuple[ErrorSpec, ...] = (
    spec("SST-SNO001", Severity.ERROR, "Unrecognised Snowflake refusal", "Snowflake refused: {detail}", None),
    spec("SST-SNO002", Severity.ERROR, "Object already exists", "{value} already exists", "choose another name"),
    spec(
        "SST-SNO003",
        Severity.ERROR,
        "Object does not exist or is not authorised",
        "{value} does not exist or is not authorised",
        "publish the object or grant access",
    ),
    spec(
        "SST-SNO004",
        Severity.ERROR,
        "Insufficient privileges",
        "insufficient privileges for {value}",
        "grant the privilege to the deploying role",
    ),
    spec("SST-SNO009", Severity.ERROR, "SQL compilation error", "SQL compilation error: {detail}", "fix the statement"),
    spec(
        "SST-SNO022",
        Severity.ERROR,
        "Concurrent DDL or lock timeout",
        "lock timeout on {value}",
        "retry or serialise publishers",
    ),
    spec(
        "SST-SNO030",
        Severity.ERROR,
        "Relation is missing or not visible",
        "model '{model}': {relation} does not exist, or the role cannot see it",
        "build the model in this target, or grant the role access to it",
    ),
    spec(
        "SST-SNO031",
        Severity.ERROR,
        "Enrichment step failed",
        "model '{model}': {step} failed: {detail}",
        "fix the cause the message names, then run sst enrich again",
    ),
)
