"""Snowflake codes (SNO): an error Snowflake returned for a statement, classified by its signature."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Snowflake"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-SNO001",
        Severity.ERROR,
        "Snowflake refused and SST does not recognise the refusal",
        "Snowflake refused: {detail}",
        "read internal_detail; if this recurs, add a signature",
        condition=(
            "Snowflake refused a statement and no SST signature matches the error; also an eval "
            "run whose results could not be read or checked, or an eval the gate finds no "
            "completed, read attempt for"
        ),
        note=(
            "The detail is Snowflake's error, or for an eval what could not be read. When the "
            "failure ends a command's own connection, the command reports SST-PRT001 instead."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO002",
        Severity.ERROR,
        "Object already exists",
        "{value} already exists",
        "choose another name, or -- for a semantic view -- let the unconditional `CREATE OR REPLACE` handle it.",
        condition="Snowflake reported error 2002 (SQLSTATE 42710), or a message saying `already exists`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO003",
        Severity.ERROR,
        "Object does not exist or is not authorised",
        "{value} does not exist or is not authorised",
        "publish the object, or grant access",
        condition=(
            "Snowflake reported error 2003 (SQLSTATE 02000), SQLSTATE 42S02, or a message saying "
            "`does not exist` that names no schema, database or warehouse"
        ),
        note="Plan reports a missing referenced relation first, as SST-PLN007.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO004",
        Severity.ERROR,
        "Insufficient privileges",
        "insufficient privileges for {value}",
        "grant the privilege to the deploying role",
        condition=(
            "Snowflake reported error 3001 (SQLSTATE 42501), SQLSTATE 28000, or a message saying "
            "`insufficient privileges` or `not authorized`"
        ),
        note="Plan reports a missing CREATE privilege on a schema first, as SST-PLN008.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO005",
        Severity.ERROR,
        "Schema does not exist or is not authorised",
        "schema {value} does not exist or is not authorised",
        "create the schema, or correct the target",
        condition="Snowflake reported error 2043, or a message saying `schema '<name>' does not exist`",
        note="Plan reports a missing schema first, as SST-PLN010.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO006",
        Severity.ERROR,
        "Database does not exist or is not authorised",
        "database {value} does not exist or is not authorised",
        "create the database, or correct the target",
        condition="Snowflake reported a message saying `database '<name>' does not exist`",
        note="Plan reports a missing database first, as SST-PLN011.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO007",
        Severity.ERROR,
        "Warehouse does not exist or is not authorised",
        "warehouse {value} does not exist or is not authorised",
        "grant USAGE, or name an existing warehouse",
        condition="Snowflake reported a message saying `warehouse '<name>' does not exist`",
        note="Plan reports an unusable warehouse first, as SST-PLN012.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO008",
        Severity.ERROR,
        "No active warehouse in the session",
        "no active warehouse in the session",
        "set a default warehouse for the connecting user",
        condition="Snowflake reported a message saying `no active warehouse`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO009",
        Severity.ERROR,
        "SQL compilation error",
        "SQL compilation error: {detail}",
        "fix the statement",
        condition=(
            "Snowflake reported error 1003 (SQLSTATE 42000), SQLSTATE 42601, or a message saying "
            "`syntax error` or `SQL compilation error`"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO010",
        Severity.ERROR,
        "SQL execution internal error",
        "SQL execution internal error: {detail}",
        "this is the query-time class the derived-metric restrictions exist to prevent",
        condition="Snowflake reported `SQL execution internal error`, or a message carrying an incident number",
        note=(
            "Snowflake failed while running a statement it had accepted. If it recurs, the "
            "incident number in the detail is what Snowflake support asks for."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO011",
        Severity.ERROR,
        "Statement timeout",
        "statement timed out after {detail}",
        "raise the timeout, or reduce the statement",
        condition="Snowflake reported error 630 (SQLSTATE 57014), or a message saying `statement or warehouse timeout`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO012",
        Severity.ERROR,
        "Secure-object share restriction",
        "{value}: share restriction",
        "keep the object secure, or revoke the share grant",
        condition="Snowflake reported error 93932, or a message naming both a secure object and a share",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO013",
        Severity.ERROR,
        "Authentication failed",
        "authentication failed for {value}",
        "refresh the credential",
        condition=(
            "Snowflake rejected a login: error 250001 with authentication wording, or error "
            "390100, 390144, 390195, 390302, 390303, 390318 or 390422"
        ),
        note="When a command's own connection fails this way, the command reports SST-PRT001.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO014",
        Severity.ERROR,
        "Driver network failure",
        "network failure: {detail}",
        "retry",
        condition=(
            "the driver lost or could not open its connection: SQLSTATE 08000, 08003 (error "
            "250002), 08006, or 08001 with timeout or connection wording"
        ),
        note="When a command's own connection fails this way, the command reports SST-PRT001.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO015",
        Severity.ERROR,
        "max_staleness rejected",
        "max_staleness {found} rejected",
        "raise it to at least 120",
        condition="Snowflake rejected a statement with a message naming `max_staleness`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO016",
        Severity.ERROR,
        "Semantic view feature not enabled",
        "semantic views are not enabled on this account",
        "ask an account administrator to enable the feature",
        condition=("Snowflake reported semantic views as an unsupported feature, or as not enabled or not available"),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO017",
        Severity.ERROR,
        "CREATE AGENT privilege required",
        "CREATE AGENT required on {value}",
        "grant CREATE AGENT on the schema",
        condition="Snowflake reported a message naming CREATE AGENT as a missing or required privilege",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO018",
        Severity.ERROR,
        "CREATE DATASET privilege required",
        "CREATE DATASET required on {value}",
        "grant CREATE DATASET on the schema",
        condition="Snowflake reported a message naming CREATE DATASET as a missing or required privilege",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO019",
        Severity.ERROR,
        "Duplicate synonym",
        "duplicate synonym {value}",
        "make synonyms unique within the view",
        condition="Snowflake reported a message saying `duplicate synonym`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO020",
        Severity.ERROR,
        "Identifier too long",
        "identifier {value} is too long",
        "shorten the name",
        condition=("Snowflake reported an identifier as too long, or a message saying it exceeds the maximum length"),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO022",
        Severity.ERROR,
        "Concurrent DDL or lock timeout",
        "lock timeout on {value}",
        "retry, or serialise the publishers",
        condition="Snowflake reported a lock timeout or wait, a deadlock, or a concurrent change",
        note="Plan warns of an object another session holds a lock on as SST-PLN019.",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO023",
        Severity.ERROR,
        "Result set too large",
        "result set too large for {value}",
        "narrow the query",
        condition="Snowflake reported a result as too large to return",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO024",
        Severity.ERROR,
        "Query queued beyond the wait limit",
        "query queued beyond {detail}",
        "raise the queue limit, or use a larger warehouse",
        condition=("Snowflake reported a statement queued beyond its limit, or a `statement_queued_timeout`"),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO025",
        Severity.ERROR,
        "Unexpected DESCRIBE output shape",
        "DESCRIBE {value} returned an unexpected shape",
        "report this as a Snowflake drift finding",
        condition="a DESCRIBE returned rows that lack a column SST reads, such as a table's `type`",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO030",
        Severity.ERROR,
        "Relation is missing or not visible",
        "model '{model}': {relation} does not exist, or the role cannot see it",
        "build the model in this target, or grant the role access to it",
        condition="during enrich, a model's relation does not exist or the role cannot see it",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-SNO031",
        Severity.ERROR,
        "Enrichment step failed",
        "model '{model}': {step} failed: {detail}",
        "fix the cause the message names, then run sst enrich again",
        condition=(
            "an enrich step failed for a model: reading its columns or values, or a Cortex call; "
            "Cortex answered in another shape than requested; or a value to write holds template "
            "syntax dbt would run, so nothing is written for that column"
        ),
        precheck=Precheck.RUNTIME,
    ),
)
