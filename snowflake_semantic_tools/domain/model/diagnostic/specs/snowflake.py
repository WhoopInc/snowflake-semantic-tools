"""Snowflake codes (SNO) and external-system codes (PRT).

SNO classifies an error Snowflake returned for a statement. PRT reports a failure at
the boundary with an external system: the Snowflake connection, a transient failure,
a refused privilege, a missing object, an unsupported dbt manifest schema, or a file
SST refuses to read. Each prefix keeps its own section title in `SUBSYSTEMS`; `TITLE`
is the title of SNO.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Snowflake"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-PRT007",
        Severity.ERROR,
        "dbt manifest schema version unsupported",
        "manifest schema '{found}' is unsupported; expected '{expected}'",
        "use a dbt version that emits {expected}",
    ),
    spec(
        "SST-PRT001",
        Severity.ERROR,
        "Snowflake connection failed",
        "connection to {value} failed: {detail}",
        "check credentials, network access, and the selected target",
    ),
    spec(
        "SST-PRT003",
        Severity.ERROR,
        "Transient Snowflake failure",
        "transient Snowflake failure: {detail}",
        "retry the operation",
    ),
    spec(
        "SST-PRT004",
        Severity.ERROR,
        "Snowflake privilege refused",
        "Snowflake refused {value}: {detail}",
        "grant the required privilege to the primary role",
    ),
    spec(
        "SST-PRT005",
        Severity.ERROR,
        "Snowflake object not found",
        "Snowflake object {value} was not found: {detail}",
        "create the dependency or correct its name",
    ),
    spec(
        "SST-PRT009",
        Severity.ERROR,
        "Filesystem read failed",
        "could not read {path}: {detail}",
        "SST reads only regular files inside the project: replace a symbolic link with the file or folder "
        "it points to, and keep dbt's target-path inside the project",
    ),
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
