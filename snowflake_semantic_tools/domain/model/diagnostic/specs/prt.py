"""External-system codes (PRT): a failure at the boundary with an external system.

The Snowflake connection, a transient failure, a refused privilege, a missing object, an
unsupported dbt manifest schema, or a file SST refuses to read.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "External systems"

SPECS: tuple[ErrorSpec, ...] = (
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
        "SST-PRT007",
        Severity.ERROR,
        "dbt manifest schema version unsupported",
        "manifest schema '{found}' is unsupported; expected '{expected}'",
        "use a dbt version that emits {expected}",
    ),
    spec(
        "SST-PRT009",
        Severity.ERROR,
        "Filesystem read failed",
        "could not read {path}: {detail}",
        "SST reads only regular files inside the project: replace a symbolic link with the file or folder "
        "it points to, and keep dbt's target-path inside the project",
    ),
)
