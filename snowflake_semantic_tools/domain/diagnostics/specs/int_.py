"""Internal codes (INT): a defect in SST itself.

`D` emits SST-INT900 for an unregistered code and SST-INT901 for missing template
context, so both must stay registered. The module is `int_` so it does not shadow the builtin.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Internal"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-INT001",
        Severity.ERROR,
        "Unhandled internal exception",
        "internal error: {detail}",
        "report this as a bug, with the command line and the output",
        demotable=False,
    ),
    spec(
        "SST-INT900",
        Severity.ERROR,
        "Unregistered diagnostic code",
        "unregistered code {value}",
        "report this as a bug",
        demotable=False,
    ),
    spec(
        "SST-INT901",
        Severity.ERROR,
        "Missing diagnostic context",
        "{value} template needs {placeholder}, which was not supplied",
        "report this as a bug",
        demotable=False,
    ),
    spec(
        "SST-INT902",
        Severity.ERROR,
        "Domain invariant violated",
        "domain invariant violated: {detail}",
        "report this as a bug",
        demotable=False,
    ),
)
