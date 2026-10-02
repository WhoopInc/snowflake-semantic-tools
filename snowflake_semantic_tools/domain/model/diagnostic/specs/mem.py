"""Membership codes (MEM): a table that names no dbt model, or a member attached to no artifact."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Membership"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-MEM003",
        Severity.ERROR,
        "Declared table does not name a known dbt model",
        "{member} declares table '{name}', which is not a known dbt model",
        "use a dbt model name the manifest knows",
    ),
    spec(
        "SST-MEM005",
        Severity.WARNING,
        "Member attached to zero artifacts",
        "{member} attaches to no {type}",
        "add its tables to a view, or delete the member",
    ),
)
