"""Validation codes 0xx (VAL): names, descriptions, and connected validation, for every type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL001",
        Severity.ERROR,
        "Name is not unique within its type",
        "{type} '{name}' is declared more than once",
        "rename one of them",
    ),
    spec(
        "SST-VAL003",
        Severity.WARNING,
        "Description missing",
        "{type} '{name}' has no description",
        "add a description; it is how Analyst chooses between objects",
    ),
    spec(
        "SST-VAL020",
        Severity.INFO,
        "Connected validation unavailable",
        "connected validation skipped: {detail}",
        "connect to Snowflake to run connected checks",
    ),
)
