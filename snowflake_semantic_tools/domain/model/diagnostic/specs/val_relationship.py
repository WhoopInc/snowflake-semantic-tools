"""Validation codes 2xx (VAL): relationships -- conditions, endpoints, keys, join paths, and cycles."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL201",
        Severity.ERROR,
        "Relationship declares no conditions",
        "relationship '{relationship}' declares no conditions",
        "declare at least one condition",
    ),
    spec(
        "SST-VAL203",
        Severity.ERROR,
        "Relationship names a table not in the view",
        "relationship '{relationship}' names '{name}', absent from {artifact}",
        "add the table to the view, or drop the relationship",
    ),
    spec(
        "SST-VAL204",
        Severity.ERROR,
        "Relationship column is on the wrong table",
        "relationship '{relationship}': '{column}' is not on '{name}'",
        "correct the column, or swap the sides",
    ),
    spec(
        "SST-VAL209",
        Severity.WARNING,
        "Ambiguous join path between two tables",
        "{artifact}: {count} paths between '{a}' and '{b}'",
        "declare using_relationships on the affected metrics",
    ),
    spec(
        "SST-VAL210",
        Severity.WARNING,
        "Relationship target has no matching key",
        "relationship '{relationship}': '{name}' declares neither primary_key nor unique_keys over {value}",
        "declare the key; it is the cheapest fan-out protection",
    ),
    spec(
        "SST-VAL214",
        Severity.ERROR,
        "Unknown relationship reference",
        "metric '{metric}' names relationship '{relationship}', which is not declared",
        "declare the relationship or correct the name",
    ),
    spec(
        "SST-VAL215",
        Severity.ERROR,
        "Relationship graph cycle changes results",
        "{artifact}: relationship cycle {cycle}",
        "break the cycle, or split role-playing tables",
    ),
    spec(
        "SST-VAL223",
        Severity.ERROR,
        "Primary and unique keys overlap",
        "{artifact}: column '{column}' on '{model}' appears in both primary_key and unique_keys",
        "remove it from unique_keys; a primary key is already unique",
    ),
)
