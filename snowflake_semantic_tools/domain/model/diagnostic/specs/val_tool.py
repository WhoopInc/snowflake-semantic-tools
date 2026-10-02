"""Validation codes 6xx (VAL): agent tools -- members, types, ownership, and sources."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL601",
        Severity.ERROR,
        "Tool member is duplicated within its group",
        "tool group '{a}': member '{name}' is declared twice",
        "rename or remove one declaration",
    ),
    spec(
        "SST-VAL602",
        Severity.WARNING,
        "Tool member name is not globally unique",
        "tool member '{name}' is declared in {a} and {b}",
        "use the canonical two-argument tool reference",
    ),
    spec(
        "SST-VAL603",
        Severity.ERROR,
        "Unknown tool type",
        "tool member '{name}': type '{found}' is not a known tool type",
        "use one of: {expected}",
    ),
    spec(
        "SST-VAL604",
        Severity.ERROR,
        "Defined tool lacks creation properties",
        "tool member '{name}' is under define: and declares neither on: nor body_file:",
        "add the creation fields required by the tool type",
    ),
    spec(
        "SST-VAL605",
        Severity.ERROR,
        "Tool ownership category is inconsistent",
        "tool member '{name}' is under reference: and declares '{key}'",
        "move creation properties under define:, or remove them",
    ),
    spec(
        "SST-VAL606",
        Severity.ERROR,
        "Immutable tool group contains managed objects",
        "tool group '{a}' is immutable: true and declares define:",
        "remove define:, or make the group mutable",
    ),
    spec(
        "SST-VAL608",
        Severity.ERROR,
        "Tool source is not a dbt model",
        "tool member '{name}': on: '{value}' is not a model in the dbt manifest",
        "reference a dbt model in the current manifest",
    ),
    spec(
        "SST-VAL609",
        Severity.ERROR,
        "Tool column is absent from its source",
        "tool member '{name}': '{column}' is not on '{value}'",
        "use a column present on the source model",
    ),
)
