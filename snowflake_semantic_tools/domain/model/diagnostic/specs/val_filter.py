"""Validation codes 4xx (VAL): filters, custom instructions, verified queries, and compiled SQL."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL401",
        Severity.ERROR,
        "Filter expression is not boolean",
        "filter '{member}' carries labels: [filter] and its expr is not boolean",
        "make the expression boolean",
    ),
    spec(
        "SST-VAL405",
        Severity.ERROR,
        "Boolean standalone filter",
        "filter '{member}' is boolean-valued and declares no labels: key",
        "add labels: [filter] so it renders as a native LABELS = (FILTER) dimension; sst migrate refs adds it",
    ),
    spec(
        "SST-VAL412",
        Severity.ERROR,
        "Verified-query SQL source is invalid",
        "verified_query '{member}': {detail}",
        "declare exactly one of sql or sql_file",
    ),
    spec(
        "SST-VAL413",
        Severity.WARNING,
        "Verified-query SQL reads an undeclared table",
        "verified_query '{member}': its SQL reads '{name}', which is not in its tables:",
        "add the table to tables:, which decides the views the query attaches to",
    ),
    spec(
        "SST-VAL418",
        Severity.ERROR,
        "Expression does not compile against Snowflake",
        "{type} '{name}': expression failed to compile: {detail}",
        "fix the expression so Snowflake compiles it; SST also refuses, before sending it, a `;`, a "
        "comment, `$$`, an unbalanced bracket, or a statement keyword outside quotes",
    ),
)
