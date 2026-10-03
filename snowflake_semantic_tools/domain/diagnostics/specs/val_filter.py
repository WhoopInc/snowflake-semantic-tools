"""Validation codes 4xx (VAL): filters, custom instructions, verified queries, and compiled SQL."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

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
        "SST-VAL402",
        Severity.ERROR,
        "Filter label applied to a metric",
        "'{member}' carries labels: [filter] and is a metric",
        "remove the label, or declare a filter",
    ),
    spec(
        "SST-VAL403",
        Severity.ERROR,
        "Legacy filter syntax",
        "filter '{member}' uses the legacy inline form",
        "declare filters as named objects with labels:",
        demotable=False,
    ),
    spec(
        "SST-VAL404",
        Severity.WARNING,
        "Filter expression contains an unwrapped bare identifier",
        "filter '{member}' expression contains bare identifier '{column}'",
        "wrap it in {{ ref('<model>','<column>') }}",
    ),
    spec(
        "SST-VAL405",
        Severity.ERROR,
        "Boolean standalone filter",
        "filter '{member}' is boolean-valued and declares no labels: key",
        "add `labels: [filter]` so it renders as a native `LABELS = (FILTER)` dimension on its table",
    ),
    spec(
        "SST-VAL406",
        Severity.WARNING,
        "Filter synonyms declared but not emitted",
        "filter '{member}' declares synonyms that the renderer drops",
        "remove the synonyms until the renderer emits them",
    ),
    spec(
        "SST-VAL407",
        Severity.ERROR,
        "Custom instruction block has no non-empty channel",
        "custom_instruction '{member}' declares no non-empty channel",
        "populate at least one channel",
    ),
    spec(
        "SST-VAL408",
        Severity.ERROR,
        "Legacy custom-instruction rendering",
        "custom_instruction '{member}' would render as a bare string",
        "emit module_custom_instructions or the ai_-prefixed clauses",
    ),
    spec(
        "SST-VAL409",
        Severity.WARNING,
        "Instruction placed in the wrong channel",
        "custom_instruction '{member}': a {found} rule appears in the {expected} channel",
        "move the rule to the channel that acts on it",
    ),
    spec(
        "SST-VAL410",
        Severity.WARNING,
        "Two instruction blocks on one view contradict each other",
        "{artifact}: '{a}' and '{b}' give contradictory directives",
        "reconcile the two blocks",
    ),
    spec(
        "SST-VAL411",
        Severity.WARNING,
        "Instruction uses Cortex Analyst state keywords",
        "custom_instruction '{member}' uses {found} keywords, which an agent does not need",
        "write the instruction as plain natural language",
    ),
    spec(
        "SST-VAL412",
        Severity.ERROR,
        "Verified query declares neither sql nor sql_file, or both",
        "verified_query '{member}': {detail}",
        "declare exactly one of sql or sql_file",
    ),
    spec(
        "SST-VAL413",
        Severity.ERROR,
        "Verified query question text is not unique within a view",
        "{artifact}: question text is shared by '{a}' and '{b}'",
        "make the question text unique",
    ),
    spec(
        "SST-VAL414",
        Severity.WARNING,
        "Verified query SQL references a table not in its table list",
        "verified_query '{member}' queries '{relation}', absent from tables:",
        "add the table to tables:",
    ),
    spec(
        "SST-VAL415",
        Severity.WARNING,
        "Verified query returns no rows",
        "verified_query '{member}' executed and returned {row_count} rows in {elapsed_ms}ms",
        "fix the query, or widen the fixture",
    ),
    spec(
        "SST-VAL416",
        Severity.WARNING,
        "Verified query SQL contains a relative date",
        "verified_query '{member}' contains relative date '{value}'",
        "pin the date, or accept that it is runtime guidance only",
    ),
    spec(
        "SST-VAL417",
        Severity.WARNING,
        "Question text is shared across three different sets",
        "'{value}' appears as a VQ question, an agent sample_question and an eval row",
        "keep the three sets distinct",
    ),
    spec(
        "SST-VAL418",
        Severity.ERROR,
        "Expression does not compile against Snowflake",
        "{type} '{name}': expression failed to compile: {detail}",
        "fix the expression",
    ),
)
