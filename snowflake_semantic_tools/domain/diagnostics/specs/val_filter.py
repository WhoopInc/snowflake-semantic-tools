"""Validation codes 4xx (VAL): filters, custom instructions, verified queries, and compiled SQL."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL401",
        Severity.ERROR,
        "Filter expression is not boolean",
        "filter '{member}' carries labels: [filter] and its expr is not boolean",
        "make the expression boolean, or remove `labels: [filter]` so it renders as prose",
        condition=(
            "a filter labelled `filter` has an expression SST does not read as boolean: neither a predicate "
            "by its shape nor one `ref()` to a column dbt declares BOOLEAN"
        ),
    ),
    spec(
        "SST-VAL402",
        Severity.ERROR,
        "Filter label applied to a metric",
        "'{member}' carries labels: [filter] and is a metric",
        "remove `labels:` from the metric, or declare the expression under `snowflake_filters:`",
        condition="a metric declares `labels:` holding `filter`",
    ),
    spec(
        "SST-VAL403",
        Severity.ERROR,
        "Legacy filter syntax",
        "filter '{member}' uses the legacy inline form",
        "move each entry to `snowflake_filters:` as a named filter with `labels: [filter]`",
        demotable=False,
        condition="a semantic view declares `filters:`, the 0.3 inline form 1.0 removed",
        note="Reported once per inline entry.",
    ),
    spec(
        "SST-VAL404",
        Severity.WARNING,
        "Filter expression contains an unwrapped bare identifier",
        "filter '{member}' expression contains bare identifier '{column}'",
        "wrap it in `{{ ref('<model>', '<column>') }}`",
        condition=(
            "a filter's expression names a column of one of its tables without `ref()`, so the reference is not checked"
        ),
    ),
    spec(
        "SST-VAL405",
        Severity.ERROR,
        "Boolean standalone filter",
        "filter '{member}' is boolean-valued and declares no labels: key",
        "add `labels: [filter]` so it renders as a native `LABELS = (FILTER)` dimension on its table",
        condition="a filter whose expression is boolean declares no `labels:` key",
        note=(
            "Without the label it would render only as a sentence in the view's SQL-generation "
            "instructions, which no query enforces."
        ),
    ),
    spec(
        "SST-VAL406",
        Severity.WARNING,
        "Filter synonyms declared but not emitted",
        "filter '{member}' declares synonyms that the renderer drops",
        "remove `synonyms:` from the filter",
        condition="a filter declares `synonyms:`, which no clause of the view's DDL carries for a filter",
    ),
    spec(
        "SST-VAL407",
        Severity.ERROR,
        "Custom instruction block has no non-empty channel",
        "custom_instruction '{member}' declares no non-empty channel",
        "write text under `ai_sql_generation` or `ai_question_categorization`",
        condition=(
            "a custom instruction's `ai_sql_generation` and `ai_question_categorization` are both empty or absent"
        ),
        note="A block written only in the 0.3 spellings is `SST-VAL012` instead.",
    ),
    spec(
        "SST-VAL408",
        Severity.ERROR,
        "View custom_instructions is a bare string",
        "semantic view '{member}': custom_instructions is a bare string, the 0.3 form",
        "declare the text as a block under `snowflake_custom_instructions:` and list "
        "`{{ custom_instructions('<name>') }}` in the view's `custom_instructions:`",
        condition="a semantic view's `custom_instructions:` is a string, the 0.3 form, rather than a list",
    ),
    spec(
        "SST-VAL409",
        Severity.WARNING,
        "Instruction placed in the wrong channel",
        "custom_instruction '{member}': a {found} rule appears in the {expected} channel",
        "move the rule to the channel that acts on it",
        condition=(
            "a rule about whether to answer (out of scope, decline, ask for clarification) sits in "
            "`ai_sql_generation`, or a rule shaping the SQL (group by, order by, rounding, joins) sits in "
            "`ai_question_categorization`"
        ),
        note="The channel that does not act on the rule ignores it.",
    ),
    spec(
        "SST-VAL410",
        Severity.WARNING,
        "Two instruction blocks on one view contradict each other",
        "{artifact}: '{a}' and '{b}' give contradictory directives",
        "reconcile the two blocks",
        condition=(
            "two custom instructions one view attaches carry the same directive clause, one with "
            "`always`, `must` or `should` and the other with its negation"
        ),
    ),
    spec(
        "SST-VAL411",
        Severity.WARNING,
        "Instruction uses Cortex Analyst state keywords",
        "custom_instruction '{member}' uses {found} keywords, which an agent does not need",
        "write the instruction as plain natural language",
        condition=(
            "a custom instruction's text uses `UNCLEAR`, `AMBIGUOUS`, `UNAMBIGUOUS_SQL`, `UNANSWERABLE` or "
            "`OUT_OF_SCOPE`, in uppercase"
        ),
        note="Unconditional, because every 1.0 consumer is an agent.",
    ),
    spec(
        "SST-VAL413",
        Severity.ERROR,
        "Verified query question text is not unique within a view",
        "{artifact}: question text is shared by '{a}' and '{b}'",
        "make the question text unique",
        condition=(
            "two verified queries attached to one view ask the same question, compared ignoring case "
            "and runs of whitespace"
        ),
    ),
    spec(
        "SST-VAL414",
        Severity.WARNING,
        "Verified query SQL reads a table it does not name with ref()",
        "verified_query '{member}' queries '{relation}' without ref()",
        "write the table as {{ ref('<model>') }}",
        condition=(
            "a verified query's SQL reads a table by name rather than with ref(); its tables are the "
            "models its ref() calls name, so the query may land on a view that lacks the table"
        ),
    ),
    spec(
        "SST-VAL415",
        Severity.WARNING,
        "Verified query returns no rows",
        "verified_query '{member}' executed and returned {row_count} rows in {elapsed_ms}ms (in {view})",
        "fix the query, or check that the target holds the data it asks about",
        condition=(
            "connected validate runs a verified query that compiles and it returns no rows, so it teaches nothing"
        ),
        note=(
            "The query runs over its view's own tables, each logical table it names bound to the relation "
            "the view reads. One that several semantic views hold over the same relations is counted once, "
            "and reported once, naming every such view."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL416",
        Severity.WARNING,
        "Verified query SQL contains a relative date",
        "verified_query '{member}' contains relative date '{value}'",
        "pin the date, or accept that its answer changes over time",
        condition=(
            "a verified query's SQL calls a clock function such as `CURRENT_DATE` outside a string, "
            "quoted identifier or comment"
        ),
        note="A verified query that doubles as eval ground truth then scores against a moving answer.",
    ),
    spec(
        "SST-VAL417",
        Severity.WARNING,
        "Question text is shared across three different sets",
        "'{value}' appears as a VQ question, an agent sample_question and an eval row",
        "keep the three sets distinct",
        condition=(
            "one question, ignoring surrounding whitespace, is a verified query's `question`, an agent's "
            "sample question and an eval row"
        ),
    ),
    spec(
        "SST-VAL418",
        Severity.ERROR,
        "Expression or query is not valid SQL",
        "{type} '{name}' is not valid SQL: {detail}",
        "fix the expression or query; the message carries the reason",
        condition=(
            "offline: an expression or a tool member's `where:` is not one SQL expression, or verified-query "
            "SQL does not begin with SELECT or WITH or is more than one statement; connected, with "
            "`validation.snowflake_syntax_check` on: Snowflake's EXPLAIN of a metric, dimension, filter or "
            "verified query fails"
        ),
        note=(
            "Offline, verified-query SQL is checked for its shape only, so a misspelt keyword or column needs "
            "the connected check. That check compiles a verified query over its view's own tables, each "
            "logical table it names bound to the relation the view reads, wherever the view publishes, "
            "and resolves any other name in the view's schema; before apply creates that schema, on the "
            "session's own. "
            "It compiles a metric, dimension or filter over a projection with no rows, in which each "
            "`TABLE.COLUMN` reference is a NULL of the column's type: the type `DESCRIBE TABLE` reports in the "
            "target, else the `data_type` the dbt model declares, else none. It therefore catches a syntax error, "
            "an unknown function, and an argument type Snowflake refuses when it compiles, such as `SUM` over a "
            "timestamp, but not a value that fails only when a query runs, such as `SUM` over text that is not "
            "numeric, nor a column the table lacks (`--verify-schema` reports that, SST-VAL325); a reference to "
            "a metric, a filter, or a variable is untyped, and facts are not compiled. A finding is reported "
            "once, its detail naming every semantic view that holds the member."
        ),
        precheck=Precheck.OBSERVE,
    ),
)
