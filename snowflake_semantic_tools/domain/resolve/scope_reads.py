"""What a filter or verified query reads, against a view's scope.

A scope lists no filters or verified queries: each attaches by its tables. One that reads,
with a two-argument `ref()`, a column the view's scope removes would publish the data the
scope hides, so attachment leaves it out of that view and validation says so (SST-VAL337).
Bare column names, as a verified query's SQL may write them, are not read here.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored import FilterDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.semantic_view import ViewScope
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls


def column_reads(calls: tuple[TemplateCall, ...]) -> tuple[str, ...]:
    """Return the columns two-argument `ref()` calls read, each `<model>.<column>` casefolded, once each."""
    return tuple(
        dict.fromkeys(
            f"{call.args[0].casefold()}.{call.args[1].casefold()}"
            for call in calls
            if call.function == "ref" and len(call.args) == 2
        )
    )


def reader_calls(reader: FilterDef | VerifiedQueryDef) -> tuple[TemplateCall, ...]:
    """Return the calls a filter's expression or a verified query's SQL makes; none when it is malformed."""
    if reader.template_calls:
        return reader.template_calls
    try:
        return scan_template_calls(reader.expr if isinstance(reader, FilterDef) else reader.sql)
    except TemplateSyntaxError:
        return ()


def hidden_reads(reader: FilterDef | VerifiedQueryDef, scope: ViewScope) -> tuple[str, ...]:
    """Return the columns `reader` reads with `ref()` that `scope` removes, in order, once each."""
    return tuple(column for column in column_reads(reader_calls(reader)) if not scope.admits_column_name(column))
