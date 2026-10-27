"""Read one column a tool member, or an agent's search tool over one, names.

A defined member's search service is built from its `on:` model, so each column it names is a
`{{ ref('<on model>', '<column>') }}` call, and the column is the call's second argument. A
referenced member is built outside dbt, so its columns are the object's own, written bare, and
take no template call.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import (
    call_problem,
    literal_problem,
    malformed_field,
    missing_call,
    syntax_problem,
)

_REF = frozenset(("ref",))


@dataclass(frozen=True, slots=True)
class ColumnSite:
    """Where a column is written, as each diagnostic about it names it.

    Attributes:
        name: The tool member's name, which a model mismatch names.
        field: The key the column is written under, such as `search_column`.
        artifact: How a diagnostic names what holds the field.
        subject: The artifact key every diagnostic carries.
    """

    name: str
    field: str
    artifact: str
    subject: str
    origin: Origin | None


def tool_column(
    value: str, site: ColumnSite, *, referenced: bool, on_model: str | None
) -> tuple[str | None, Diagnostic | None]:
    """Return the column `value` names, or the diagnostic that keeps it from naming one.

    Args:
        referenced: The member is a reference, so its columns are written bare.
        on_model: The model a defined member's `on:` names; None when it names none, which is
            reported elsewhere, and then any model the ref names is taken.

    Returns:
        The column and None, or None and the one diagnostic, checked in this order.

    Diagnostics:
        SST-REF008: a referenced member's column holds a template call.
        SST-LOD004, SST-REF033, SST-REF003: a defined member's column does not parse.
        SST-REF050: a defined member's column is written bare.
        SST-REF003: it holds more than one call, or text beside the call.
        SST-REF034, SST-REF035, SST-REF004, SST-REF041: the call is not a `ref()`.
        SST-REF015: the `ref()` does not name a model and a column.
        SST-VAL622: the `ref()` names a model other than the member's `on:` model.
    """
    if referenced:
        problem = literal_problem(value, site.origin, field=site.field, artifact=site.artifact, subject=site.subject)
        return (None, problem) if problem is not None else (value, None)
    file = site.origin.file if site.origin is not None else "<template>"
    try:
        calls = scan_template_calls(value)
    except TemplateSyntaxError as exc:
        return None, syntax_problem(exc, file, subject=site.subject)
    if not calls:
        return None, missing_call(
            value, "ref", site.origin, field=site.field, artifact=site.artifact, subject=site.subject
        )
    call = calls[0]
    if len(calls) != 1 or call.raw.strip() != value.strip():
        return None, malformed_field(value, site.origin, subject=site.subject)
    problem = call_problem(call, _REF, site.origin, field=site.field, artifact=site.artifact, subject=site.subject)
    if problem is not None:
        return None, problem
    if len(call.args) != 2:
        return None, D(
            "SST-REF015",
            origin=site.origin,
            subject=site.subject,
            ref_function="ref",
            expected="2",
            found=len(call.args),
        )
    model, column = call.args
    if on_model is not None and model.casefold() != on_model.casefold():
        return None, D(
            "SST-VAL622",
            origin=site.origin,
            subject=site.subject,
            name=site.name,
            field=site.field,
            found=model,
            expected=on_model,
        )
    return column, None
