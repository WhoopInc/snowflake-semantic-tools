"""Resolve a stage path that names an SST tool member: `"{{ tool('<stage>') }}/<path>"`.

A stage SST declares as a tool member is named by reference, so the path follows the member to
wherever its settings place it on each target. A stage SST does not declare is an external
object, written as its own `@<database>.<schema>.<stage>/<path>`, and kept as written.
"""

from __future__ import annotations

from collections.abc import Callable

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolKind, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import call_problem, malformed_field, syntax_problem
from snowflake_semantic_tools.domain.resolve.template import STAGE_PATH


def resolve_stage_path(
    text: str,
    catalog: ToolCatalog,
    defined_at: Callable[[ToolMember], QualifiedName],
    origin: Origin,
    *,
    field: str,
    artifact: str,
    subject: str | None = None,
) -> str | Diagnostic:
    """Return a stage path with its opening `{{ tool('<stage>') }}` call replaced by `@<stage>`.

    A path that holds no template is a literal, external stage, returned as written. One
    that does must open with exactly one `tool()` call naming a `stage` member; the text after
    the call is kept as written.

    Example:
        `{{ tool('weekly_exports') }}/2026/` renders as `@DB.SCHEMA.WEEKLY_EXPORTS/2026/`.

    Args:
        defined_at: Where a member SST defines publishes; a referenced member is its relation.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: the template does not parse.
        SST-REF003: the path holds more than one call, or text before the call.
        SST-REF034, SST-REF035, SST-REF004, SST-REF041, SST-REF015: as `call_problem` refuses
            the call under the stage-path policy.
        SST-REF010: the call names no tool member.
        SST-REF020: the member is not a stage.
        SST-REF018, SST-REF019: a referenced stage has no usable relation for the target.
    """
    if "{{" not in text:
        return text
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError as exc:
        return syntax_problem(exc, origin.file, subject=subject)
    if len(calls) != 1 or text[: calls[0].start].strip():
        return malformed_field(text, origin, subject=subject)
    call = calls[0]
    problem = call_problem(call, STAGE_PATH.allowed, origin, field=field, artifact=artifact, subject=subject)
    if problem is not None:
        return problem
    member, problems = catalog.resolve(call.args[0])
    if member is None:
        return problems[0]
    if member.type != ToolKind.STAGE.value:
        return D(
            "SST-REF020",
            origin=origin,
            subject=subject,
            ref_function="tool",
            name=member.name,
            found=member.type,
            expected=ToolKind.STAGE.value,
        )
    if member.ownership is ToolOwnership.DEFINE:
        location = defined_at(member)
    else:
        relation, problems = catalog.relation(member)
        if relation is None:
            return problems[0]
        location = relation
    return f"@{location.sql}{text[call.end :]}"


def stage_member(text: str) -> str | None:
    """Return the member a stage path's opening `{{ tool('<stage>') }}` names; None for any other path.

    A path that does not parse, or is not one opening one-name `tool()` call, names none: the
    resolver reports it.
    """
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return None
    if len(calls) != 1 or calls[0].function != "tool" or len(calls[0].args) != 1 or text[: calls[0].start].strip():
        return None
    return calls[0].args[0]
