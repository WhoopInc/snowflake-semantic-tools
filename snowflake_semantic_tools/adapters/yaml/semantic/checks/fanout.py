"""Report the authored members that attach to no view.

Attachment is implicit -- a member lands in every view that holds its tables -- so a member
whose tables no view holds is authored and connected to nothing, with nothing in its file to
say so. How far attachment reaches is reported from the compiled views instead.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedView

# The authored member types; columns are dbt's, and attach with their model.
AUTHORED_TYPES = ("metric", "filter", "relationship", "verified_query", "custom_instruction")


def _attachment_diagnostics(
    views: tuple[ParsedView, ...],
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
) -> tuple[Diagnostic, ...]:
    """Report each authored member that attaches to no view.

    A poisoned member is left out, and so is a metric built on one: the diagnostic that poisoned
    it already says why neither is in any view.

    Diagnostics:
        SST-VAL007: an unpoisoned authored member attaches to no view.
    """
    authored = [member for member in members if member.type_name in AUTHORED_TYPES and not member.poisoned]
    poisoned_metrics = {
        member.name.casefold() for member in members if member.type_name == "metric" and member.poisoned
    }
    diagnostics = [
        D(
            "SST-VAL007",
            origin=member.origin,
            subject=member.key,
            type=member.type_name,
            name=member.name,
        )
        for member in authored
        if not attachment.get(member.key) and not _built_on(member, poisoned_metrics)
    ]
    return tuple(diagnostics)


def _built_on(member: ParsedMember, poisoned_metrics: set[str]) -> bool:
    """Report whether a member's expression calls `metric()` on a poisoned metric."""
    return any(
        call.function == "metric" and call.args and call.args[0].casefold() in poisoned_metrics
        for call in member.template_calls
    )
