"""Check each verified query's SQL, then the verified queries each view attaches together."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import VerifiedQueryDef
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.validate.relative_date import sql_relative_date


def relative_date_diagnostics(queries: tuple[VerifiedQueryDef, ...]) -> tuple[Diagnostic, ...]:
    """Report each verified query whose SQL reads the clock, naming the first function that does.

    Diagnostics:
        SST-VAL416: when the SQL calls a function such as CURRENT_DATE outside a string or comment.
    """
    return tuple(
        D(
            "SST-VAL416",
            member=query.name,
            value=value,
            subject=artifact_key("verified_query", query.name),
            origin=query.origin,
        )
        for query in queries
        if (value := sql_relative_date(query.sql)) is not None
    )


def duplicate_question_diagnostics(
    members: tuple[ParsedMember, ...], attachment: Mapping[str, tuple[str, ...]]
) -> tuple[Diagnostic, ...]:
    """Report, view by view, each verified query whose question an earlier one attached to it asks.

    Questions compare stripped and casefolded, with each run of whitespace read as one
    space. Views are visited in key order, and each view's queries in member order.

    Args:
        attachment: The view keys each member key attaches to.

    Diagnostics:
        SST-VAL413: when two verified queries attached to one view share their question text.
    """
    by_view: dict[str, list[tuple[ParsedMember, VerifiedQueryDef]]] = {}
    for member in members:
        if member.type_name != "verified_query" or not isinstance(member.source, VerifiedQueryDef):
            continue
        for view_key in attachment.get(member.key, ()):
            by_view.setdefault(view_key, []).append((member, member.source))
    diagnostics: list[Diagnostic] = []
    for view_key in sorted(by_view):
        seen: dict[str, VerifiedQueryDef] = {}
        for _, query in by_view[view_key]:
            text = " ".join(query.question.casefold().split())
            first = seen.setdefault(text, query)
            if first is not query:
                diagnostics.append(
                    D(
                        "SST-VAL413",
                        artifact=view_key,
                        a=first.name,
                        b=query.name,
                        subject=view_key,
                        origin=query.origin,
                        related=tuple(origin for origin in (first.origin,) if origin is not None),
                    )
                )
    return tuple(diagnostics)
