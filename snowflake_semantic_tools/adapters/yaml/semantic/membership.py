"""Assemble the member-resolution request from the parsed project, and run it with the build.

`domain.resolve.membership` decides attachment and reports what it means; this module reads
what those checks need from the authored records -- which only the YAML adapter knows the
shape of -- and hands them over as plain values.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.authored import InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.project import ArtifactKey, MemberKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, ViewScope
from snowflake_semantic_tools.domain.resolve.membership import MembershipResult, resolve_membership
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts, MembershipRequest


def membership(
    members: tuple[ParsedMember, ...],
    poisoned: frozenset[str],
    *,
    view_tables: Mapping[ArtifactKey, frozenset[str]],
    reported_views: frozenset[ArtifactKey],
    view_instructions: Mapping[ArtifactKey, frozenset[str]],
    known_models: frozenset[str],
    reported: Iterable[Diagnostic],
    view_scopes: Mapping[ArtifactKey, ViewScope] | None = None,
) -> tuple[tuple[ParsedMember, ...], MembershipResult]:
    """Mark the poisoned members, then resolve every member's membership.

    Args:
        poisoned: The casefolded keys of the members the load leaves out.
        reported_views: The keys of the views that will be built.
        view_instructions: For each view, the casefolded names of the custom instructions it names.
        known_models: The casefolded names of the dbt models.
        reported: Every diagnostic reported so far, from which a poisoned member's unresolved
            references are counted.
        view_scopes: Each built view's include or exclude lists, by its key.

    Returns:
        Every member, the poisoned ones marked so, and what member resolution decided.
    """
    marked = tuple((replace(member, poisoned=True) if member.key in poisoned else member) for member in members)
    request = MembershipRequest(
        members=marked,
        view_tables=view_tables,
        registry=SEMANTIC_REGISTRY,
        reported_views=reported_views,
        view_named_members=view_instructions,
        known_models=known_models,
        facts={member.key: facts for member in marked if (facts := _facts(member)) is not None},
        joins=tuple(
            (member.source.from_table.casefold(), member.source.to_table.casefold())
            for member in marked
            if isinstance(member.source, Relationship)
        ),
        instruction_channels={
            member.name.casefold(): _channels(member.source)
            for member in marked
            if isinstance(member.source, InstructionDef)
        },
        unresolved=_unresolved({member.key for member in marked if member.poisoned}, reported),
        view_scopes=view_scopes or {},
    )
    return marked, resolve_membership(request)


def _facts(member: ParsedMember) -> MemberFacts | None:
    """What member resolution needs of a metric or a verified query beyond its tables; None for others."""
    source = member.source
    if isinstance(source, MetricDef):
        return MemberFacts(
            derived=source.derived,
            private=source.access_modifier == "private_access",
            referenced_metrics=source.referenced_metrics,
            using_relationships=tuple(name.casefold() for name in source.using_relationships),
        )
    if isinstance(source, VerifiedQueryDef):
        names = (call.args[0].casefold() for call in member.template_calls if call.function == "metric" and call.args)
        return MemberFacts(referenced_metrics=tuple(dict.fromkeys(names)))
    return None


def _channels(source: InstructionDef) -> frozenset[str]:
    """The instruction channels a custom instruction's text writes."""
    written = (
        ("ai_sql_generation", source.ai_sql_generation),
        ("ai_question_categorization", source.ai_question_categorization),
    )
    return frozenset(channel for channel, text in written if text)


def _unresolved(keys: set[MemberKey], reported: Iterable[Diagnostic]) -> dict[MemberKey, int]:
    """Count, for each of `keys`, the reported references to it that did not resolve.

    A reference that did not resolve is reported under a REF code, or as an undeclared
    variable; each diagnostic names its member by its subject, compared casefolded.
    """
    counts: dict[MemberKey, int] = {}
    wanted = {key.casefold(): key for key in keys}
    for diagnostic in reported:
        subject = (diagnostic.subject or "").casefold()
        if subject in wanted and (diagnostic.code.startswith("SST-REF") or diagnostic.code == "SST-CFG029"):
            counts[wanted[subject]] = counts.get(wanted[subject], 0) + 1
    return counts
