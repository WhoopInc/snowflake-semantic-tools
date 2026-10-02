"""Member resolution: attach every member to its artifacts, and report what the attachment means.

`resolve_membership` is the one step the semantic load calls to decide membership. It runs the
attachment twice -- the second run is the idempotence check -- and then reports, from the
answer, the members whose tables cannot be right, the members whose reach is surprising, and
any sign that the attachment contradicts its own rule. The checks are pure functions of the
request, split by what they read: `membership_tables` the members' own tables,
`membership_reach` the answer, and this module the answer against the rule that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.project import ArtifactKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import AttachRule, Registry
from snowflake_semantic_tools.domain.resolve.members import attach_members, attach_view_members, effective_tables
from snowflake_semantic_tools.domain.resolve.membership_model import Attachment, MembershipRequest
from snowflake_semantic_tools.domain.resolve.membership_reach import reach_diagnostics
from snowflake_semantic_tools.domain.resolve.membership_tables import table_diagnostics


@dataclass(frozen=True, slots=True)
class MembershipResult:
    """The attachment member resolution decided, and what it reported.

    Attributes:
        attachment: Each member's key, mapped to the keys of the views it attaches to, sorted.
    """

    attachment: Attachment
    diagnostics: DiagnosticBag


def resolve_membership(request: MembershipRequest) -> MembershipResult:
    """Attach every member, check the attachment against its rule, and report what it means.

    Diagnostics:
        Those of `membership_tables.table_diagnostics`, then of
        `membership_reach.reach_diagnostics`, then of `invariant_diagnostics`.
    """
    table_attachment = attach_members(request.view_tables, request.members, request.registry)
    attachment = _attach(request)
    repeat = _attach(request)
    diagnostics = (
        *table_diagnostics(request),
        *reach_diagnostics(request, attachment),
        *invariant_diagnostics(request, table_attachment, attachment, repeat),
    )
    return MembershipResult(attachment, DiagnosticBag(diagnostics))


def _attach(request: MembershipRequest) -> Attachment:
    return attach_view_members(
        request.view_tables,
        request.members,
        request.registry,
        view_named_members=request.view_named_members,
        metric_dependencies=request.metric_dependencies(),
    )


def invariant_diagnostics(
    request: MembershipRequest,
    table_attachment: Attachment,
    attachment: Attachment,
    repeat: Attachment,
) -> tuple[Diagnostic, ...]:
    """Report every way an attachment contradicts the rule that produced it.

    None of these can follow from a project: each means the attachment code itself is wrong,
    so each is reported rather than trusted.

    Args:
        table_attachment: What table membership alone attached, from `attach_members`.
        attachment: The attachment, as the load uses it.
        repeat: The same attachment, computed a second time.

    Diagnostics:
        SST-MEM100: a member is attached to an artifact whose type takes no members.
        SST-MEM008: a member is attached to a view that lacks one of its tables.
        SST-MEM013: a member that only table membership places was placed elsewhere.
        SST-MEM106: the same, for a verified query.
        SST-MEM900: the second computation of the attachment differs from the first.
    """
    dependencies = request.metric_dependencies()
    diagnostics: list[Diagnostic] = []
    for member in request.members:
        placed = attachment.get(member.key, ())
        diagnostics.extend(_untyped_attachment(member, placed, request.registry))
        rule = request.registry.members[member.type_name].attaches_by
        if rule is AttachRule.TABLE_MEMBERSHIP:
            diagnostics.extend(_missing_tables(member, placed, request))
            if member.key not in dependencies:
                diagnostics.extend(_divergent(member, table_attachment.get(member.key, ()), placed))
        if repeat.get(member.key, ()) != placed:
            diagnostics.append(D("SST-MEM900", member=member.key, subject=member.key, origin=member.origin))
    return tuple(diagnostics)


def _untyped_attachment(member: ParsedMember, placed: tuple[ArtifactKey, ...], registry: Registry) -> list[Diagnostic]:
    """Report each artifact `member` is attached to whose type owns no member types (SST-MEM100)."""
    diagnostics: list[Diagnostic] = []
    for artifact in placed:
        type_name = split_artifact_key(artifact)[0]
        descriptor = registry.artifacts.get(type_name)
        if descriptor is None or not descriptor.member_types:
            diagnostics.append(
                D("SST-MEM100", type=type_name, member=member.key, subject=member.key, origin=member.origin)
            )
    return diagnostics


def _missing_tables(
    member: ParsedMember, placed: tuple[ArtifactKey, ...], request: MembershipRequest
) -> list[Diagnostic]:
    """Report each view `member` is attached to that lacks one of the tables it needs (SST-MEM008)."""
    needed = effective_tables(member)
    return [
        D("SST-MEM008", member=member.key, artifact=artifact, name=table, subject=member.key, origin=member.origin)
        for artifact in placed
        for table in sorted(needed - request.view_tables.get(artifact, frozenset()))
    ]


def _divergent(
    member: ParsedMember, by_tables: tuple[ArtifactKey, ...], placed: tuple[ArtifactKey, ...]
) -> list[Diagnostic]:
    """Report a member placed other than where table membership put it (SST-MEM013, SST-MEM106)."""
    if by_tables == placed:
        return []
    if member.type_name == "verified_query":
        return [D("SST-MEM106", member=member.key, subject=member.key, origin=member.origin)]
    return [
        D(
            "SST-MEM013",
            member=member.key,
            a=", ".join(by_tables) or "nothing",
            b=", ".join(placed) or "nothing",
            subject=member.key,
            origin=member.origin,
        )
    ]
