"""Membership checks over the attachment answer: what each member reaches, and each view holds.

Attachment is implicit -- a member lands wherever its tables are -- so the answer is reported
rather than left to be inferred: a member that reaches nothing, or more than one view, a view
that holds nothing, and the attachments that make one member or one table mean two things.
"""

from __future__ import annotations

from collections.abc import Iterator
from itertools import combinations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.project import ArtifactKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import AttachRule
from snowflake_semantic_tools.domain.resolve.members import effective_tables
from snowflake_semantic_tools.domain.resolve.membership_model import Attachment, MembershipRequest
from snowflake_semantic_tools.domain.resolve.membership_tables import authored_members

# The member types a view must hold one of to answer anything.
_ANSWERING_TYPES = ("dimension", "fact", "metric")
# The two channels a custom instruction writes, as `instruction_channels` names them.
_CHANNELS = ("ai_sql_generation", "ai_question_categorization")


def reach_diagnostics(request: MembershipRequest, attachment: Attachment) -> tuple[Diagnostic, ...]:
    """Report what the attachment means for each member, then for each view.

    Diagnostics:
        SST-MEM005: an authored member attaches to no view, unless a metric it is built on is
            poisoned, however indirectly.
        SST-MEM010: with it, a table it declares that the closest view reaches only by a join.
        SST-MEM011: an authored member attaches to more than one view.
        SST-MEM015: a verified query references a private metric.
        SST-MEM016: a metric's join path is attached to one of its views and not another.
        SST-MEM107: a poisoned member was not attached because its references did not resolve.
        SST-MEM104: a reported view whose tables are all dbt models holds no dimension, fact or
            metric.
        SST-MEM103: what each reported view holds, by member type.
        SST-MEM105: two reported views share a table and attach disjoint guidance for it.
    """
    return (
        *_member_reach(request, attachment),
        *_skipped(request),
        *_view_contents(request, attachment),
        *_instruction_conflicts(request, attachment),
    )


def _member_reach(request: MembershipRequest, attachment: Attachment) -> Iterator[Diagnostic]:
    """Report each authored member's reach; see `reach_diagnostics`.

    A member built, however indirectly, on a poisoned metric attaches nowhere because that
    metric does not, which is already reported, so it is not reported as unattached as well.
    """
    metrics = [member for member in request.members if member.type_name == "metric"]
    private = frozenset(member.name.casefold() for member in metrics if request.facts_of(member).private)
    poisoned = _blocked(request, metrics)
    for member in authored_members(request):
        placed = attachment.get(member.key, ())
        facts = request.facts_of(member)
        cascades = bool(poisoned.intersection(facts.referenced_metrics))
        if not placed and not cascades and (effective_tables(member) or facts.derived):
            yield D("SST-MEM005", member=member.key, type="semantic_view", subject=member.key, origin=member.origin)
            yield from _join_only(request, member)
        if len(placed) > 1:
            yield D("SST-MEM011", member=member.key, count=len(placed), subject=member.key, origin=member.origin)
        if member.type_name == "verified_query":
            for name in facts.referenced_metrics:
                if name in private:
                    yield D(
                        "SST-MEM015",
                        member=artifact_key("metric", name),
                        artifact=member.key,
                        subject=member.key,
                        origin=member.origin,
                    )
        yield from _conflicting_scope(member, placed, request, attachment)


def _blocked(request: MembershipRequest, metrics: list[ParsedMember]) -> frozenset[str]:
    """Return the casefolded names of the poisoned metrics and of every metric built on one."""
    blocked = {member.name.casefold() for member in metrics if member.poisoned}
    grown = True
    while grown:
        grown = False
        for member in metrics:
            name = member.name.casefold()
            if name not in blocked and blocked.intersection(request.facts_of(member).referenced_metrics):
                blocked.add(name)
                grown = True
    return frozenset(blocked)


def _join_only(request: MembershipRequest, member: ParsedMember) -> Iterator[Diagnostic]:
    """Report each table the view closest to holding `member` reaches only through a relationship.

    The closest view lacks the fewest of the member's tables; of two that tie, the first in
    key order. A missing table is reached through a join when a declared relationship joins it
    to one of that view's tables.
    """
    needed = effective_tables(member)
    if not needed or not request.view_tables:
        return
    closest = min(sorted(request.view_tables), key=lambda key: len(needed - request.view_tables[key]))
    tables = request.view_tables[closest]
    for missing in sorted(needed - tables):
        if any(
            (left in tables and right == missing) or (right in tables and left == missing)
            for left, right in request.joins
        ):
            yield D(
                "SST-MEM010",
                member=member.key,
                name=missing,
                artifact=closest,
                subject=member.key,
                origin=member.origin,
            )


def _conflicting_scope(
    member: ParsedMember, placed: tuple[ArtifactKey, ...], request: MembershipRequest, attachment: Attachment
) -> Iterator[Diagnostic]:
    """Report a metric whose join path one of its views holds and another lacks (SST-MEM016).

    Each view's metric renders `USING` the same relationships, so in a view that lacks one the
    metric means something else, or nothing. One pair is reported per metric.
    """
    for relationship in request.facts_of(member).using_relationships:
        joined = set(attachment.get(artifact_key("relationship", relationship), ()))
        with_join = [view for view in placed if view in joined]
        without_join = [view for view in placed if view not in joined]
        if with_join and without_join:
            yield D(
                "SST-MEM016",
                member=member.key,
                a=with_join[0],
                b=without_join[0],
                subject=member.key,
                origin=member.origin,
            )
            return


def _skipped(request: MembershipRequest) -> Iterator[Diagnostic]:
    """Report each poisoned member whose references did not resolve, which attachment skipped (SST-MEM107)."""
    for member in request.members:
        count = request.unresolved.get(member.key, 0)
        if member.poisoned and count:
            yield D("SST-MEM107", member=member.key, count=count, subject=member.key, origin=member.origin)


def _held(request: MembershipRequest, attachment: Attachment) -> dict[ArtifactKey, dict[str, int]]:
    """Count, for each reported view, the members of each type attached to it."""
    held: dict[ArtifactKey, dict[str, int]] = {view: {} for view in sorted(request.reported_views)}
    for member in request.members:
        for view in attachment.get(member.key, ()):
            if view in held:
                held[view][member.type_name] = held[view].get(member.type_name, 0) + 1
    return held


def _view_contents(request: MembershipRequest, attachment: Attachment) -> Iterator[Diagnostic]:
    """Report each reported view that holds nothing that answers, then what each view holds."""
    held = _held(request, attachment)
    for view, counts in held.items():
        # A table that names no dbt model holds no columns, and is reported as such already.
        known = request.view_tables.get(view, frozenset()) <= request.known_models
        if known and not any(counts.get(type_name) for type_name in _ANSWERING_TYPES):
            yield D("SST-MEM104", artifact=view, subject=view)
    order = sorted(request.registry.members.values(), key=lambda descriptor: descriptor.clause_position)
    for view, counts in held.items():
        summary = ", ".join(
            f"{counts[descriptor.name]} {descriptor.name}" for descriptor in order if counts.get(descriptor.name)
        )
        yield D("SST-MEM103", artifact=view, value=summary or "no members", subject=view)


def _instruction_conflicts(request: MembershipRequest, attachment: Attachment) -> Iterator[Diagnostic]:
    """Report two reported views that share a table and attach disjoint guidance through one channel.

    For each instruction channel, each view's guidance is the set of custom instructions it
    attaches that write that channel. Two views conflict when both carry guidance in a channel
    and neither view's set holds the other's: the shared table is then described two ways, and
    neither view repeats what the other says. One diagnostic is reported per pair of views.
    """
    channels = _view_channels(request, attachment)
    for first, second in combinations(sorted(request.reported_views), 2):
        shared = sorted(request.view_tables.get(first, frozenset()) & request.view_tables.get(second, frozenset()))
        if shared and any(_diverge(channels[first].get(c, set()), channels[second].get(c, set())) for c in _CHANNELS):
            yield D("SST-MEM105", a=first, b=second, name=shared[0], subject=first)


def _view_channels(request: MembershipRequest, attachment: Attachment) -> dict[ArtifactKey, dict[str, set[str]]]:
    """For each reported view, the custom instructions it attaches, grouped by the channels they write."""
    channels: dict[ArtifactKey, dict[str, set[str]]] = {view: {} for view in request.reported_views}
    for member in request.members:
        if request.registry.members[member.type_name].attaches_by is not AttachRule.VIEW_NAME:
            continue
        name = member.name.casefold()
        for view in attachment.get(member.key, ()):
            for channel in request.instruction_channels.get(name, frozenset()):
                channels.setdefault(view, {}).setdefault(channel, set()).add(name)
    return channels


def _diverge(first: set[str], second: set[str]) -> bool:
    return bool(first) and bool(second) and not first <= second and not second <= first
