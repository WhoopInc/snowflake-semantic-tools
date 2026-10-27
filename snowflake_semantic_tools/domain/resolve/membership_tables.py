"""Membership checks over a member's own tables, and what the metrics it references reach.

Only a member authored in SST's own files is checked, and only one that is not poisoned: a
fact or dimension names exactly its model's table, and a poisoned member was reported where it
broke. A member a view attaches by name declares no tables to check.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.project import ArtifactKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import AttachRule, MemberSource, Registry
from snowflake_semantic_tools.domain.resolve.membership_model import MembershipRequest


def authored_members(request: MembershipRequest) -> Iterator[ParsedMember]:
    """Yield, in member order, each unpoisoned member authored in SST's files and attached by tables."""
    for member in request.members:
        if not member.poisoned and _by_tables(member, request.registry):
            yield member


def _by_tables(member: ParsedMember, registry: Registry) -> bool:
    descriptor = registry.members[member.type_name]
    return descriptor.source is MemberSource.FILES and descriptor.attaches_by is AttachRule.TABLE_MEMBERSHIP


def inferred_tables(member: ParsedMember) -> frozenset[str]:
    """Return the casefolded models the member's `ref()` calls name: the tables its expression reaches."""
    return frozenset(call.args[0].casefold() for call in member.template_calls if call.function == "ref" and call.args)


def table_diagnostics(request: MembershipRequest) -> tuple[Diagnostic, ...]:
    """Check each authored member's tables against its expression, the views and the models.

    A metric's, filter's or verified query's tables are the models its templates name, so a
    member that names none is an error.

    Diagnostics, member by member in this order:
        SST-MEM001: one of its tables is a dbt model that no view lists.
        SST-MEM002: the member names no table.
        SST-MEM007: a metric it references reaches a table its own tables leave out.
    """
    listed = frozenset(table for tables in request.view_tables.values() for table in tables)
    transitive = _transitive_tables(request)
    diagnostics: list[Diagnostic] = []
    for member in authored_members(request):
        facts = request.facts_of(member)
        declared = member.declared_tables or ()
        if not facts.derived:
            diagnostics.extend(_unlisted(member, declared, listed, request.known_models))
        if not declared and not inferred_tables(member) and not facts.derived:
            diagnostics.append(_member_code("SST-MEM002", member))
        reached = transitive.get(member.key, frozenset()) - set(declared)
        if declared and not facts.derived and reached:
            diagnostics.append(_member_code("SST-MEM007", member, outside=", ".join(sorted(reached))))
    return tuple(diagnostics)


def _member_code(code: str, member: ParsedMember, **context: Any) -> Diagnostic:
    return D(code, member=member.key, subject=member.key, origin=member.origin, **context)


def _unlisted(
    member: ParsedMember, declared: tuple[str, ...], listed: frozenset[str], known_models: frozenset[str]
) -> list[Diagnostic]:
    """Report each declared table that is a dbt model and that no view lists (SST-MEM001).

    A table that is no dbt model is its `ref()`'s to report (SST-REF001), so it is left out here.
    """
    return [
        _member_code("SST-MEM001", member, name=table, type="semantic_view")
        for table in dict.fromkeys(declared)
        if table in known_models and table not in listed
    ]


def _transitive_tables(request: MembershipRequest) -> Mapping[str, frozenset[str]]:
    """Return, for each metric, the tables of every metric its references reach, however indirectly.

    A metric's own tables are its declared ones, or its inferred ones when it declares none. The
    walk is breadth-first over the `metric()` references and visits each metric once, so a
    cycle ends it rather than looping.
    """
    own: dict[ArtifactKey, frozenset[str]] = {}
    for member in request.members:
        if member.type_name == "metric":
            own[member.key] = frozenset(member.declared_tables or inferred_tables(member))
    dependencies = request.metric_dependencies()
    reached: dict[str, frozenset[str]] = {}
    for key in own:
        seen: set[str] = set()
        frontier = list(dependencies.get(key, ()))
        tables: set[str] = set()
        while frontier:
            current = frontier.pop()
            if current in seen or current not in own:
                continue
            seen.add(current)
            tables |= own[current]
            frontier.extend(dependencies.get(current, ()))
        reached[key] = frozenset(tables)
    return reached
