"""Attach semantic members to the views they belong to: pure functions, with no I/O.

`attach_members` attaches by table membership alone: a member that needs tables belongs
to every view that holds all of them. `attach_view_members` then places the members a
view names, and attaches a member that depends on others only where all of them are
attached. A member's tables are casefolded, so each view's must be too, and a member's
views are listed in artifact-key order.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.project import ArtifactKey, MemberKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import AttachRule, Registry


def effective_tables(member: ParsedMember) -> frozenset[str]:
    """Return the tables a member needs, casefolded.

    Declared tables win, even an empty declaration; without one, the tables are the first
    argument of each `ref()` the member's templates call. Empty when the member names none,
    which attaches it to no view by table membership.
    """
    if member.declared_tables is not None:
        return frozenset(table.casefold() for table in member.declared_tables)
    return frozenset(call.args[0].casefold() for call in member.template_calls if call.function == "ref" and call.args)


def attach_members(
    view_tables: Mapping[ArtifactKey, frozenset[str]],
    members: Iterable[ParsedMember],
    registry: Registry,
) -> Mapping[MemberKey, tuple[ArtifactKey, ...]]:
    """Attach each member to every view that holds all the tables it needs, by table membership alone.

    A poisoned member, a member that needs no table, and a member of a type the registry
    attaches by view name attach to no view here; `attach_view_members` places the last.

    Args:
        view_tables: Each view's tables, casefolded, by the view's artifact key.

    Returns:
        A read-only mapping with an entry for every member's key: the keys of the views it
        attaches to, in artifact-key order; empty when it attaches to none.

    Raises:
        KeyError: a member's type is not in the registry.
    """
    attached: dict[MemberKey, tuple[ArtifactKey, ...]] = {}
    for member in members:
        descriptor = registry.members[member.type_name]
        if member.poisoned:
            attached[member.key] = ()
            continue
        if descriptor.attaches_by is AttachRule.VIEW_NAME:
            attached[member.key] = ()
            continue
        needed = effective_tables(member)
        attached[member.key] = tuple(
            artifact_key for artifact_key, tables in sorted(view_tables.items()) if needed and needed.issubset(tables)
        )
    return MappingProxyType(attached)


def attach_view_members(
    view_tables: Mapping[ArtifactKey, frozenset[str]],
    members: Iterable[ParsedMember],
    registry: Registry,
    *,
    view_named_members: Mapping[ArtifactKey, frozenset[str]] | None = None,
    metric_dependencies: Mapping[MemberKey, tuple[MemberKey, ...]] | None = None,
) -> Mapping[MemberKey, tuple[ArtifactKey, ...]]:
    """Attach every semantic member exactly once, including view/name and metric dependency rules."""
    candidates = tuple(members)
    attached = dict(attach_members(view_tables, candidates, registry))
    named = view_named_members or {}
    for member in candidates:
        descriptor = registry.members[member.type_name]
        if descriptor.attaches_by is not AttachRule.VIEW_NAME:
            continue
        attached[member.key] = tuple(
            artifact_key for artifact_key, names in sorted(named.items()) if member.name.casefold() in names
        )

    dependencies = metric_dependencies or {}
    changed = True
    while changed:
        changed = False
        for member in candidates:
            dependency_keys = dependencies.get(member.key, ())
            if not dependency_keys:
                continue
            initial = attached.get(member.key, ())
            allowed = set(initial) if effective_tables(member) else set(view_tables)
            destinations = tuple(
                artifact_key
                for artifact_key in sorted(view_tables)
                if artifact_key in allowed
                and all(artifact_key in attached.get(dependency, ()) for dependency in dependency_keys)
            )
            if destinations != attached.get(member.key, ()):
                attached[member.key] = destinations
                changed = True
    return MappingProxyType(attached)
