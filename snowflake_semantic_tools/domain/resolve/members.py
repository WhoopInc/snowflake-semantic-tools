"""Pure member attachment for Milestone 1 semantic views."""

from __future__ import annotations

from types import MappingProxyType
from typing import Iterable, Mapping

from ..model.project import ArtifactKey, MemberKey, ParsedMember
from ..model.registry import AttachRule, Registry


def effective_tables(member: ParsedMember) -> frozenset[str]:
    if member.declared_tables is not None:
        return frozenset(table.casefold() for table in member.declared_tables)
    return frozenset(call.args[0].casefold() for call in member.template_calls if call.function == "ref" and call.args)


def attach_members(
    view_tables: Mapping[ArtifactKey, frozenset[str]],
    members: Iterable[ParsedMember],
    registry: Registry,
) -> Mapping[MemberKey, tuple[ArtifactKey, ...]]:
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
