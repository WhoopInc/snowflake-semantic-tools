"""The values member resolution reads: the request, and what it knows of each member."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.project import ArtifactKey, MemberKey, ParsedMember
from snowflake_semantic_tools.domain.model.registry import Registry
from snowflake_semantic_tools.domain.model.semantic_view import ViewScope

Attachment = Mapping[MemberKey, tuple[ArtifactKey, ...]]


@dataclass(frozen=True, slots=True)
class MemberFacts:
    """What the membership checks know of a member beyond its tables.

    Attributes:
        derived: The member is a derived metric, built from other metrics, so it is view-scoped.
        private: The member is a metric that cannot be selected from outside its view.
        referenced_metrics: The casefolded names of the metrics its expression or SQL references.
        using_relationships: The casefolded names of the relationships it joins through.
    """

    derived: bool = False
    private: bool = False
    referenced_metrics: tuple[str, ...] = ()
    using_relationships: tuple[str, ...] = ()


NO_FACTS = MemberFacts()


@dataclass(frozen=True, slots=True)
class MembershipRequest:
    """Everything member resolution reads; none of it is changed.

    Attributes:
        members: Every member, the poisoned ones marked so.
        view_tables: Each view's casefolded tables, by the view's artifact key.
        reported_views: The keys of the views to report on: those that will be built.
        view_named_members: For each view, the casefolded names of the members it names.
        known_models: The casefolded names of the dbt models.
        facts: What the checks need to know of a member beyond its tables, by member key; a
            member without an entry has `NO_FACTS`.
        joins: Each declared relationship's two tables, casefolded.
        instruction_channels: For each custom instruction's casefolded name, the instruction
            channels its text writes.
        unresolved: For each poisoned member's key, how many of its references did not resolve.
        view_scopes: Each view's include or exclude lists, by the view's artifact key; a view
            without an entry admits every member its tables attach.
    """

    members: tuple[ParsedMember, ...]
    view_tables: Mapping[ArtifactKey, frozenset[str]]
    registry: Registry
    reported_views: frozenset[ArtifactKey] = frozenset()
    view_named_members: Mapping[ArtifactKey, frozenset[str]] = field(default_factory=lambda: MappingProxyType({}))
    known_models: frozenset[str] = frozenset()
    facts: Mapping[MemberKey, MemberFacts] = field(default_factory=lambda: MappingProxyType({}))
    joins: tuple[tuple[str, str], ...] = ()
    instruction_channels: Mapping[str, frozenset[str]] = field(default_factory=lambda: MappingProxyType({}))
    unresolved: Mapping[MemberKey, int] = field(default_factory=lambda: MappingProxyType({}))
    view_scopes: Mapping[ArtifactKey, ViewScope] = field(default_factory=lambda: MappingProxyType({}))

    def facts_of(self, member: ParsedMember) -> MemberFacts:
        """Return what is known of `member` beyond its tables."""
        return self.facts.get(member.key, NO_FACTS)

    def metric_dependencies(self) -> dict[MemberKey, tuple[MemberKey, ...]]:
        """Each metric's key, mapped to the keys of the metrics it references."""
        return {
            key: tuple(artifact_key("metric", name) for name in facts.referenced_metrics)
            for key, facts in self.facts.items()
            if key.startswith("metric:") and facts.referenced_metrics
        }
