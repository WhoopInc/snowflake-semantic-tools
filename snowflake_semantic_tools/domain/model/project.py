"""Immutable values crossing the compiler's explicit phase boundaries."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TypeAlias

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.reference import TemplateCall
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView

ArtifactKey: TypeAlias = str
MemberKey: TypeAlias = str


@dataclass(frozen=True, slots=True)
class ParsedView:
    """One `semantic_views:` entry as authored, before members attach to it.

    Attributes:
        source: The entry's YAML mapping, as authored.
        declared_tables: The casefolded dbt model names its `tables:` list names; empty when the
            list cannot be read.
        template_calls: The template calls its `tables:` entries make.
        poisoned: It is already reported as broken, so it is not built.
    """

    name: str
    origin: Origin
    source_path: str
    source: Mapping[str, object]
    declared_tables: tuple[str, ...]
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class ParsedMember:
    """One semantic member before it attaches: a metric, filter, relationship, column, and so on.

    Attributes:
        type_name: The member type's registry name, such as `metric`.
        name: As authored, or `<model>.<column>` for a fact or dimension.
        source: The record it was parsed into, whose type depends on `type_name`.
        declared_tables: The tables it needs a view to hold; None when it declares none, so its
            tables are the models its `ref()` calls name.
        template_calls: The template calls in its expression or SQL.
        poisoned: It is already reported as broken, so it attaches to no view.
    """

    type_name: str
    name: str
    origin: Origin
    source: object
    declared_tables: tuple[str, ...] | None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False

    @property
    def key(self) -> MemberKey:
        """The member's key: its type name and its casefolded name."""
        return artifact_key(self.type_name, self.name.casefold())


@dataclass(frozen=True, slots=True)
class ParsedProject:
    """Every semantic view and member of a project, unresolved, with what parsing reported."""

    views: tuple[ParsedView, ...]
    members: tuple[ParsedMember, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def members_by_type(self) -> Mapping[str, tuple[ParsedMember, ...]]:
        """The members grouped by type name, each group in member order; a type with none is absent."""
        grouped: dict[str, list[ParsedMember]] = {}
        for member in self.members:
            grouped.setdefault(member.type_name, []).append(member)
        return MappingProxyType({name: tuple(values) for name, values in grouped.items()})


@dataclass(frozen=True, slots=True)
class ResolvedProject:
    """The views built from a parsed project, with the views each member attached to.

    Attributes:
        attachment: Each member's key, mapped to the keys of the views it attaches to, sorted;
            a member that attaches nowhere, a poisoned one included, maps to ().
        custom_instruction_names: By casefolded view key, the casefolded names of the custom
            instructions the view attaches, sorted.
    """

    views: tuple[SemanticView, ...]
    attachment: Mapping[MemberKey, tuple[ArtifactKey, ...]]
    custom_instruction_names: Mapping[ArtifactKey, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    diagnostics: DiagnosticBag = DiagnosticBag()


@dataclass(frozen=True, slots=True)
class SemanticViewProject:
    """What a semantic-view source returns: the views that built, and every diagnostic of the load.

    A view a diagnostic kept from building is absent rather than partly built.
    """

    views: tuple[SemanticView, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()
