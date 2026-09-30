"""Immutable values crossing the compiler's explicit phase boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, TypeAlias

from .artifact_key import artifact_key
from .diagnostic import DiagnosticBag, Origin
from .reference import TemplateCall
from .semantic_view import SemanticView

ArtifactKey: TypeAlias = str
MemberKey: TypeAlias = str


@dataclass(frozen=True, slots=True)
class ParsedView:
    name: str
    origin: Origin
    source_path: str
    source: Mapping[str, object]
    declared_tables: tuple[str, ...]
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class ParsedMember:
    type_name: str
    name: str
    origin: Origin
    source: object
    declared_tables: tuple[str, ...] | None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False

    @property
    def key(self) -> MemberKey:
        return artifact_key(self.type_name, self.name.casefold())


@dataclass(frozen=True, slots=True)
class ParsedProject:
    views: tuple[ParsedView, ...]
    members: tuple[ParsedMember, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def members_by_type(self) -> Mapping[str, tuple[ParsedMember, ...]]:
        grouped: dict[str, list[ParsedMember]] = {}
        for member in self.members:
            grouped.setdefault(member.type_name, []).append(member)
        return MappingProxyType({name: tuple(values) for name, values in grouped.items()})


@dataclass(frozen=True, slots=True)
class ResolvedProject:
    views: tuple[SemanticView, ...]
    attachment: Mapping[MemberKey, tuple[ArtifactKey, ...]]
    custom_instruction_names: Mapping[ArtifactKey, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    diagnostics: DiagnosticBag = DiagnosticBag()


@dataclass(frozen=True, slots=True)
class SemanticViewProject:
    views: tuple[SemanticView, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()
