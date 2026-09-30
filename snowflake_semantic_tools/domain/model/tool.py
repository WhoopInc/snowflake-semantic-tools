"""Immutable tool declarations and pure resolution/validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from .artifact_key import artifact_key
from .dbt import DbtCatalog
from .diagnostic import D, Diagnostic, DiagnosticBag, Origin
from .identifier import QualifiedName


class ToolOwnership(Enum):
    DEFINE = "define"
    REFERENCE = "reference"


class ToolKind(Enum):
    CORTEX_SEARCH_SERVICE = "cortex_search_service"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    STAGE = "stage"
    AGENT = "agent"
    GENERIC = "generic"


@dataclass(frozen=True, slots=True)
class ToolParameter:
    name: str
    type: str
    required: bool


@dataclass(frozen=True, slots=True)
class ToolColumn:
    name: str
    description: str
    type: str
    searchable: bool
    filterable: bool


@dataclass(frozen=True, slots=True)
class ToolMember:
    group: str
    name: str
    type: str
    ownership: ToolOwnership
    origin: Origin
    source_file: str
    description: str | None = None
    on_model: str | None = None
    search_column: str | None = None
    attribute_columns: tuple[str, ...] = ()
    title_column: str | None = None
    id_column: str | None = None
    columns: tuple[ToolColumn, ...] = ()
    relative_path_column: str | None = None
    warehouse: str | None = None
    signature: tuple[ToolParameter, ...] = ()
    where: str | None = None
    target_lag: str | None = None
    embedding_model: str | None = None
    language: str | None = None
    runtime_version: str | None = None
    handler: str | None = None
    body_file: str | None = None
    body: str | None = None
    returns: str | None = None
    execute_as: str | None = None
    packages: tuple[str, ...] = ()
    imports: tuple[str, ...] = ()
    external_access_integrations: tuple[str, ...] = ()
    secrets: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    relations: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    creation_keys: tuple[str, ...] = ()

    @property
    def declaration_key(self) -> str:
        return f"{self.group.casefold()}:{self.name.casefold()}"

    @property
    def artifact_key(self) -> str | None:
        if self.ownership is ToolOwnership.REFERENCE:
            return None
        return artifact_key("tool", self.name.casefold())


@dataclass(frozen=True, slots=True)
class ToolGroup:
    name: str
    origin: Origin
    source_file: str
    description: str | None = None
    owner: str | None = None
    immutable: bool = False
    members: tuple[ToolMember, ...] = ()


@dataclass(frozen=True, slots=True)
class ToolCatalog:
    groups: tuple[ToolGroup, ...]
    target_name: str
    declared_targets: frozenset[str]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def members(self) -> tuple[ToolMember, ...]:
        return tuple(member for group in self.groups for member in group.members)

    @property
    def managed(self) -> tuple[ToolMember, ...]:
        return tuple(member for member in self.members if member.ownership is ToolOwnership.DEFINE)

    @property
    def references(self) -> tuple[ToolMember, ...]:
        return tuple(member for member in self.members if member.ownership is ToolOwnership.REFERENCE)

    def resolve(self, *args: str) -> tuple[ToolMember | None, DiagnosticBag]:
        if len(args) == 2:
            group_name, member_name = (value.casefold() for value in args)
            matches = tuple(
                member
                for member in self.members
                if member.group.casefold() == group_name and member.name.casefold() == member_name
            )
        elif len(args) == 1:
            member_name = args[0].casefold()
            matches = tuple(member for member in self.members if member.name.casefold() == member_name)
        else:
            matches = ()
        if len(matches) == 1:
            return matches[0], DiagnosticBag()
        group = args[0] if len(args) == 2 else "*"
        name = args[-1] if args else ""
        return None, DiagnosticBag((D("SST-REF010", group=group, name=name),))

    def relation(self, member: ToolMember) -> tuple[QualifiedName | None, DiagnosticBag]:
        if member.ownership is ToolOwnership.DEFINE:
            return None, DiagnosticBag()
        value = member.relations.get(self.target_name)
        if value is None:
            return None, DiagnosticBag(
                (
                    D(
                        "SST-REF018",
                        ref_function="tool",
                        name=f"{member.group}', '{member.name}",
                        target=self.target_name,
                        origin=member.origin,
                        subject=f"tool_ref:{member.declaration_key}",
                    ),
                )
            )
        try:
            return QualifiedName.parse(value), DiagnosticBag()
        except ValueError:
            return None, DiagnosticBag(
                (
                    D(
                        "SST-REF019",
                        value=value,
                        origin=member.origin,
                        subject=f"tool_ref:{member.declaration_key}",
                    ),
                )
            )


KNOWN_TOOL_TYPES = frozenset(kind.value for kind in ToolKind)
CREATION_KEYS = frozenset(
    (
        "on",
        "where",
        "attribute_columns",
        "target_lag",
        "embedding_model",
        "language",
        "runtime_version",
        "handler",
        "body_file",
        "returns",
        "packages",
        "imports",
        "external_access_integrations",
        "secrets",
    )
)


def validate_tool_catalog(catalog: ToolCatalog, dbt: DbtCatalog) -> DiagnosticBag:
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    groups_by_name: dict[str, list[ToolGroup]] = {}
    members_by_name: dict[str, list[ToolMember]] = {}
    for group in catalog.groups:
        groups_by_name.setdefault(group.name.casefold(), []).append(group)
        if group.immutable and any(member.ownership is ToolOwnership.DEFINE for member in group.members):
            diagnostics.append(D("SST-VAL606", a=group.name, origin=group.origin, subject=f"tool_group:{group.name}"))
        within: dict[str, list[ToolMember]] = {}
        for member in group.members:
            within.setdefault(member.name.casefold(), []).append(member)
            members_by_name.setdefault(member.name.casefold(), []).append(member)
            diagnostics.extend(_validate_member(member, catalog, dbt))
        for duplicate in within.values():
            if len(duplicate) > 1:
                diagnostics.append(
                    D(
                        "SST-VAL601",
                        a=group.name,
                        name=duplicate[0].name,
                        origin=duplicate[0].origin,
                        related=tuple(member.origin for member in duplicate[1:]),
                    )
                )
    for duplicate_groups in groups_by_name.values():
        if len(duplicate_groups) > 1:
            diagnostics.append(
                D(
                    "SST-VAL001",
                    type="tool group",
                    name=duplicate_groups[0].name,
                    origin=duplicate_groups[0].origin,
                    related=tuple(group.origin for group in duplicate_groups[1:]),
                )
            )
    for duplicate_members in members_by_name.values():
        groups = tuple(dict.fromkeys(member.group for member in duplicate_members))
        if len(groups) > 1:
            diagnostics.append(
                D(
                    "SST-VAL602",
                    name=duplicate_members[0].name,
                    a=groups[0],
                    b=groups[1],
                    origin=duplicate_members[0].origin,
                )
            )
    return DiagnosticBag(sorted(diagnostics, key=lambda item: (item.origin.file if item.origin else "", item.code)))


def _validate_member(member: ToolMember, catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    subject = artifact_key("tool", member.name.casefold())
    if member.type not in KNOWN_TOOL_TYPES:
        diagnostics.append(
            D(
                "SST-VAL603",
                name=member.name,
                found=member.type,
                expected=", ".join(sorted(KNOWN_TOOL_TYPES)),
                origin=member.origin,
                subject=subject,
            )
        )
    if member.ownership is ToolOwnership.DEFINE:
        if not member.on_model and not member.body_file and member.type != ToolKind.STAGE.value:
            diagnostics.append(D("SST-VAL604", name=member.name, origin=member.origin, subject=subject))
        if member.relations:
            diagnostics.append(
                D("SST-VAL605", name=member.name, key="relations", origin=member.origin, subject=subject)
            )
    else:
        if not member.relations:
            diagnostics.append(
                D("SST-PRS002", artifact=subject, field="relations", origin=member.origin, subject=subject)
            )
        for key in member.creation_keys:
            diagnostics.append(D("SST-VAL605", name=member.name, key=key, origin=member.origin, subject=subject))
        for target_name, value in member.relations.items():
            if target_name not in catalog.declared_targets:
                diagnostics.append(
                    D(
                        "SST-REF018",
                        ref_function="tool target",
                        name=target_name,
                        target="profiles.yml",
                        origin=member.origin,
                        subject=subject,
                    )
                )
            try:
                QualifiedName.parse(value)
            except ValueError:
                diagnostics.append(D("SST-REF019", value=value, origin=member.origin, subject=subject))
        if catalog.target_name not in member.relations:
            diagnostics.append(
                D(
                    "SST-REF018",
                    ref_function="tool",
                    name=f"{member.group}', '{member.name}",
                    target=catalog.target_name,
                    origin=member.origin,
                    subject=subject,
                )
            )
        if (
            not next(group.immutable for group in catalog.groups if group.name == member.group)
            and "dev" in member.relations
            and "prod" in member.relations
            and member.relations["dev"].casefold() == member.relations["prod"].casefold()
        ):
            diagnostics.append(
                D(
                    "SST-REF023",
                    ref_function="tool",
                    name=f"{member.group}', '{member.name}",
                    value=member.relations["dev"],
                    origin=member.origin,
                    subject=subject,
                )
            )
    if any(parameter.type.casefold() == "object" for parameter in member.signature):
        parameter = next(parameter for parameter in member.signature if parameter.type.casefold() == "object")
        diagnostics.append(
            D(
                "SST-PRS032",
                artifact=subject,
                field=parameter.name,
                found=parameter.type,
                origin=member.origin,
                subject=subject,
            )
        )
    if member.ownership is ToolOwnership.DEFINE and member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        model = dbt.model(member.on_model or "")
        if model is None:
            diagnostics.append(
                D("SST-VAL608", name=member.name, value=member.on_model or "", origin=member.origin, subject=subject)
            )
        else:
            for column in (member.search_column, *member.attribute_columns):
                if column and model.column(column) is None:
                    diagnostics.append(
                        D(
                            "SST-VAL609",
                            name=member.name,
                            column=column,
                            value=model.name,
                            origin=member.origin,
                            subject=subject,
                        )
                    )
    if member.ownership is ToolOwnership.DEFINE and member.body_file:
        if member.body is None:
            diagnostics.append(
                D("SST-LOD018", file=member.source_file, path=member.body_file, origin=member.origin, subject=subject)
            )
        elif not member.body.strip():
            diagnostics.append(
                D("SST-LOD019", path=member.body_file, file=member.source_file, origin=member.origin, subject=subject)
            )
    return tuple(diagnostics)
