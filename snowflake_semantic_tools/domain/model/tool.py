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
from .validation import Emitter


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
    """Check every tool group and member, returning the catalog's load diagnostics with the new ones.

    Returns:
        Every diagnostic, sorted by origin file (none first), then code; ties keep the
        order they were found in.

    Diagnostics:
        SST-VAL606: an immutable group declares `define:` members.
        SST-VAL603: a member's type is not a known tool type.
        SST-VAL604: a `define:` member declares neither `on:` nor `body_file:`, and is not a stage.
        SST-VAL605: a `define:` member declares `relations`, or a `reference:` member a creation key.
        SST-PRS002: a `reference:` member declares no `relations`.
        SST-REF018: a relation names a target `profiles.yml` does not declare, or none names the current one.
        SST-REF019: a relation is not a three-part name.
        SST-REF023: a mutable group's reference resolves to one object for both dev and prod.
        SST-PRS032: a signature parameter has type OBJECT; only the first is reported.
        SST-VAL608: a defined search service's `on:` is not a dbt model.
        SST-VAL609: a search or attribute column is not on the search service's model.
        SST-LOD018: a defined member's `body_file:` does not exist.
        SST-LOD019: a defined member's `body_file:` is empty.
        SST-VAL601: a group declares one member name twice, ignoring case.
        SST-VAL001: a group name is declared twice, ignoring case.
        SST-VAL602: one member name is declared in two groups.
    """
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    for group in catalog.groups:
        diagnostics.extend(_validate_group(group, catalog, dbt))
    diagnostics.extend(_duplicate_groups(catalog.groups))
    diagnostics.extend(_names_in_several_groups(catalog.groups))
    return DiagnosticBag(sorted(diagnostics, key=lambda item: (item.origin.file if item.origin else "", item.code)))


def _validate_group(group: ToolGroup, catalog: ToolCatalog, dbt: DbtCatalog) -> list[Diagnostic]:
    """Check one group, then each of its members, then member names it repeats."""
    diagnostics: list[Diagnostic] = []
    if group.immutable and any(member.ownership is ToolOwnership.DEFINE for member in group.members):
        diagnostics.append(D("SST-VAL606", a=group.name, origin=group.origin, subject=f"tool_group:{group.name}"))
    within: dict[str, list[ToolMember]] = {}
    for member in group.members:
        within.setdefault(member.name.casefold(), []).append(member)
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
    return diagnostics


def _duplicate_groups(groups: tuple[ToolGroup, ...]) -> list[Diagnostic]:
    """Report each group name declared more than once, at its first declaration."""
    groups_by_name: dict[str, list[ToolGroup]] = {}
    for group in groups:
        groups_by_name.setdefault(group.name.casefold(), []).append(group)
    return [
        D(
            "SST-VAL001",
            type="tool group",
            name=duplicate_groups[0].name,
            origin=duplicate_groups[0].origin,
            related=tuple(group.origin for group in duplicate_groups[1:]),
        )
        for duplicate_groups in groups_by_name.values()
        if len(duplicate_groups) > 1
    ]


def _names_in_several_groups(groups: tuple[ToolGroup, ...]) -> list[Diagnostic]:
    """Report each member name declared in more than one group, naming the first two groups."""
    members_by_name: dict[str, list[ToolMember]] = {}
    for group in groups:
        for member in group.members:
            members_by_name.setdefault(member.name.casefold(), []).append(member)
    diagnostics: list[Diagnostic] = []
    for duplicate_members in members_by_name.values():
        owners = tuple(dict.fromkeys(member.group for member in duplicate_members))
        if len(owners) > 1:
            diagnostics.append(
                D(
                    "SST-VAL602",
                    name=duplicate_members[0].name,
                    a=owners[0],
                    b=owners[1],
                    origin=duplicate_members[0].origin,
                )
            )
    return diagnostics


def _validate_member(member: ToolMember, catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Run every member rule, in this order; each reports against the member's key and origin."""
    subject = artifact_key("tool", member.name.casefold())
    return (
        *_known_type(member, subject),
        *_define_fields(member, subject),
        *_reference_fields(member, subject),
        *_reference_relations(member, catalog, subject),
        *_object_parameter(member, subject),
        *_search_columns(member, dbt, subject),
        *_body_file(member, subject),
    )


def _known_type(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a type that is not a known tool type."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.type not in KNOWN_TOOL_TYPES:
        emit("SST-VAL603", name=member.name, found=member.type, expected=", ".join(sorted(KNOWN_TOOL_TYPES)))
    return emit.diagnostics


def _define_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a `define:` member with nothing to create it from, or with `relations`."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE:
        return emit.diagnostics
    if not member.on_model and not member.body_file and member.type != ToolKind.STAGE.value:
        emit("SST-VAL604", name=member.name)
    if member.relations:
        emit("SST-VAL605", name=member.name, key="relations")
    return emit.diagnostics


def _reference_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a `reference:` member without `relations`, then each creation key it declares."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is ToolOwnership.DEFINE:
        return emit.diagnostics
    if not member.relations:
        emit("SST-PRS002", artifact=subject, field="relations")
    for key in member.creation_keys:
        emit("SST-VAL605", name=member.name, key=key)
    return emit.diagnostics


def _reference_relations(member: ToolMember, catalog: ToolCatalog, subject: str) -> tuple[Diagnostic, ...]:
    """Check each relation of a `reference:` member, then its current target and shared object.

    Each relation's undeclared target is reported before its malformed name.
    """
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is ToolOwnership.DEFINE:
        return emit.diagnostics
    for target_name, value in member.relations.items():
        if target_name not in catalog.declared_targets:
            emit("SST-REF018", ref_function="tool target", name=target_name, target="profiles.yml")
        try:
            QualifiedName.parse(value)
        except ValueError:
            emit("SST-REF019", value=value)
    if catalog.target_name not in member.relations:
        emit("SST-REF018", ref_function="tool", name=f"{member.group}', '{member.name}", target=catalog.target_name)
    if _shares_one_object(member, catalog):
        emit("SST-REF023", ref_function="tool", name=f"{member.group}', '{member.name}", value=member.relations["dev"])
    return emit.diagnostics


def _shares_one_object(member: ToolMember, catalog: ToolCatalog) -> bool:
    """Return whether dev and prod name one object although the member's group is not immutable.

    Mutability comes from the first group declared under the member's exact group name.
    """
    return (
        not next(group.immutable for group in catalog.groups if group.name == member.group)
        and "dev" in member.relations
        and "prod" in member.relations
        and member.relations["dev"].casefold() == member.relations["prod"].casefold()
    )


def _object_parameter(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report the first signature parameter typed OBJECT, which tool input schemas cannot carry."""
    emit = Emitter(subject=subject, origin=member.origin)
    parameter = next((parameter for parameter in member.signature if parameter.type.casefold() == "object"), None)
    if parameter is not None:
        emit("SST-PRS032", artifact=subject, field=parameter.name, found=parameter.type)
    return emit.diagnostics


def _search_columns(member: ToolMember, dbt: DbtCatalog, subject: str) -> tuple[Diagnostic, ...]:
    """Check that a defined search service reads a dbt model that has its search and attribute columns."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE or member.type != ToolKind.CORTEX_SEARCH_SERVICE.value:
        return emit.diagnostics
    model = dbt.model(member.on_model or "")
    if model is None:
        emit("SST-VAL608", name=member.name, value=member.on_model or "")
        return emit.diagnostics
    for column in (member.search_column, *member.attribute_columns):
        if column and model.column(column) is None:
            emit("SST-VAL609", name=member.name, column=column, value=model.name)
    return emit.diagnostics


def _body_file(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a defined member's `body_file:` that could not be read, or that is blank."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE or not member.body_file:
        return emit.diagnostics
    if member.body is None:
        emit("SST-LOD018", file=member.source_file, path=member.body_file)
    elif not member.body.strip():
        emit("SST-LOD019", path=member.body_file, file=member.source_file)
    return emit.diagnostics
