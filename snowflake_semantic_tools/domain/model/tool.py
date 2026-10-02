"""Immutable tool declarations: members, groups, and the catalog a project declares."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


class ToolOwnership(Enum):
    """Which list of its group a tool member is declared in, and so whether SST creates it.

    A DEFINE member is an object SST creates and publishes. A REFERENCE member is an object that
    already exists, which SST locates on each target through its `relations` and never publishes.
    Each value is the group's YAML key for the list.
    """

    DEFINE = "define"
    REFERENCE = "reference"


class ToolKind(Enum):
    """The `type:` values a tool member may declare.

    `render_tool` renders a search service, procedure, function, or stage, and refuses an
    `agent` or `generic` member.
    """

    CORTEX_SEARCH_SERVICE = "cortex_search_service"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    STAGE = "stage"
    AGENT = "agent"
    GENERIC = "generic"


@dataclass(frozen=True, slots=True)
class ToolParameter:
    """One `signature:` parameter of a procedure or function member.

    Attributes:
        name: Rendered as an identifier, so upper-cased unless written quoted.
        type: The SQL type as written, rendered verbatim.
        required: The parameter's `required:` flag; False when unset.
    """

    name: str
    type: str
    required: bool


@dataclass(frozen=True, slots=True)
class ToolColumn:
    """One `columns_and_descriptions:` entry of a search service member.

    The service's query selects every such column. An agent's search tool over the member
    describes each searchable or filterable one with these values unless the tool authors its
    own columns; when neither sets an id or title column, the tool takes the first column whose
    name ends `_id` or `_name`.

    Attributes:
        description: "" when unset.
        type: The column's type as written; "" when unset.
    """

    name: str
    description: str
    type: str
    searchable: bool
    filterable: bool


@dataclass(frozen=True, slots=True)
class ToolMember:
    """One member of a tool group: an object SST defines, or an existing one it references.

    An optional field the entry leaves out is None or empty. Validation, not construction,
    checks which fields a member's type and ownership need.

    Attributes:
        group: The name of the group that declares it, as written.
        type: The `type:` as written; validation reports one that is not a `ToolKind` value.
        source_file: The tools file that declares it, relative to the project root.
        on_model: The dbt model `on:` names with `{{ ref('<model>') }}`; None when `on:` is
            absent or is not one such call.
        search_column: From `search_column:`, or else from `on.search_column`.
        attribute_columns: From `attribute_columns:`, or else from `on.attributes`.
        where: A predicate appended verbatim to a search service's query.
        refresh_mode: How a search service refreshes, `AUTO`, `FULL` or `INCREMENTAL` as
            written; None leaves it to Snowflake, which refreshes incrementally where it can.
        body: The text `body_file:` names; None when it could not be read, which validation
            reports.
        secrets: Each secret variable's name, mapped to the three-part name of its secret.
        relations: For a `reference:` member, each target's name mapped to the three-part name
            of the existing object on that target.
        creation_keys: The creation keys the entry declares, sorted; validation reports any on
            a `reference:` member.
    """

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
    refresh_mode: str | None = None
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
        """`<group>:<name>`, both casefolded; diagnostics on its relations name it in their subject."""
        return f"{self.group.casefold()}:{self.name.casefold()}"

    @property
    def artifact_key(self) -> str | None:
        """The key a `define:` member publishes under, `tool:<name>`; None for a `reference:` one.

        The name is casefolded, and the group is not part of the key.
        """
        if self.ownership is ToolOwnership.REFERENCE:
            return None
        return artifact_key("tool", self.name.casefold())


@dataclass(frozen=True, slots=True)
class ToolGroup:
    """One entry of a tools file's `tools:` list: a named group of tool members.

    Attributes:
        name: The `group:` value as written.
        immutable: The group only references objects: it may declare no `define:` members, and
            its references may name one object for both dev and prod.
    """

    name: str
    origin: Origin
    source_file: str
    description: str | None = None
    owner: str | None = None
    immutable: bool = False
    members: tuple[ToolMember, ...] = ()


@dataclass(frozen=True, slots=True)
class ToolCatalog:
    """Every tool group a project declares, for the target being compiled.

    Attributes:
        target_name: The `profiles.yml` target being compiled, which picks the relation each
            `reference:` member resolves to.
        declared_targets: Every target `profiles.yml` declares; a relation naming any other is
            reported.
        diagnostics: What loading the groups reported; `validate_tool_catalog` returns these
            together with its own.
    """

    groups: tuple[ToolGroup, ...]
    target_name: str
    declared_targets: frozenset[str]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def members(self) -> tuple[ToolMember, ...]:
        """Every member of every group, group by group."""
        return tuple(member for group in self.groups for member in group.members)

    @property
    def managed(self) -> tuple[ToolMember, ...]:
        """The `define:` members, which SST publishes."""
        return tuple(member for member in self.members if member.ownership is ToolOwnership.DEFINE)

    @property
    def references(self) -> tuple[ToolMember, ...]:
        """The `reference:` members, which name objects that already exist."""
        return tuple(member for member in self.members if member.ownership is ToolOwnership.REFERENCE)

    def resolve(self, *args: str) -> tuple[ToolMember | None, DiagnosticBag]:
        """Find the one member a `tool()` reference names, ignoring case.

        Args:
            args: The reference's arguments: a group and a member name, or a member name alone,
                which matches in any group. Any other count matches nothing.

        Returns:
            The member and no diagnostics, or None and the diagnostic.

        Diagnostics:
            SST-REF010: no member matches, or more than one does.
        """
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
        """Locate a `reference:` member's existing object on the current target.

        Returns:
            The object's name and no diagnostics; None and no diagnostics for a `define:`
            member, which has no relation; or None and the diagnostic that prevents it.

        Diagnostics:
            SST-REF018: the member has no relation for the current target.
            SST-REF019: the relation for the current target is not a three-part name.
        """
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
        "refresh_mode",
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
