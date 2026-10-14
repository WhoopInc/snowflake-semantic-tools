"""Immutable tool declarations: the members a project declares, and the catalog of them."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.setting import Setting


class ToolOwnership(Enum):
    """Whether SST creates a tool member, which its `relations:` decide.

    A DEFINE member is an object SST creates and publishes. A REFERENCE member writes
    `relations:`: an object that already exists, which SST locates on each target through them
    and never publishes. Each value is how a diagnostic names the kind.
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
    """One `columns:` entry of a search service member.

    The service's query selects every such column. An agent's search tool over the member
    describes each searchable or filterable one with these values unless the tool authors its
    own columns; when neither sets an id or title column, the tool takes the first column whose
    name ends `_id` or `_name`.

    Attributes:
        name: The column its `column:` names, by `{{ ref('<on model>', '<column>') }}` on a
            defined member and as written on a referenced one.
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
    """One tool member: an object SST defines, or an existing one it references.

    A member is declared once in the project, by a `tool:` file or an entry of a `tools:` list.
    An optional field the entry leaves out is None or empty. Validation, not construction,
    checks which fields a member's type and ownership need.

    Attributes:
        type: The `type:` as written; validation reports one that is not a `ToolKind` value.
        ownership: REFERENCE when the entry writes `relations:`, else DEFINE.
        source_file: The tools file that declares it, relative to the project root.
        on_model: The dbt model `on:` names with `{{ ref('<model>') }}`; None when `on:` is
            absent or is not one such call.
        search_column: The column `search_column:`, or else `on.search_column`, names.
        attribute_columns: The columns `attribute_columns:`, or else `on.attributes`, names.
        title_column, id_column, relative_path_column: The column each names.
        warehouse, target_lag, embedding_model, execute_as: What the DDL renders; None as loaded,
            and filled when the member compiles, from its resolved settings.
        where: A predicate appended to a search service's query, its template calls resolved:
            a column ref becomes the bare column name.
        refresh_mode: How a search service refreshes, `AUTO`, `FULL` or `INCREMENTAL` as
            written; None leaves it to Snowflake, which refreshes incrementally where it can.
        body_file: The file `body:`'s `{{ file() }}` names, relative to the project root; None
            when `body:` names none.
        body: The text of `body_file`; None when it could not be read, which loading reports.
        secrets: Each secret variable's name, mapped to the three-part name of its secret.
        relations: For a referenced member, each target's name mapped to the three-part name
            of the existing object on that target.
        creation_keys: The creation keys the entry declares, sorted; validation reports any on
            a referenced member.
        folder: The directory of its file below `project.tools_dir`, one segment per directory,
            which the `tools:` folder routes name.
        meta: The `meta:` mapping as written, which SST never reads; `meta.owner` names its owner.
        config: The `config:` mapping as written; its keys are the `tools:` settings.
        settings: Its settings, resolved over `config:`, the folder routes and the block root;
            empty when loaded without a configuration.
    """

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
    folder: tuple[str, ...] = ()
    meta: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    config: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    settings: Mapping[str, Setting] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def artifact_key(self) -> str | None:
        """The key a defined member publishes under, `tool:<name>`; None for a referenced one.

        The name is casefolded.
        """
        if self.ownership is ToolOwnership.REFERENCE:
            return None
        return artifact_key("tool", self.name.casefold())

    @property
    def immutable(self) -> bool:
        """Whether its settings resolve `immutable` to true: it may only reference an object."""
        setting = self.settings.get("immutable")
        return setting is not None and setting.value is True

    @property
    def configured_warehouse(self) -> str | None:
        """The warehouse the member's own `config:` names, as written; None when it names none.

        An agent tool over the member runs there when the tool names no warehouse of its own; a
        folder route's or the block root's warehouse is where SST creates the object, not where
        an agent's tool runs.
        """
        value = self.config.get("warehouse")
        return value if isinstance(value, str) else None


@dataclass(frozen=True, slots=True)
class ToolCatalog:
    """Every tool member a project declares, for the target being compiled.

    Attributes:
        members: Every member, in file order, then in the order each file lists them.
        target_name: The `profiles.yml` target being compiled, which picks the relation each
            referenced member resolves to.
        declared_targets: Every target `profiles.yml` declares; a relation naming any other is
            reported.
        diagnostics: What loading the members reported; `validate_tool_catalog` returns these
            together with its own.
    """

    members: tuple[ToolMember, ...]
    target_name: str
    declared_targets: frozenset[str]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def managed(self) -> tuple[ToolMember, ...]:
        """The defined members, which SST publishes."""
        return tuple(member for member in self.members if member.ownership is ToolOwnership.DEFINE)

    @property
    def references(self) -> tuple[ToolMember, ...]:
        """The referenced members, which name objects that already exist."""
        return tuple(member for member in self.members if member.ownership is ToolOwnership.REFERENCE)

    def resolve(self, name: str) -> tuple[ToolMember | None, DiagnosticBag]:
        """Find the member a `{{ tool('<name>') }}` reference names, ignoring case.

        A name declared twice is SST-VAL001, an error; the first declaration answers here.

        Returns:
            The member and no diagnostics, or None and the diagnostic.

        Diagnostics:
            SST-REF010: no member has the name.
        """
        folded = name.casefold()
        match = next((member for member in self.members if member.name.casefold() == folded), None)
        if match is not None:
            return match, DiagnosticBag()
        return None, DiagnosticBag((D("SST-REF010", name=name),))

    def relation(self, member: ToolMember) -> tuple[QualifiedName | None, DiagnosticBag]:
        """Locate a referenced member's existing object on the current target.

        Returns:
            The object's name and no diagnostics; None and no diagnostics for a defined
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
                        name=member.name,
                        target=self.target_name,
                        origin=member.origin,
                        subject=f"tool_ref:{member.name.casefold()}",
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
                        subject=f"tool_ref:{member.name.casefold()}",
                    ),
                )
            )


KNOWN_TOOL_TYPES = frozenset(kind.value for kind in ToolKind)
# The rights a defined procedure runs with, as `EXECUTE AS` takes them, lower-cased with single spaces.
EXECUTE_AS = ("caller", "owner", "restricted caller")
# The routine languages whose DDL requires RUNTIME_VERSION and HANDLER, lower-cased. SQL and
# JavaScript routines take neither clause: the body is the routine.
HANDLER_LANGUAGES = frozenset(("java", "python", "scala"))
# The routine languages whose DDL has no RUNTIME_VERSION, HANDLER, PACKAGES or IMPORTS.
NON_HANDLER_LANGUAGES = frozenset(("javascript", "sql"))
CREATION_KEYS = frozenset(
    (
        "on",
        "where",
        "attribute_columns",
        "refresh_mode",
        "language",
        "runtime_version",
        "handler",
        "body",
        "returns",
        "packages",
        "imports",
        "external_access_integrations",
        "secrets",
    )
)
# The keys a tools file's root and a member may write: `tool:` holds one member, `tools:` a list
# of them. YAML 1.1 reads an unquoted `on:` as the boolean `true`, which a member's loader reads
# as `on:`.
TOOLS_FILE_KEYS = frozenset(("tool", "tools"))
TOOL_MEMBER_KEYS = frozenset(
    (
        *CREATION_KEYS,
        "name",
        "type",
        "description",
        "search_column",
        "title_column",
        "id_column",
        "columns",
        "relative_path_column",
        "signature",
        "relations",
        "meta",
        "config",
    )
)
# The keys a search service member's `columns:` entry may write.
TOOL_COLUMN_KEYS = frozenset(("column", "description", "type", "searchable", "filterable"))
# Keys a member, or an entry of a `tools:` list, wrote before tools lost their groups, with where
# each is written now. Each is an error, so a file still writing one learns it does nothing.
MOVED_TOOL_KEYS: Mapping[str, str] = MappingProxyType(
    {
        "warehouse": "a tool member sets it under config:, as config.warehouse",
        "target_lag": "a tool member sets it under config:, as config.target_lag",
        "embedding_model": "a tool member sets it under config:, as config.embedding_model",
        "execute_as": "a tool member sets it under config:, as config.execute_as",
        "immutable": "a tool member sets it under config:, as config.immutable, or a folder route as +immutable",
        "owner": "a tool member names its owner under meta:, as meta.owner",
        "body_file": "a routine names its body as body: \"{{ file('<path>') }}\", relative to the tool file",
        "columns_and_descriptions": (
            "a search service lists its columns under columns:, each column a {{ ref('<model>', '<column>') }}"
        ),
        "group": "tools have no groups: a member's folder under the tools directory routes it",
        "define": "tools have no groups: write each member as a tool: file or a tools: list entry",
        "reference": "tools have no groups: a member that writes relations: is a reference",
    }
)


def vocabulary_key(value: str) -> str:
    """A value from a closed vocabulary such as `EXECUTE_AS`, spelled as it is: lower-cased, single-spaced.

    Example:
        vocabulary_key("Restricted   CALLER") == "restricted caller"
    """
    return " ".join(value.casefold().split())
