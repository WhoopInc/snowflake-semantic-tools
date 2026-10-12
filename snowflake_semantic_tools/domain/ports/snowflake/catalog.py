"""The read-only port to what exists in Snowflake, how it is defined, and who the session is."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker, ShowRow


@dataclass(frozen=True, slots=True)
class ExtensionObservation:
    """One `SHOW CORTEX EXTENSIONS` row."""

    qualified_name: QualifiedName
    extension_type: str
    comment: str | None
    owner: str
    effective_version: str | None = None
    latest_certified_version: str | None = None


@dataclass(frozen=True, slots=True)
class ExtensionVersion:
    """One `SHOW VERSIONS IN CORTEX EXTENSION` row, addressed by its system name."""

    name: str
    alias: str | None
    location: str
    is_default: bool = False
    certification_status: str | None = None


@dataclass(frozen=True, slots=True)
class AgentVersion:
    """One `SHOW VERSIONS IN AGENT` row.

    Attributes:
        name: The committed version's system name, such as `VERSION$3`; None for the live version.
        alias: The version's alias; None when it has none.
        is_default: The version an unversioned call runs.
        specification: The version's specification as Snowflake stores it; None when the role
            may not read it, as for a secure agent the owner role is not using.
    """

    name: str | None
    alias: str | None
    is_default: bool = False
    comment: str | None = None
    specification: str | None = None


@dataclass(frozen=True, slots=True)
class AgentVersions:
    """An agent's versions, the live one included, as `SHOW VERSIONS IN AGENT` lists them.

    Attributes:
        versions: Every row SHOW prints, in its order.
    """

    versions: tuple[AgentVersion, ...] = ()

    @property
    def has_live_version(self) -> bool:
        """A live version exists, beside which ADD VERSION fails."""
        return any(version.name is None for version in self.versions)

    @property
    def aliases(self) -> tuple[str, ...]:
        """Every version alias the agent shows, live version included, as SHOW prints them."""
        return tuple(version.alias for version in self.versions if version.alias)


class CatalogPort(Protocol):
    """Read what exists in Snowflake and how it is defined, and who the session is.

    No method writes. Absence is a value where the method says so; a read Snowflake refuses
    raises `SnowflakePortError`.
    """

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        """Return every object of one type in a schema, with its names as SHOW prints them.

        Built-in routines, which SHOW FUNCTIONS and SHOW PROCEDURES also list, are left out.
        Never writes.

        Returns:
            One row per object; empty when the schema holds none.

        Raises:
            SnowflakePortError: the SHOW failed, or SST does not observe that object type.
        """
        ...

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        """Return every grant on one object, as SHOW GRANTS lists them.

        `routine_signature` holds a procedure's or function's argument types, which address
        the overload; any other object ignores it. Never writes.

        Raises:
            SnowflakePortError: SHOW GRANTS failed, including for an object that does not exist,
                or the object is a dataset, whose grants Snowflake does not list this way.
        """
        ...

    def tag_references(self, object_type: str, qualified_name: QualifiedName) -> tuple[str, ...]:
        """Return the qualified names of the tags set on one object, sorted, as TAG_REFERENCES lists them.

        A tag the object inherits from its schema or database is left out: it is set on the
        parent, so replacing the object keeps it. Only a semantic view is read. Never writes.

        Raises:
            SnowflakePortError: the read failed, or `object_type` is not one this reads, in which
                case nothing was sent.
        """
        ...

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str,
    ) -> OwnershipMarker | None:
        """Return the SST ownership marker in the comment of one object of `object_type`.

        Never writes.

        Returns:
            The marker; None when no such object exists or its comment carries no marker.

        Raises:
            SnowflakePortError: the SHOW that finds the object failed.
        """
        ...

    def get_ddl(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> str:
        """Return one object's current definition, as GET_DDL prints it.

        `object_type` is the type SHOW lists, which GET_DDL may spell otherwise: an AGENT is
        read as `CORTEX_AGENT`. A procedure or function is addressed by `routine_signature`,
        its argument types. Needs REFERENCES or OWNERSHIP on the object. Never writes.

        Raises:
            SnowflakePortError: GET_DDL failed, including for an object that does not exist or
                a role that may not read its definition; or GET_DDL reads no object of the
                type, such as a stage, a dataset, or a Cortex extension, and nothing was sent.
        """
        ...

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        """Report whether an object of one type exists under a qualified name.

        `object_type` is a type SHOW lists, `DATASET`, `TABLE` for a base table only, or
        `TABLE OR VIEW` for either. Never writes.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        """Report whether a dataset exists under a qualified name, each part compared casefolded.

        Never writes.

        Raises:
            SnowflakePortError: SHOW DATASETS failed.
        """
        ...

    def dataset_versions(self, qualified_name: QualifiedName) -> tuple[str, ...]:
        """Return the names of a dataset's versions, as SHOW VERSIONS lists them, in its order.

        The list holds every version, including ones SST did not add, such as the system
        version Cortex agent evaluation adds to a dataset it runs against. Never writes.

        Raises:
            SnowflakePortError: SHOW VERSIONS failed, as it does for a dataset that does not exist.
        """
        ...

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        """Return a base table's columns as `(name, type)` pairs, both uppercase, in table order.

        Never writes.

        Returns:
            The columns; None when no base table exists under the name.

        Raises:
            SnowflakePortError: the lookup or DESCRIBE TABLE failed.
        """
        ...

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        """Return the file format a stage declares, as text a caller compares once normalized.

        The text is the named file format the stage references, else its inline format
        options. Never writes.

        Returns:
            The file format; None when the stage declares none.

        Raises:
            SnowflakePortError: DESCRIBE STAGE failed, including for a stage that does not exist.
        """
        ...

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        """Return a stage's type as SHOW STAGES reports it, such as `INTERNAL NO CSE`.

        Never writes.

        Returns:
            The type; None when no stage exists under the name, and empty when SHOW reports none.

        Raises:
            SnowflakePortError: SHOW STAGES failed.
        """
        ...

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        """Return what SHOW CORTEX EXTENSIONS reports for one Cortex extension.

        Never writes.

        Returns:
            The extension's row; None when no extension exists under the name.

        Raises:
            SnowflakePortError: the SHOW failed.
        """
        ...

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        """Return a Cortex extension's versions as SHOW VERSIONS lists them.

        Never writes.

        Raises:
            SnowflakePortError: the listing failed, including for an extension that does not exist.
        """
        ...

    def observe_agent_versions(self, qualified_name: QualifiedName) -> AgentVersions:
        """Return an agent's versions, the live one included, as SHOW VERSIONS IN AGENT lists them.

        One read serves both an update, which needs the live version and the aliases, and
        `plan --capture-prior`, which needs the version the agent serves and its specification.

        Never writes.

        Raises:
            SnowflakePortError: SHOW VERSIONS IN AGENT failed, including for an agent that does not exist.
        """
        ...

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        """Resolve a version selector to the committed agent version it names, such as `VERSION$3`.

        A `VERSION$<n>` selector resolves to itself, uppercased, without a read; `committed`
        names the version aliased LAST, and `alias:<name>` the version with that alias.
        Never writes.

        Raises:
            AgentVersionNotFound: the selector names no committed version.
            SnowflakePortError: DESCRIBE AGENT failed, or returned unusable aliases.
        """
        ...

    def current_role(self) -> str:
        """Return the session's current role.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...

    def show_row(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        """Return the row SHOW lists for one object, its columns lowercased and its values as text.

        `object_type` is a type `object_exists` takes. A null value reads as `""`. Never writes.

        Returns:
            The row; None when no object of that type exists under the name.

        Raises:
            SnowflakePortError: the SHOW failed.
        """
        ...

    def describe_properties(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        """Return what DESCRIBE reports for one object, as lowercase property names and text values.

        A DESCRIBE that lists properties as `name`/`value` (or `property`/`property_value`)
        rows gives one entry per row; one that answers with a single row gives its columns.
        Never writes.

        Returns:
            The properties; None when no object of that type exists under the name.

        Raises:
            SnowflakePortError: the DESCRIBE failed.
        """
        ...

    def object_parameter(self, object_type: str, name: str, parameter: str) -> str | None:
        """Return one parameter's value on an account-level object, such as a warehouse's timeout.

        Never writes.

        Returns:
            The value as SHOW PARAMETERS prints it; None when the object reports no such parameter.

        Raises:
            SnowflakePortError: SHOW PARAMETERS failed, including for an object that does not exist.
        """
        ...

    def current_account_locator(self) -> str:
        """Return the locator of the account the session is connected to.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...
