"""The read-only port to what exists in Snowflake, how it is defined, and who the session is."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker, ShowRow


@dataclass(frozen=True, slots=True)
class StageObservation:
    """Whether a stage exists and, when it does, the file format it declares.

    Attributes:
        file_format: The format as `CatalogPort.describe_stage_file_format` returns it; None
            when the stage does not exist or declares none.
    """

    exists: bool
    file_format: str | None = None


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
            SnowflakePortError: SHOW GRANTS failed, including for an object that does not exist.
        """
        ...

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        """Return the SST ownership marker in the comment of one object of `object_type`.

        Never writes.

        Returns:
            The marker; None when no such object exists or its comment carries no marker.

        Raises:
            SnowflakePortError: the SHOW that finds the object failed.
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

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        """Return a base table's columns as `(name, type)` pairs, both uppercase, in table order.

        Never writes.

        Returns:
            The columns; None when no base table exists under the name.

        Raises:
            SnowflakePortError: the lookup or DESCRIBE TABLE failed.
        """
        ...

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        """Report whether a stage exists and, when it does, the file format it declares.

        Never writes.

        Returns:
            `exists=False` when there is no stage; otherwise the stage's file format, as
            `describe_stage_file_format` returns it.

        Raises:
            SnowflakePortError: the lookup failed.
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

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        """Report whether an agent has a live version, which an update must commit first.

        Never writes.

        Raises:
            SnowflakePortError: SHOW VERSIONS IN AGENT failed.
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

    def current_account_locator(self) -> str:
        """Return the locator of the account the session is connected to.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...
