"""The read-only port plan's preflight checks the target through before it decides."""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope

# How many roles deep `missing_role_privileges` follows the role hierarchy before it stops.
ROLE_HIERARCHY_DEPTH = 16


class PreflightPort(Protocol):
    """Read whether the target can take the planned writes: its containers, privileges, and locks.

    No method writes. A read Snowflake refuses raises `SnowflakePortError`.
    """

    def database_exists(self, database: Identifier) -> bool:
        """Report whether a database exists and the session's role can see it.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def schema_exists(self, scope: SchemaScope) -> bool:
        """Report whether a schema exists and the session's role can see it.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def relation_exists(self, qualified_name: QualifiedName) -> bool:
        """Report whether a table or view exists under a qualified name.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        """Report whether an object of one type exists under a qualified name, as `CatalogPort` does.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def current_role(self) -> str:
        """Return the session's current role, as `CatalogPort` does.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...

    def warehouse_exists(self, warehouse: Identifier) -> bool:
        """Report whether a warehouse exists and the session's role can see it.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return, in the given order, the schema privileges no role in the session holds.

        A role holds a privilege on the schema when it is granted it, or owns the schema.

        Raises:
            SnowflakePortError: the schema's grants could not be read.
        """
        ...

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return, in the given order, the schema privileges one role lacks, inheritance included.

        Unlike `missing_privileges`, the session's secondary roles do not count: this is what a
        task running as `role` holds. The role holds a privilege when it, or a role or database
        role granted to it, is granted the privilege or owns the schema. The hierarchy is read
        with SHOW GRANTS TO ROLE and TO DATABASE ROLE, each role once however often it is
        granted, and no deeper than `ROLE_HIERARCHY_DEPTH`; a privilege only a deeper role
        holds reads as missing.

        Args:
            role: The account role, as CURRENT_ROLE() names it.

        Raises:
            SnowflakePortError: a role's grants could not be read.
        """
        ...

    def locked_objects(self, scope: SchemaScope) -> tuple[QualifiedName, ...]:
        """Return the objects in a schema another session holds a lock on.

        Raises:
            SnowflakePortError: the locks could not be read.
        """
        ...

    def external_references(self, qualified_name: QualifiedName) -> tuple[QualifiedName, ...]:
        """Return the objects that name an object, as Snowflake's dependency record lists them.

        Raises:
            SnowflakePortError: the dependency record could not be read.
        """
        ...
