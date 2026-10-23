"""The read-only port plan's preflight checks the target through before it decides."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope

# How many roles deep `missing_role_privileges` follows the role hierarchy before it stops.
ROLE_HIERARCHY_DEPTH = 16


@dataclass(frozen=True, slots=True)
class RoleWalk:
    """What reading one role's schema privileges up its hierarchy found.

    Attributes:
        missing: The privileges the role was not found to hold, in the order asked.
        beyond_depth: Those of `missing` still climbing toward the role when the walk stopped
            at `ROLE_HIERARCHY_DEPTH` roles, which a more distant inheritance may pass on.
    """

    missing: tuple[str, ...] = ()
    beyond_depth: tuple[str, ...] = ()


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

        A role holds a privilege on the schema when it is granted it, or owns the schema, or is
        granted a database role that holds it, directly or through other database roles, no
        further than `ROLE_HIERARCHY_DEPTH` grants. Whether a role is in the session, primary,
        secondary, or inherited, is Snowflake's to answer, so secondary roles count. Snowflake
        authorizes a `CREATE <object>` statement by the primary role alone, so plan asks
        `walk_role_privileges` of the primary role for its `CREATE` privileges, and asks this
        only about what that role lacks: a privilege held here is then held by a secondary role,
        or by a role further up than the walk climbs. With no secondary role active, plan asks
        this alone.

        Raises:
            SnowflakePortError: the schema's grants could not be read.
        """
        ...

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        """Return a stage's TYPE as SHOW STAGES lists it, as `CatalogPort` does; None when it is absent.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def table_kind(self, table: QualifiedName) -> str | None:
        """Return whether a table is a `hybrid` or a `standard` table, as INFORMATION_SCHEMA says.

        Never raises: None when the table does not exist, and when the read failed, since a
        kind that cannot be read is not known to be the wrong one.
        """
        ...

    def missing_stage_privileges(self, stage: QualifiedName, privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return, in the given order, the privileges on a stage no role in the session holds.

        A role holds a privilege on the stage when it is granted it, owns the stage, or holds it
        through a database role, as `missing_privileges` reads a schema's. READ and WRITE are not
        `CREATE` privileges, so a secondary role's count.

        Raises:
            SnowflakePortError: the stage's grants could not be read.
        """
        ...

    # Snowflake private preview (PrPr): CoWork profiles
    def missing_cowork_privileges(self, cowork_object: str, privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return, in the given order, the privileges on the CoWork object no role in the session holds.

        `cowork_object` is named as SHOW SNOWFLAKE INTELLIGENCES lists it. A role holds a privilege
        on it when SHOW GRANTS ON SNOWFLAKE INTELLIGENCE lists the privilege, or OWNERSHIP, granted
        to it, directly or to a role it inherits; a secondary role's count. Only account roles
        hold privileges on an account-level object. Never writes.

        Raises:
            SnowflakePortError: the object's grants could not be read.
        """
        ...

    def has_secondary_roles(self) -> bool:
        """Report whether the session has any secondary role active, as CURRENT_SECONDARY_ROLES() says.

        Without one, every role in the session is the primary role or one it inherits, so
        `missing_privileges` answers what `missing_role_privileges` would of the primary role.
        An answer that cannot be read as none reads as some.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...

    def walk_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> RoleWalk:
        """Return what `missing_role_privileges` returns, and which of it the depth limit cut short.

        Raises:
            SnowflakePortError: the schema's or a role's grants could not be read.
        """
        ...

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return, in the given order, the schema privileges one role lacks, inheritance included.

        Unlike `missing_privileges`, the session's secondary roles do not count: this is what a
        task running as `role` holds, and what a `CREATE` the session's primary role `role` runs
        may use. The role holds a privilege when it, or a role or database
        role granted to it, is granted the privilege or owns the schema. The hierarchy is read
        upward from the schema: SHOW GRANTS ON SCHEMA names the holders, and SHOW GRANTS OF
        ROLE and OF DATABASE ROLE climb from them toward `role`, each role once however often
        it is granted and never `role`'s own grants, no further than `ROLE_HIERARCHY_DEPTH`
        roles; a privilege only a more distant role holds reads as missing. The account roles
        the grants name are walked first, and database roles only for what those leave missing.
        A privilege no role holds on the schema reads as missing without reading any role.

        Args:
            role: The account role, as CURRENT_ROLE() names it.

        Raises:
            SnowflakePortError: the schema's or a role's grants could not be read.
        """
        ...

    def locked_objects(self, scope: SchemaScope) -> tuple[QualifiedName, ...]:
        """Return the objects in a schema another session holds a lock on.

        Every user's locks are read, which Snowflake permits only a role with MONITOR on the
        account; a role without it is refused with a privilege failure.

        Raises:
            SnowflakePortError: the locks could not be read.
        """
        ...

    def external_references(self, qualified_name: QualifiedName) -> tuple[QualifiedName, ...]:
        """Return the objects that name an object, as Snowflake's dependency record lists them.

        The record is `SNOWFLAKE.ACCOUNT_USAGE`, which Snowflake shows only a role granted
        IMPORTED PRIVILEGES on the `SNOWFLAKE` database, or a database role there that covers
        it; a role without it is refused as if the schema did not exist.

        Raises:
            SnowflakePortError: the dependency record could not be read.
        """
        ...
