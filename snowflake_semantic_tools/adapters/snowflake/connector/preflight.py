"""What plan's preflight reads: the target's containers, the role's privileges, locks, and references.

Nothing here writes. A name SHOW ... LIKE lists is compared again, folded, because LIKE
ignores case and reads `_` and `%` as wildcards.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.catalog import CatalogMethods
from snowflake_semantic_tools.adapters.snowflake.connector.grant_walk import (
    OWNERSHIP,
    Grantee,
    grantees_of,
    held_by,
    schema_holders,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.preflight import ROLE_HIERARCHY_DEPTH, PreflightPort
from snowflake_semantic_tools.domain.sql import ident, literal, sql
from snowflake_semantic_tools.domain.sql import scope as scope_sql


class PreflightMethods(CatalogMethods, PreflightPort):
    """Read whether the target can take the planned writes."""

    def database_exists(self, database: Identifier) -> bool:
        rows = self._dict_rows(sql("SHOW DATABASES LIKE {pattern}", pattern=literal(database.folded)))
        return any(str(row.get("name") or "") == database.folded for row in rows)

    def schema_exists(self, scope: SchemaScope) -> bool:
        rows = self._dict_rows(
            sql(
                "SHOW SCHEMAS LIKE {pattern} IN DATABASE {database}",
                pattern=literal(scope.schema.folded),
                database=ident(scope.database),
            )
        )
        return any(str(row.get("name") or "") == scope.schema.folded for row in rows)

    def relation_exists(self, qualified_name: QualifiedName) -> bool:
        return self.object_exists("TABLE OR VIEW", qualified_name)

    def warehouse_exists(self, warehouse: Identifier) -> bool:
        rows = self._dict_rows(sql("SHOW WAREHOUSES LIKE {pattern}", pattern=literal(warehouse.folded)))
        return any(str(row.get("name") or "") == warehouse.folded for row in rows)

    def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        rows = self._dict_rows(sql("SHOW GRANTS ON SCHEMA {scope}", scope=scope_sql(scope)))
        holders: dict[str, set[str]] = {}
        for row in rows:
            if str(row.get("granted_to") or "").upper() == "ROLE":
                holders.setdefault(str(row.get("privilege") or "").upper(), set()).add(str(row.get("grantee_name")))
        in_session: dict[str, bool] = {}
        missing: list[str] = []
        for privilege in privileges:
            roles = sorted(holders.get(privilege.upper(), set()) | holders.get(OWNERSHIP, set()))
            if not any(self._role_in_session(role, in_session) for role in roles):
                missing.append(privilege)
        return tuple(missing)

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        holders = schema_holders(
            self._dict_rows(sql("SHOW GRANTS ON SCHEMA {scope}", scope=scope_sql(scope))), scope, privileges
        )
        held = held_by(Grantee.role(_shown(role)), holders, self._grantees_of, ROLE_HIERARCHY_DEPTH)
        return tuple(privilege for privilege in privileges if privilege.upper() not in held)

    def _grantees_of(self, grantee: Grantee) -> tuple[Grantee, ...]:
        return grantees_of(grantee, self._dict_rows(grantee.grants_of))

    def locked_objects(self, scope: SchemaScope) -> tuple[QualifiedName, ...]:
        rows = self._dict_rows(sql("SHOW LOCKS IN ACCOUNT"))
        locked: dict[tuple[str, str, str], QualifiedName] = {}
        for row in rows:
            if str(row.get("status") or "").upper() != "HOLDING":
                continue
            try:
                name = QualifiedName.parse(str(row.get("resource") or ""))
            except ValueError:
                continue
            if SchemaScope.from_qualified_name(name) == scope:
                locked.setdefault(name.folded, name)
        return tuple(locked.values())

    def external_references(self, qualified_name: QualifiedName) -> tuple[QualifiedName, ...]:
        result = self.query(
            sql(
                "SELECT REFERENCING_DATABASE, REFERENCING_SCHEMA, REFERENCING_OBJECT_NAME "
                "FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES "
                "WHERE REFERENCED_DATABASE = %s AND REFERENCED_SCHEMA = %s AND REFERENCED_OBJECT_NAME = %s"
            ),
            qualified_name.folded,
        )
        names: dict[tuple[str, str, str], QualifiedName] = {}
        for database, schema, name in result.rows:
            referrer = QualifiedName(
                Identifier.shown(str(database)), Identifier.shown(str(schema)), Identifier.shown(str(name))
            )
            names.setdefault(referrer.folded, referrer)
        return tuple(names.values())

    def _role_in_session(self, role: str, cache: dict[str, bool]) -> bool:
        """Report whether a role is active in the session, directly or through the role hierarchy."""
        if role not in cache:
            rows = self.query(sql("SELECT IS_ROLE_IN_SESSION(%s)"), (role,)).rows
            cache[role] = bool(rows and rows[0][0])
        return cache[role]


def _shown(role: str) -> Identifier:
    """A role as CURRENT_ROLE() names it, or double-quoted to keep its case."""
    return Identifier.parse(role) if role.startswith('"') else Identifier.shown(role)
