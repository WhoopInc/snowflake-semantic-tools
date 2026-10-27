"""What plan's preflight reads: the target's containers, the role's privileges, locks, and references.

Nothing here writes. A name SHOW ... LIKE lists is compared again, folded, because LIKE
ignores case and reads `_` and `%` as wildcards.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.json_files import JsonFileError
from snowflake_semantic_tools.adapters.snowflake.connector.catalog import CatalogMethods
from snowflake_semantic_tools.adapters.snowflake.connector.grant_walk import (
    Grantee,
    account_holders,
    account_object_holders,
    grantees_of,
    held_by,
    schema_holders,
)
from snowflake_semantic_tools.adapters.snowflake.connector.lock_table import kind_in, kind_sql
from snowflake_semantic_tools.adapters.snowflake.connector.session import _variant_value
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.preflight import ROLE_HIERARCHY_DEPTH, PreflightPort, RoleWalk
from snowflake_semantic_tools.domain.sql import ident, literal, qname, sql
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

    def table_kind(self, table: QualifiedName) -> str | None:
        try:
            rows = self.query(kind_sql(table), (table.schema.folded, table.name.folded)).rows
        except SnowflakePortError:
            return None
        return kind_in(rows)

    def warehouse_exists(self, warehouse: Identifier) -> bool:
        rows = self._dict_rows(sql("SHOW WAREHOUSES LIKE {pattern}", pattern=literal(warehouse.folded)))
        return any(str(row.get("name") or "") == warehouse.folded for row in rows)

    def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        grants = self._dict_rows(sql("SHOW GRANTS ON SCHEMA {scope}", scope=scope_sql(scope)))
        return self._lacking(schema_holders(grants, scope, privileges), privileges)

    def missing_stage_privileges(self, stage: QualifiedName, privileges: tuple[str, ...]) -> tuple[str, ...]:
        grants = self._dict_rows(sql("SHOW GRANTS ON STAGE {stage}", stage=qname(stage)))
        return self._lacking(schema_holders(grants, SchemaScope.from_qualified_name(stage), privileges), privileges)

    # Snowflake private preview (PrPr): CoWork profiles
    def missing_cowork_privileges(self, cowork_object: str, privileges: tuple[str, ...]) -> tuple[str, ...]:
        name = ident(Identifier.shown(cowork_object))
        grants = self._dict_rows(sql("SHOW GRANTS ON SNOWFLAKE INTELLIGENCE {name}", name=name))
        return self._lacking(account_object_holders(grants, privileges), privileges)

    def _lacking(self, holders: dict[Grantee, frozenset[str]], privileges: tuple[str, ...]) -> tuple[str, ...]:
        """Return the privileges no role in the session holds, given what each grantee of `holders` holds.

        A role holds a privilege when a grant to it names the privilege or OWNERSHIP, or when
        it is granted a database role that does, directly or through other database roles.
        The account roles the grants name are asked about first; database roles are read only
        for the privileges none of those holds in the session, so a database role that cannot
        be read matters only when a direct holder does not already answer.
        """
        in_session: dict[str, bool] = {}
        direct = {grantee: held for grantee, held in holders.items() if grantee.is_account_role}
        missing = self._unheld(direct, privileges, in_session)
        wanted = {privilege.upper() for privilege in missing}
        through = {
            grantee: held & wanted for grantee, held in holders.items() if not grantee.is_account_role and held & wanted
        }
        if not through:
            return missing
        return self._unheld(account_holders(through, self._grantees_of, ROLE_HIERARCHY_DEPTH), missing, in_session)

    def _unheld(
        self, holders: dict[Grantee, frozenset[str]], privileges: tuple[str, ...], in_session: dict[str, bool]
    ) -> tuple[str, ...]:
        """Return the privileges no account role among `holders` that is in the session holds."""
        missing: list[str] = []
        for privilege in privileges:
            roles = sorted(role.names[0].value for role, held in holders.items() if privilege.upper() in held)
            if not any(self._role_in_session(role, in_session) for role in roles):
                missing.append(privilege)
        return tuple(missing)

    def has_secondary_roles(self) -> bool:
        # From the docs, not measured: a JSON object whose `roles` lists the active secondary
        # roles and whose `value` is `ALL` when every granted role is; both empty for none.
        rows = self.query(sql("SELECT CURRENT_SECONDARY_ROLES()")).rows
        try:
            value = _variant_value(rows[0][0] if rows else None, None)
        except JsonFileError:
            return True
        if not isinstance(value, dict):
            return True
        return bool(str(value.get("roles") or "").strip() or str(value.get("value") or "").strip())

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        return self.walk_role_privileges(role, scope, privileges).missing

    def walk_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> RoleWalk:
        """Return the privileges `role` does not hold on the schema, through its hierarchy.

        As `_lacking` does, the account roles the grants name are walked first; database roles
        are read only for the privileges none of those passes up to `role`.
        """
        holders = schema_holders(
            self._dict_rows(sql("SHOW GRANTS ON SCHEMA {scope}", scope=scope_sql(scope))), scope, privileges
        )
        grantee = Grantee.role(_shown(role))
        direct = {holder: held for holder, held in holders.items() if holder.is_account_role}
        held, cut_short = held_by(grantee, direct, self._grantees_of, ROLE_HIERARCHY_DEPTH)
        wanted = {privilege.upper() for privilege in privileges} - held
        through = {
            holder: found & wanted for holder, found in holders.items() if not holder.is_account_role and found & wanted
        }
        if through:
            more, deeper = held_by(grantee, through, self._grantees_of, ROLE_HIERARCHY_DEPTH)
            held, cut_short = held | more, cut_short | deeper
        missing = tuple(privilege for privilege in privileges if privilege.upper() not in held)
        return RoleWalk(missing, tuple(privilege for privilege in missing if privilege.upper() in cut_short))

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
