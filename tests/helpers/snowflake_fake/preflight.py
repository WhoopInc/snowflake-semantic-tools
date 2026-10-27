"""The fake's `PreflightPort`: the reads plan makes before it trusts a target, from `preflight`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.preflight import RoleWalk
from tests.helpers.snowflake_fake.world import SnowflakeWorld


class FakePreflight(SnowflakeWorld):
    """The preflight reads, answered from `self.preflight` (`PreflightAnswers`)."""

    def database_exists(self, database: Identifier) -> bool:
        self._check("database_exists")
        return database.folded not in self.preflight.missing_databases

    def schema_exists(self, scope: SchemaScope) -> bool:
        self._check("schema_exists")
        return scope.sql not in self.preflight.missing_schemas

    def relation_exists(self, qualified_name: QualifiedName) -> bool:
        self._check("relation_exists")
        return qualified_name.sql not in self.preflight.missing_relations

    def table_kind(self, table: QualifiedName) -> str | None:
        self._check("table_kind")
        return self.preflight.table_kinds.get(table.sql)

    def warehouse_exists(self, warehouse: Identifier) -> bool:
        self._check("warehouse_exists")
        return warehouse.folded not in self.preflight.missing_warehouses

    def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._check("missing_privileges")
        lacking = self.preflight.lacking.get(scope.sql, ())
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def missing_stage_privileges(self, stage: QualifiedName, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._check("missing_stage_privileges")
        lacking = self.preflight.stage_lacking.get(stage.sql, ())
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def missing_cowork_privileges(self, cowork_object: str, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._check("missing_cowork_privileges")
        lacking = self.preflight.cowork_lacking.get(cowork_object, ())
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def has_secondary_roles(self) -> bool:
        self._check("has_secondary_roles")
        return self.preflight.secondary_roles

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._check("missing_role_privileges")
        lacking = (*self.preflight.role_lacking.get(scope.sql, ()), *self.preflight.lacking.get(scope.sql, ()))
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def walk_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> RoleWalk:
        missing = self.missing_role_privileges(role, scope, privileges)
        beyond = self.preflight.role_beyond_depth.get(scope.sql, ())
        return RoleWalk(missing, tuple(privilege for privilege in missing if privilege in beyond))

    def locked_objects(self, scope: SchemaScope) -> tuple[QualifiedName, ...]:
        self._check("locked_objects")
        return self.preflight.locked.get(scope.sql, ())

    def external_references(self, qualified_name: QualifiedName) -> tuple[QualifiedName, ...]:
        self._check("external_references")
        return self.preflight.references.get(qualified_name.sql, ())
