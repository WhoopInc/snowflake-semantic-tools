"""Answers to plan's preflight reads for the Snowflake doubles: by default everything exists and nothing is locked."""

from __future__ import annotations

from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


@dataclass
class PreflightAnswers:
    """What a double answers the preflight reads with; a test fills the sets it rehearses.

    Names are compared as `QualifiedName.sql` and `SchemaScope.sql` spell them; databases
    and warehouses folded.
    """

    missing_databases: set[str] = field(default_factory=set)
    missing_schemas: set[str] = field(default_factory=set)
    missing_relations: set[str] = field(default_factory=set)
    missing_warehouses: set[str] = field(default_factory=set)
    lacking: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # What the session's primary role lacks by itself and through its hierarchy, by schema.
    role_lacking: dict[str, tuple[str, ...]] = field(default_factory=dict)
    locked: dict[str, tuple[QualifiedName, ...]] = field(default_factory=dict)
    references: dict[str, tuple[QualifiedName, ...]] = field(default_factory=dict)
    refused: set[str] = field(default_factory=set)


class PreflightDouble:
    """The preflight reads, answered from `self.preflight`; mixed into a double that sets it."""

    preflight: PreflightAnswers

    def _refuse(self, read: str) -> None:
        if read in self.preflight.refused:
            raise SnowflakePortError(f"{read} refused")

    def database_exists(self, database: Identifier) -> bool:
        self._refuse("database_exists")
        return database.folded not in self.preflight.missing_databases

    def schema_exists(self, scope: SchemaScope) -> bool:
        self._refuse("schema_exists")
        return scope.sql not in self.preflight.missing_schemas

    def relation_exists(self, qualified_name: QualifiedName) -> bool:
        self._refuse("relation_exists")
        return qualified_name.sql not in self.preflight.missing_relations

    def warehouse_exists(self, warehouse: Identifier) -> bool:
        self._refuse("warehouse_exists")
        return warehouse.folded not in self.preflight.missing_warehouses

    def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._refuse("missing_privileges")
        lacking = self.preflight.lacking.get(scope.sql, ())
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def missing_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
        self._refuse("missing_role_privileges")
        lacking = self.preflight.role_lacking.get(scope.sql, ())
        return tuple(privilege for privilege in privileges if privilege in lacking)

    def locked_objects(self, scope: SchemaScope) -> tuple[QualifiedName, ...]:
        self._refuse("locked_objects")
        return self.preflight.locked.get(scope.sql, ())

    def external_references(self, qualified_name: QualifiedName) -> tuple[QualifiedName, ...]:
        self._refuse("external_references")
        return self.preflight.references.get(qualified_name.sql, ())
