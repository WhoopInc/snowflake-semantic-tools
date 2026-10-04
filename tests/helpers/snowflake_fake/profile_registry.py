"""The fake's `ProfileRegistryPort`: CoCo Desktop's profile registry table, row by row.

Rows are kept by registry and CONFIG_NAME. VARIANT columns read back as JSON text, as the
driver returns them. A `refused` fragment in the statement the connector would run (CREATE
TABLE for the registry, MERGE for a row) refuses it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.world import SnowflakeWorld

PROFILE_REGISTRY_SHAPE: tuple[tuple[str, str], ...] = (
    ("CONFIG_NAME", "VARCHAR(16777216)"),
    ("DESCRIPTION", "VARCHAR(16777216)"),
    ("OWNER_TEAM", "VARCHAR(16777216)"),
    ("SKILL_REPOS", "VARIANT"),
    ("MCP_SERVERS", "VARIANT"),
    ("COMMAND_REPOS", "VARIANT"),
    ("ENV_VARS", "VARIANT"),
    ("SETTINGS_OVERRIDES", "VARIANT"),
    ("VERSION", "VARCHAR(16777216)"),
    ("ACTIVE", "BOOLEAN"),
    ("CREATED_AT", "TIMESTAMP_NTZ(9)"),
    ("UPDATED_AT", "TIMESTAMP_NTZ(9)"),
    ("SYSTEM_PROMPT_REPO", "VARIANT"),
    ("HOOKS", "VARIANT"),
    ("SCRIPTS", "VARIANT"),
    ("PLUGINS", "VARIANT"),
    ("ALLOWED_ROLES", "VARIANT"),
    ("PERMISSIONS", "VARIANT"),
)
_VARIANT_COLUMNS = frozenset(name for name, kind in PROFILE_REGISTRY_SHAPE if kind == "VARIANT")


class FakeProfileRegistry(SnowflakeWorld):
    """`ProfileRegistryPort` over the shared account."""

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        self._check("ensure_profile_registry")
        if self._refusal(f"CREATE TABLE IF NOT EXISTS {qualified_name.sql}") is not None:
            raise SnowflakePortError("recorded refusal: CREATE TABLE")
        self.tables.setdefault(qualified_name.sql, PROFILE_REGISTRY_SHAPE)
        self.profile_rows.setdefault(qualified_name.sql, {})

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        self._check("read_profile_row")
        row = self.profile_rows.get(registry.sql, {}).get(name)
        return dict(row) if row is not None else None

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        self._check("merge_profile_row")
        if self._refusal(f"MERGE INTO {registry.sql}") is not None:
            raise SnowflakePortError("recorded refusal: MERGE")
        rows = self.profile_rows.setdefault(registry.sql, {})
        name = str(row["CONFIG_NAME"])
        current = rows.get(name)
        if current is not None and (expected_version is None or current.get("VERSION") != expected_version):
            return 0
        rows[name] = {
            **(current or {}),
            **{key: _variant_text(key, value) for key, value in row.items()},
            "ACTIVE": True,
        }
        return 1

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        self._check("deactivate_profile_row")
        row = self.profile_rows.get(registry.sql, {}).get(name)
        if row is None or row.get("VERSION") != expected_version or row.get("ACTIVE") is not True:
            return 0
        row["ACTIVE"] = False
        return 1

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        self._check("desktop_profile_rows")
        rows = self.profile_rows.get(registry.sql, {})
        return tuple(dict(rows[name]) for name in sorted(rows) if rows[name].get("ACTIVE") is True)


def _variant_text(column: str, value: object) -> object:
    """The driver returns VARIANT columns as JSON text, so the fake does too."""
    if column in _VARIANT_COLUMNS and value is not None:
        return json.dumps(value, sort_keys=True)
    return value
