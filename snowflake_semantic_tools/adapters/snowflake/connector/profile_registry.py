"""CoCo Desktop's profile registry, which SST creates, reads, and writes one guarded row at a time.

SST writes only the columns Desktop reads. Each write is guarded by the VERSION the caller
observed and returns the rows it changed, so a count of 0 reveals a concurrent writer.
"""

from __future__ import annotations

from typing import Mapping

from ....domain.model.identifier import QualifiedName
from ....domain.model.lifecycle import QueryResult
from ....domain.ports.snowflake import ProfileRegistryPort, SnowflakePortError
from .session import Session, _json_text, _require_ok

# The 18 columns of CoCo Desktop's profile registry, as the production table
# declares them. Desktop reads 12; the rest belong to other writers and are
# never written by SST on update.
PROFILE_REGISTRY_COLUMNS = (
    "CONFIG_NAME VARCHAR NOT NULL PRIMARY KEY, DESCRIPTION VARCHAR, OWNER_TEAM VARCHAR, "
    "SKILL_REPOS VARIANT, MCP_SERVERS VARIANT, COMMAND_REPOS VARIANT, ENV_VARS VARIANT, "
    "SETTINGS_OVERRIDES VARIANT, VERSION VARCHAR DEFAULT '1.0', ACTIVE BOOLEAN DEFAULT TRUE, "
    "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), "
    "SYSTEM_PROMPT_REPO VARIANT, HOOKS VARIANT, SCRIPTS VARIANT, PLUGINS VARIANT, ALLOWED_ROLES VARIANT, "
    "PERMISSIONS VARIANT"
)
PROFILE_VARIANT_COLUMNS = (
    "SKILL_REPOS",
    "SYSTEM_PROMPT_REPO",
    "MCP_SERVERS",
    "HOOKS",
    "PLUGINS",
    "COMMAND_REPOS",
    "ENV_VARS",
    "SETTINGS_OVERRIDES",
)


class ProfileRegistryMethods(Session, ProfileRegistryPort):
    """Keep CoCo Desktop's profile registry, one row per CONFIG_NAME."""

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        result = self.execute_script((f"CREATE TABLE IF NOT EXISTS {qualified_name.sql} ({PROFILE_REGISTRY_COLUMNS})",))
        _require_ok(result, "profile registry creation failed")

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        rows = _named_rows(self.query(f"SELECT * FROM {registry.sql} WHERE CONFIG_NAME = %s", (name,)))
        if len(rows) > 1:
            raise SnowflakePortError(f"profile registry {registry.sql} holds {len(rows)} rows named {name!r}")
        return rows[0] if rows else None

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        """Write the row in one MERGE on CONFIG_NAME, guarded by the VERSION the caller observed."""
        params: dict[str, object] = {
            "name": row["CONFIG_NAME"],
            "description": row.get("DESCRIPTION"),
            "owner_team": row.get("OWNER_TEAM"),
            "version": row["VERSION"],
            "expected": expected_version,
            **{column.lower(): _variant_parameter(row.get(column)) for column in PROFILE_VARIANT_COLUMNS},
        }
        updates = ", ".join(f"{column} = PARSE_JSON(%({column.lower()})s)" for column in PROFILE_VARIANT_COLUMNS)
        inserted = ", ".join(PROFILE_VARIANT_COLUMNS)
        values = ", ".join(f"PARSE_JSON(%({column.lower()})s)" for column in PROFILE_VARIANT_COLUMNS)
        result = self.query(
            f"MERGE INTO {registry.sql} AS t USING (SELECT %(name)s AS CONFIG_NAME) AS s "
            "ON t.CONFIG_NAME = s.CONFIG_NAME "
            "WHEN MATCHED AND t.VERSION = %(expected)s THEN UPDATE SET "
            f"DESCRIPTION = %(description)s, OWNER_TEAM = %(owner_team)s, VERSION = %(version)s, {updates}, "
            "ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT (CONFIG_NAME, DESCRIPTION, OWNER_TEAM, VERSION, {inserted}, ACTIVE) "
            f"VALUES (%(name)s, %(description)s, %(owner_team)s, %(version)s, {values}, TRUE)",
            params,
        )
        return _affected(result)

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        result = self.query(
            f"UPDATE {registry.sql} SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
            "WHERE CONFIG_NAME = %s AND VERSION = %s AND ACTIVE = TRUE",
            (name, expected_version),
        )
        return _affected(result)

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        """Run the exact query CoCo Desktop runs against its registry."""
        return _named_rows(self.query(f"SELECT * FROM {registry.sql} WHERE active = TRUE ORDER BY config_name"))


def _named_rows(result: QueryResult) -> tuple[dict[str, object], ...]:
    """Return each row of a result as a mapping from its column names, uppercased, to its values."""
    return tuple(dict(zip((column.upper() for column in result.columns), row)) for row in result.rows)


def _variant_parameter(value: object) -> str | None:
    """JSON text for PARSE_JSON; None binds SQL NULL rather than a JSON null."""
    return None if value is None else _json_text(value)


def _affected(result: QueryResult) -> int:
    """Sum the row counts a DML statement reports, one integer per column; booleans are not counts."""
    return sum(
        int(value) for row in result.rows for value in row if isinstance(value, int) and not isinstance(value, bool)
    )
