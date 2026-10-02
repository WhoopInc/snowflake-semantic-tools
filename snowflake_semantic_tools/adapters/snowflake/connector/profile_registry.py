"""CoCo Desktop's profile registry, which SST creates, reads, and writes one guarded row at a time.

SST writes only the columns Desktop reads. Each write is guarded by the VERSION the caller
observed and returns the rows it changed, so a count of 0 reveals a concurrent writer.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _json_text, _require_ok
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.profile_registry import ProfileRegistryPort
from snowflake_semantic_tools.domain.sql import Sql, ident, join, qname, sql

# The 18 columns of CoCo Desktop's profile registry, as the production table
# declares them. Desktop reads 12; the rest belong to other writers and are
# never written by SST on update.
PROFILE_REGISTRY_COLUMNS = sql(
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


# Each VARIANT column's value, bound by the column's lowercased name and parsed as JSON.
_VARIANT_BINDS = {
    "SKILL_REPOS": sql("PARSE_JSON(%(skill_repos)s)"),
    "SYSTEM_PROMPT_REPO": sql("PARSE_JSON(%(system_prompt_repo)s)"),
    "MCP_SERVERS": sql("PARSE_JSON(%(mcp_servers)s)"),
    "HOOKS": sql("PARSE_JSON(%(hooks)s)"),
    "PLUGINS": sql("PARSE_JSON(%(plugins)s)"),
    "COMMAND_REPOS": sql("PARSE_JSON(%(command_repos)s)"),
    "ENV_VARS": sql("PARSE_JSON(%(env_vars)s)"),
    "SETTINGS_OVERRIDES": sql("PARSE_JSON(%(settings_overrides)s)"),
}


class ProfileRegistryMethods(Session, ProfileRegistryPort):
    """Keep CoCo Desktop's profile registry, one row per CONFIG_NAME."""

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        result = self.execute_script(
            (
                sql(
                    "CREATE TABLE IF NOT EXISTS {table} ({columns})",
                    table=qname(qualified_name),
                    columns=PROFILE_REGISTRY_COLUMNS,
                ),
            )
        )
        _require_ok(result, "profile registry creation failed")

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        rows = _named_rows(
            self.query(sql("SELECT * FROM {table} WHERE CONFIG_NAME = %s", table=qname(registry)), (name,))
        )
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
        updates = join(
            ", ",
            (
                sql("{column} = {value}", column=_column(column), value=_variant_bind(column))
                for column in PROFILE_VARIANT_COLUMNS
            ),
        )
        inserted = join(", ", (_column(column) for column in PROFILE_VARIANT_COLUMNS))
        values = join(", ", (_variant_bind(column) for column in PROFILE_VARIANT_COLUMNS))
        result = self.query(
            sql(
                "MERGE INTO {table} AS t USING (SELECT %(name)s AS CONFIG_NAME) AS s "
                "ON t.CONFIG_NAME = s.CONFIG_NAME "
                "WHEN MATCHED AND t.VERSION = %(expected)s THEN UPDATE SET "
                "DESCRIPTION = %(description)s, OWNER_TEAM = %(owner_team)s, VERSION = %(version)s, {updates}, "
                "ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP() "
                "WHEN NOT MATCHED THEN INSERT (CONFIG_NAME, DESCRIPTION, OWNER_TEAM, VERSION, {inserted}, ACTIVE) "
                "VALUES (%(name)s, %(description)s, %(owner_team)s, %(version)s, {values}, TRUE)",
                table=qname(registry),
                updates=updates,
                inserted=inserted,
                values=values,
            ),
            params,
        )
        return _affected(result)

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        result = self.query(
            sql(
                "UPDATE {table} SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() "
                "WHERE CONFIG_NAME = %s AND VERSION = %s AND ACTIVE = TRUE",
                table=qname(registry),
            ),
            (name, expected_version),
        )
        return _affected(result)

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        """Run the exact query CoCo Desktop runs against its registry."""
        return _named_rows(
            self.query(sql("SELECT * FROM {table} WHERE active = TRUE ORDER BY config_name", table=qname(registry)))
        )


def _column(name: str) -> Sql:
    return ident(Identifier(name))


def _variant_bind(column: str) -> Sql:
    """`PARSE_JSON(%(<column>)s)`: a VARIANT column's named parameter, the column's name lowercased."""
    return _VARIANT_BINDS[column]


def _named_rows(result: QueryResult) -> tuple[dict[str, object], ...]:
    """Return each row of a result as a mapping from its column names, uppercased, to its values."""
    return tuple(dict(zip((column.upper() for column in result.columns), row, strict=False)) for row in result.rows)


def _variant_parameter(value: object) -> str | None:
    """JSON text for PARSE_JSON; None binds SQL NULL rather than a JSON null."""
    return None if value is None else _json_text(value)


def _affected(result: QueryResult) -> int:
    """Sum the row counts a DML statement reports, one integer per column; booleans are not counts."""
    return sum(
        int(value) for row in result.rows for value in row if isinstance(value, int) and not isinstance(value, bool)
    )
