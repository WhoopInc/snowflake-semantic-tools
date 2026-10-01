"""What exists in Snowflake and how it is defined, read with SHOW, DESCRIBE, and INFORMATION_SCHEMA.

Nothing here writes. SHOW ... LIKE ignores case and reads `_` and `%` as wildcards, so a
lookup by name compares each listed name again, under the case rule that lookup has always
used: uppercased for ownership markers and object existence, casefolded for datasets,
stages, and extensions. The two differ only for names outside ASCII.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _variant_value
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker, ShowRow, extract_marker
from snowflake_semantic_tools.domain.model.sql import string_literal
from snowflake_semantic_tools.domain.ports.snowflake import (
    CatalogPort,
    ExtensionObservation,
    ExtensionVersion,
    SnowflakePortError,
    StageObservation,
)

# The object types SST observes, each spelled as SHOW spells it once pluralized with an S.
OBJECT_TYPES = frozenset(
    (
        "SEMANTIC VIEW",
        "CORTEX SEARCH SERVICE",
        "CORTEX EXTENSION",
        "PROCEDURE",
        "FUNCTION",
        "STAGE",
        "AGENT",
        "DATASET",
        "TABLE",
        "VIEW",
    )
)


class CatalogMethods(Session, CatalogPort):
    """Observe objects, grants, stages, extensions, agents, and the session's identity."""

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        normalized_type = _object_type(object_type)
        sql = f"SHOW {normalized_type}S IN SCHEMA {scope.sql}"
        rows = self._dict_rows(sql)
        return tuple(
            sorted(
                (
                    ShowRow(
                        name=str(row["name"]),
                        # SHOW FUNCTIONS and SHOW PROCEDURES name the database `catalog_name`.
                        database_name=str(row.get("database_name") or row.get("catalog_name") or scope.database.folded),
                        schema_name=str(row.get("schema_name") or scope.schema.folded),
                        owner=str(row.get("owner") or ""),
                        created_on=str(row.get("created_on") or ""),
                        comment=_show_comment(row),
                        object_type=normalized_type,
                    )
                    for row in rows
                    # Both also list every built-in routine in every schema; none is an artifact.
                    if str(row.get("is_builtin") or "").upper() != "Y"
                ),
                key=lambda item: item.qualified_name.folded,
            )
        )

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        object_name = qualified_name.sql
        if object_type.upper() in {"PROCEDURE", "FUNCTION"}:
            object_name += f"({', '.join(routine_signature)})"
        rows = self._dict_rows(f"SHOW GRANTS ON {_object_type(object_type)} {object_name}")
        return tuple(
            sorted(
                GrantRow(
                    privilege=str(row.get("privilege") or ""),
                    granted_to=str(row.get("granted_to") or ""),
                    grantee_name=str(row.get("grantee_name") or ""),
                    granted_by=str(row.get("granted_by") or ""),
                    grant_option=str(row.get("grant_option") or "").lower() in {"true", "yes"},
                )
                for row in rows
            )
        )

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        rows = self._show_like(object_type, qualified_name, str.upper)
        return extract_marker(_show_comment(rows[0])) if rows else None

    def current_role(self) -> str:
        return str(self.query("SELECT CURRENT_ROLE()").rows[0][0])

    def current_account_locator(self) -> str:
        return str(self.query("SELECT CURRENT_ACCOUNT()").rows[0][0])

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        normalized_input = " ".join(object_type.upper().split())
        if normalized_input == "DATASET":
            return self.dataset_exists(qualified_name)
        if normalized_input in {"TABLE OR VIEW", "TABLE"}:
            type_filter = "" if normalized_input == "TABLE OR VIEW" else " AND TABLE_TYPE = 'BASE TABLE'"
            result = self.query(
                f"SELECT COUNT(*) FROM {qualified_name.database.sql}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s{type_filter}",
                (qualified_name.schema.folded, qualified_name.name.folded),
            )
            count = result.rows[0][0] if result.rows else 0
            return isinstance(count, (int, float, str)) and int(count) > 0
        database, schema = qualified_name.database.folded, qualified_name.schema.folded
        # A row that omits its database or schema is taken to be in the schema SHOW searched.
        return any(
            str(row.get("database_name") or database).upper() == database.upper()
            and str(row.get("schema_name") or schema).upper() == schema.upper()
            for row in self._show_like(normalized_input, qualified_name, str.upper)
        )

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        return any(
            _identifier_matches(row.get("database_name"), qualified_name.database.folded)
            and _identifier_matches(row.get("schema_name"), qualified_name.schema.folded)
            for row in self._show_like("DATASET", qualified_name, str.casefold)
        )

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        if not self.object_exists("STAGE", qualified_name):
            return StageObservation(False)
        return StageObservation(True, self.describe_stage_file_format(qualified_name))

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        rows = self._dict_rows(f"DESCRIBE STAGE {qualified_name.sql}")
        properties = {
            str(row.get("property") or row.get("name") or "").upper(): row.get("property_value", row.get("value"))
            for row in rows
        }
        file_format = properties.get("FILE_FORMAT")
        if file_format is not None:
            return str(file_format)
        required = (
            "TYPE",
            "FIELD_DELIMITER",
            "RECORD_DELIMITER",
            "SKIP_HEADER",
            "FIELD_OPTIONALLY_ENCLOSED_BY",
            "ESCAPE_UNENCLOSED_FIELD",
        )
        if not any(key in properties for key in required):
            return None
        return " ".join(f"{key}={properties.get(key)}" for key in required)

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        rows = self._show_like("STAGE", qualified_name, str.casefold)
        return str(rows[0].get("type") or "") if rows else None

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        rows = self._show_like("CORTEX EXTENSION", qualified_name, str.casefold)
        if not rows:
            return None
        match = rows[0]
        return ExtensionObservation(
            qualified_name=qualified_name,
            extension_type=str(match.get("type") or "").upper(),
            comment=str(match["comment"]) if match.get("comment") is not None else None,
            owner=str(match.get("owner") or ""),
            effective_version=_optional_text(match.get("effective_version")),
            latest_certified_version=_optional_text(match.get("latest_certified_version")),
        )

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        rows = self._dict_rows(f"SHOW VERSIONS IN CORTEX EXTENSION {qualified_name.sql}")
        return tuple(
            ExtensionVersion(
                name=str(row.get("name") or "").upper(),
                alias=_optional_text(row.get("alias")),
                location=str(row.get("location_uri") or ""),
                is_default=str(row.get("is_default") or "").casefold() == "true",
                certification_status=_optional_text(row.get("certification_status")),
            )
            for row in rows
            if row.get("name")
        )

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        if not self.object_exists("TABLE", qualified_name):
            return None
        rows = self._dict_rows(f"DESCRIBE TABLE {qualified_name.sql}")
        return tuple((str(row.get("name") or "").upper(), str(row.get("type") or "").upper()) for row in rows)

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        rows = self._dict_rows(f"SHOW VERSIONS IN AGENT {qualified_name.sql}")
        return any(row.get("name") is None for row in rows)

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        rows = self._dict_rows(f"DESCRIBE AGENT {qualified_name.sql}")
        # Unlike DESCRIBE STAGE above, a row's `name` and `value` win over `property` and
        # `property_value`, and names compare casefolded.
        properties = {
            str(row.get("name") or row.get("property") or "").casefold(): row.get("value", row.get("property_value"))
            for row in rows
        }
        aliases = _variant_value(properties.get("aliases"), {})
        if not isinstance(aliases, dict):
            raise SnowflakePortError(f"agent {qualified_name.sql} returned invalid aliases metadata")
        key = "LAST" if selector == "committed" else selector.removeprefix("alias:")
        resolved = next((value for alias, value in aliases.items() if str(alias).casefold() == key.casefold()), None)
        if not isinstance(resolved, str) or not resolved.upper().startswith("VERSION$"):
            raise SnowflakePortError(
                f"agent {qualified_name.sql} selector {selector!r} does not resolve to a committed version"
            )
        return resolved.upper()

    def _show_like(
        self, object_type: str, qualified_name: QualifiedName, fold: Callable[[str], str]
    ) -> tuple[dict[str, Any], ...]:
        """Return the rows SHOW <type>S LIKE lists for a name in its schema, keeping those it names.

        A row is kept when its name equals the looked-up name once `fold` (`str.upper` or
        `str.casefold`, the caller's case rule) is applied to both; rows keep SHOW's order.

        Raises:
            SnowflakePortError: SST does not observe `object_type`, or the SHOW failed.
        """
        pattern = string_literal(qualified_name.name.folded)
        rows = self._dict_rows(
            f"SHOW {_object_type(object_type)}S LIKE {pattern} IN SCHEMA "
            f"{qualified_name.database.sql}.{qualified_name.schema.sql}"
        )
        expected = fold(qualified_name.name.folded)
        return tuple(row for row in rows if fold(str(row.get("name") or "")) == expected)


def _object_type(value: str) -> str:
    normalized = " ".join(value.upper().split())
    if normalized not in OBJECT_TYPES:
        raise SnowflakePortError(f"unsupported Snowflake object type {value!r}")
    return normalized


def _identifier_matches(value: object, expected: str) -> bool:
    return str(value or "").casefold() == expected.casefold()


def _show_comment(row: Mapping[str, object]) -> str | None:
    """An object's COMMENT from a SHOW row; routines report it as `description`."""
    value = row["comment"] if "comment" in row else row.get("description")
    return str(value) if value is not None else None


def _optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
