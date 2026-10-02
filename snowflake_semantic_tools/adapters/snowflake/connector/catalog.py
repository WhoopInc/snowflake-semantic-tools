"""What exists in Snowflake and how it is defined, read with SHOW, DESCRIBE, and INFORMATION_SCHEMA.

Nothing here writes. SHOW ... LIKE ignores case and reads `_` and `%` as wildcards, so a
lookup by name compares each listed name again, under the case rule that lookup has always
used: uppercased for ownership markers and object existence, casefolded for datasets,
stages, and extensions. The two differ only for names outside ASCII.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _variant_value
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker, ShowRow, extract_marker
from snowflake_semantic_tools.domain.ports.snowflake.catalog import (
    CatalogPort,
    ExtensionObservation,
    ExtensionVersion,
    StageObservation,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import AgentVersionNotFound, SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, datatype, ident, join, keyword, literal, qname, scope, sql

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
        "TAG",
    )
)
# The account-level object types SHOW PARAMETERS ... IN <type> is asked about.
PARAMETER_OBJECT_TYPES = frozenset(("WAREHOUSE",))


class CatalogMethods(Session, CatalogPort):
    """Observe objects, grants, stages, extensions, agents, and the session's identity."""

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        normalized_type = _object_type(object_type)
        rows = self._dict_rows(
            sql("SHOW {kind} IN SCHEMA {scope}", kind=keyword(normalized_type, plural=True), scope=_scope(scope))
        )
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
        object_name = qname(qualified_name)
        if object_type.upper() in {"PROCEDURE", "FUNCTION"}:
            types = join(", ", (datatype(value) for value in routine_signature))
            object_name = sql("{name}({types})", name=object_name, types=types)
        rows = self._dict_rows(
            sql("SHOW GRANTS ON {kind} {name}", kind=keyword(_object_type(object_type)), name=object_name)
        )
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
        return str(self.query(sql("SELECT CURRENT_ROLE()")).rows[0][0])

    def current_account_locator(self) -> str:
        return str(self.query(sql("SELECT CURRENT_ACCOUNT()")).rows[0][0])

    def show_row(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        rows = self._show_like(object_type, qualified_name, str.upper)
        return _text_row(rows[0]) if rows else None

    def describe_properties(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        if not self.object_exists(object_type, qualified_name):
            return None
        rows = self._dict_rows(
            sql("DESCRIBE {kind} {name}", kind=keyword(_object_type(object_type)), name=qname(qualified_name))
        )
        properties: dict[str, str] = {}
        for row in rows:
            key = row.get("property", row.get("name"))
            if len(rows) > 1 and key is not None and ("value" in row or "property_value" in row):
                properties[str(key).casefold()] = _text(row.get("property_value", row.get("value")))
        if len(rows) == 1:
            properties.update(_text_row(rows[0]))
        return properties

    def object_parameter(self, object_type: str, name: str, parameter: str) -> str | None:
        kind = " ".join(object_type.upper().split())
        if kind not in PARAMETER_OBJECT_TYPES:
            raise SnowflakePortError(f"unsupported parameter object type {object_type!r}")
        rows = self._dict_rows(
            sql(
                "SHOW PARAMETERS LIKE {parameter} IN {kind} {name}",
                parameter=literal(parameter),
                kind=keyword(kind),
                name=ident(Identifier.parse(name)),
            )
        )
        match = next((row for row in rows if str(row.get("key") or "").upper() == parameter.upper()), None)
        return _text(match.get("value")) if match is not None else None

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        normalized_input = " ".join(object_type.upper().split())
        if normalized_input == "DATASET":
            return self.dataset_exists(qualified_name)
        if normalized_input in {"TABLE OR VIEW", "TABLE"}:
            type_filter = sql("") if normalized_input == "TABLE OR VIEW" else sql(" AND TABLE_TYPE = 'BASE TABLE'")
            result = self.query(
                sql(
                    "SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s{type_filter}",
                    database=ident(qualified_name.database),
                    type_filter=type_filter,
                ),
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

    def dataset_versions(self, qualified_name: QualifiedName) -> tuple[str, ...]:
        rows = self._dict_rows(sql("SHOW VERSIONS IN DATASET {dataset}", dataset=qname(qualified_name)))
        return tuple(str(row.get("name") or "") for row in rows if row.get("name"))

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        if not self.object_exists("STAGE", qualified_name):
            return StageObservation(False)
        return StageObservation(True, self.describe_stage_file_format(qualified_name))

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        rows = self._dict_rows(sql("DESCRIBE STAGE {stage}", stage=qname(qualified_name)))
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
        rows = self._dict_rows(sql("SHOW VERSIONS IN CORTEX EXTENSION {extension}", extension=qname(qualified_name)))
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
        rows = self._dict_rows(sql("DESCRIBE TABLE {table}", table=qname(qualified_name)))
        if any("name" not in row or "type" not in row for row in rows):
            raise _unexpected_describe(f"TABLE {qualified_name.sql}")
        return tuple((str(row.get("name") or "").upper(), str(row.get("type") or "").upper()) for row in rows)

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        rows = self._dict_rows(sql("SHOW VERSIONS IN AGENT {agent}", agent=qname(qualified_name)))
        return any(row.get("name") is None for row in rows)

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        rows = self._dict_rows(sql("DESCRIBE AGENT {agent}", agent=qname(qualified_name)))
        # Unlike DESCRIBE STAGE above, a row's `name` and `value` win over `property` and
        # `property_value`, and names compare casefolded.
        properties = {
            str(row.get("name") or row.get("property") or "").casefold(): row.get("value", row.get("property_value"))
            for row in rows
        }
        aliases = _variant_value(properties.get("aliases"), {})
        if not isinstance(aliases, dict):
            raise _unexpected_describe(f"AGENT {qualified_name.sql}")
        key = "LAST" if selector == "committed" else selector.removeprefix("alias:")
        resolved = next((value for alias, value in aliases.items() if str(alias).casefold() == key.casefold()), None)
        if not isinstance(resolved, str) or not resolved.upper().startswith("VERSION$"):
            raise AgentVersionNotFound(
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
        rows = self._dict_rows(
            sql(
                "SHOW {kind} LIKE {pattern} IN SCHEMA {scope}",
                kind=keyword(_object_type(object_type), plural=True),
                pattern=literal(qualified_name.name.folded),
                scope=_scope(SchemaScope.from_qualified_name(qualified_name)),
            )
        )
        expected = fold(qualified_name.name.folded)
        return tuple(row for row in rows if fold(str(row.get("name") or "")) == expected)


def _scope(value: SchemaScope) -> Sql:
    return scope(value)


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


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _text_row(row: Mapping[str, object]) -> dict[str, str]:
    return {str(key).casefold(): _text(value) for key, value in row.items()}


def _optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _unexpected_describe(described: str) -> SnowflakePortError:
    """The error for a DESCRIBE whose rows lack what SST reads, such as `type` from a table.

    Diagnostics:
        SST-SNO025: the DESCRIBE of `described` returned an unexpected shape.
    """
    return SnowflakePortError(
        f"the DESCRIBE of {described} returned an unexpected shape", diagnostic=D("SST-SNO025", value=described)
    )
