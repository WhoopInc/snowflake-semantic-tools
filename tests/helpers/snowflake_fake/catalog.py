"""The fake's `CatalogPort`: what SHOW, DESCRIBE, GET_DDL and the version reads report.

Each read answers from the account `SnowflakeWorld` holds and never writes. A read armed with
`fail` raises before it answers, as the connector raises `SnowflakePortError` when Snowflake
refuses the statement behind it.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation, ExtensionVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import AgentVersionNotFound, SnowflakePortError
from tests.helpers.snowflake_fake.extensions import versions
from tests.helpers.snowflake_fake.world import SnowflakeWorld


class FakeCatalog(SnowflakeWorld):
    """`CatalogPort` over the shared account."""

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        self._check("show_objects")
        return self.objects.get((object_type.upper(), scope.sql), ())

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        del object_type, routine_signature
        self._check("show_grants")
        return self.grants.get(qualified_name.sql, ())

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        del object_type
        self._check("describe_marker")
        return self.markers.get(qualified_name.sql)

    def get_ddl(self, object_type: str, qualified_name: QualifiedName) -> str:
        self._check("get_ddl")
        if qualified_name.sql not in self.definitions:
            raise SnowflakePortError(f"GET_DDL refused for {object_type} {qualified_name.sql}")
        return self.definitions[qualified_name.sql]

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        self._check("object_exists")
        if object_type.upper() == "DATASET":
            return self.dataset_exists(qualified_name)
        if object_type.upper() == "STAGE" and qualified_name.sql in self.stage_formats:
            return True
        if self.existing is None:
            return object_type == "TABLE OR VIEW"
        return qualified_name.sql in self.existing

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        self._check("dataset_exists")
        return self.existing is not None and qualified_name.sql in self.existing

    def dataset_versions(self, qualified_name: QualifiedName) -> tuple[str, ...]:
        self._check("dataset_versions")
        return tuple(self.dataset_version_names.get(qualified_name.sql, ()))

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        self._check("table_columns")
        return self.tables.get(qualified_name.sql)

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        self._check("describe_stage_file_format")
        return self.stage_formats.get(qualified_name.sql)

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        self._check("stage_type")
        if qualified_name.sql in self.stage_types:
            return self.stage_types[qualified_name.sql]
        return "INTERNAL NO CSE" if self.object_exists("STAGE", qualified_name) else None

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        self._check("observe_extension")
        extension = self.extensions.get(qualified_name.sql)
        if extension is None:
            return None
        listed = versions(extension)
        default = next((version for version in listed if version["is_default"]), None)
        # MEASURED 2026-09-29: once a version is certified, the extension serves the
        # latest certified version instead of the default.
        certified = next((version for version in reversed(listed) if version["certification"] == "CERTIFIED"), None)
        effective = certified or default
        return ExtensionObservation(
            qualified_name=qualified_name,
            extension_type=str(extension["type"]),
            comment=extension["comment"] if isinstance(extension["comment"], str) else None,
            owner=self.role,
            effective_version=str(effective["name"]) if effective is not None else None,
            latest_certified_version=str(certified["name"]) if certified is not None else None,
        )

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        self._check("extension_versions")
        extension = self.extensions.get(qualified_name.sql)
        if extension is None:
            raise SnowflakePortError(f"Cortex extension {qualified_name.sql} does not exist")
        return tuple(
            ExtensionVersion(
                name=str(version["name"]),
                alias=version["alias"] if isinstance(version["alias"], str) else None,
                location=str(version["location"]),
                is_default=bool(version["is_default"]),
                certification_status=(version["certification"] if isinstance(version["certification"], str) else None),
            )
            for version in versions(extension)
        )

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        self._check("agent_has_live_version")
        return qualified_name.sql in self.live_agents

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        self._check("resolve_agent_version")
        if selector.upper().startswith("VERSION$"):
            return selector.upper()
        try:
            return self.agent_versions[(qualified_name.sql, selector.casefold())]
        except KeyError as exc:
            raise AgentVersionNotFound(
                f"agent {qualified_name.sql} selector {selector!r} does not resolve to a committed version"
            ) from exc

    def current_role(self) -> str:
        self._check("current_role")
        return self.role

    def current_account_locator(self) -> str:
        self._check("current_account_locator")
        return self.account_locator

    def show_row(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        self._check("show_row")
        return self.show_rows.get(f"{object_type.upper()} {qualified_name.sql}")

    def describe_properties(self, object_type: str, qualified_name: QualifiedName) -> Mapping[str, str] | None:
        self._check("describe_properties")
        return self.descriptions.get(f"{object_type.upper()} {qualified_name.sql}")

    def object_parameter(self, object_type: str, name: str, parameter: str) -> str | None:
        self._check("object_parameter")
        return self.object_parameters.get((object_type.upper(), name.upper(), parameter.upper()))
