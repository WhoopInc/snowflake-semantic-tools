"""Doubles for `sst import`'s tests: a Snowflake that lists whole scopes, and the setup an import runs with.

`FakeImportPort` is the shared `FakeSnowflake`, which already answers SHOW, DESCRIBE, agent
versions, extension versions and stage reads, plus the two reads only an import makes: a
database-wide listing and an eval input table's rows. `staged_agent` puts one agent there as
`SHOW AGENTS` and `SHOW VERSIONS IN AGENT` would list it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.app.importing import ImportObjects, ImportReport, ImportRequest, ImportScope, ImportSetup
from snowflake_semantic_tools.domain.importing.references import Placed, ProjectIndex
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from tests.helpers.snowflake_fake import FakeSnowflake

TARGET = TargetIdentity("dev", "ACCT", Identifier.parse("DB"), Identifier.parse("S"), "ANALYST", "WH")
ANALYST_SPEC: dict[str, Any] = {
    "models": {"orchestration": "claude-sonnet-4-6"},
    "instructions": {"response": "Lead with the answer.", "sample_questions": [{"question": "Revenue?"}]},
    "tools": [{"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "SALES", "description": "Orders."}}],
    "tool_resources": {
        "SALES": {"semantic_view": "DB.SV.SALES", "execution_environment": {"type": "warehouse", "warehouse": "WH"}}
    },
}


class FakeImportPort(FakeSnowflake):
    """`FakeSnowflake`, listing a database's every schema, and answering eval input tables by name."""

    def __init__(self) -> None:
        super().__init__()
        self.eval_tables: dict[str, tuple[tuple[str, Any], ...]] = {}

    def show_objects_in(self, object_type: str, database: Identifier, schema: Identifier | None) -> tuple[ShowRow, ...]:
        self._check("show_objects_in")
        if schema is not None:
            return self.show_objects(object_type, SchemaScope(database, schema))
        kind = " ".join(object_type.upper().split())
        return tuple(
            row
            for (listed, scope), rows in sorted(self.objects.items())
            if listed == kind and scope.split(".")[0].upper() == database.folded
            for row in rows
        )

    def eval_rows(self, table: QualifiedName) -> tuple[tuple[str, Any], ...] | None:
        self._check("eval_rows")
        return self.eval_tables.get(table.sql)


def staged_agent(
    port: FakeImportPort,
    name: str,
    spec: Mapping[str, Any] | None = None,
    *,
    comment: str = "Answers questions about sales.",
    profile: Mapping[str, str] | None = None,
    alias: str | None = None,
) -> QualifiedName:
    """Put an agent on the fake, listed in its schema, serving one committed version of `spec`."""
    qualified = QualifiedName.parse(name)
    database, schema, object_name = qualified.folded
    port.show(ShowRow(object_name, database, schema, "OWNER", "now", comment, object_type="AGENT"))
    port.show_rows[f"AGENT {qualified.sql}"] = {
        "name": object_name,
        "comment": comment,
        "profile": json.dumps(profile or {"display_name": "Sales"}),
        "is_secure": "false",
    }
    text = json.dumps(spec if spec is not None else ANALYST_SPEC)
    port.agent_version_rows[qualified.sql] = (AgentVersion("VERSION$1", alias, True, None, text),)
    return qualified


def index(*placed: Placed, **fields: Any) -> ProjectIndex:
    """A project index on the `dev` target, holding `placed` and any other field given."""
    return ProjectIndex("dev", placed, **fields)


def setup(project: ProjectIndex | None = None, config: Mapping[str, Any] | None = None, *existing: str) -> ImportSetup:
    """The setup an import runs with: the index, a configuration, and the files that already exist."""
    present = frozenset(existing)
    return ImportSetup(
        project if project is not None else index(),
        MappingProxyType(dict(config or {})),
        TARGET,
        "ANALYST",
        lambda path: path in present,
    )


def run(
    port: FakeImportPort, artifact_type: str, scope: str, project: ImportSetup | None = None, **options: Any
) -> ImportReport:
    """Import `scope` as `artifact_type` against the fake."""
    request = ImportRequest(artifact_type, ImportScope.parse(scope), **options)
    return ImportObjects(port, project if project is not None else setup()).run(request)


def codes(report: ImportReport) -> list[str]:
    """Every code the run reported, the scope's first, then each object's, in order."""
    return [item.code for item in report.diagnostics] + [
        item.code for obj in report.objects for item in obj.diagnostics
    ]


def dbt_model(name: str, relation: str, *columns: Any, **fields: Any) -> Any:
    """A dbt model of `relation`, with `columns` (`DbtColumn`s) and any other field given."""
    from snowflake_semantic_tools.domain.model.dbt import DbtModel

    return DbtModel(f"model.p.{name}", name, relation, (), (), tuple(columns), **fields)


def dbt_column(name: str, **fields: Any) -> Any:
    """A documented dbt column, typed as `fields` say."""
    from snowflake_semantic_tools.domain.model.dbt import DbtColumn

    return DbtColumn(name, fields.pop("description", None), None, fields.pop("column_type", None), **fields)


def view_document(*dimensions: Mapping[str, Any], description: str = "Orders.") -> dict[str, Any]:
    """A one-table view's YAML, as SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW returns it, over DB.MARTS.ORDERS."""
    return {
        "name": "ORDERS_VIEW",
        "tables": [
            {
                "name": "ORDERS",
                "description": description,
                "base_table": {"database": "DB", "schema": "MARTS", "table": "ORDERS"},
                "dimensions": list(dimensions),
            }
        ],
    }
