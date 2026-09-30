"""Pure renderers for SST-managed Snowflake tool backing objects."""

from __future__ import annotations

import re

from ..model.artifact_key import artifact_key
from ..model.identifier import Identifier, QualifiedName
from ..model.lifecycle import ProbeKind, RenderedArtifact, SmokeProbe
from ..model.registry import GrantPreservation
from ..model.sql import string_literal
from ..model.tool import ToolKind, ToolMember


def render_tool(member: ToolMember, target: QualifiedName, source_relation: QualifiedName | None) -> RenderedArtifact:
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        return _render_search(member, target, source_relation)
    if member.type == ToolKind.PROCEDURE.value:
        return _render_routine(member, target, procedure=True)
    if member.type == ToolKind.FUNCTION.value:
        return _render_routine(member, target, procedure=False)
    if member.type == ToolKind.STAGE.value:
        return _render_stage(member, target)
    raise ValueError(f"managed tool kind {member.type!r} is not publishable")


def _render_search(
    member: ToolMember,
    target: QualifiedName,
    source_relation: QualifiedName | None,
) -> RenderedArtifact:
    if source_relation is None or not member.search_column or not member.warehouse or not member.target_lag:
        raise ValueError(f"search service {member.name!r} is missing required render fields")
    selected = tuple(
        dict.fromkeys(
            (
                *(column.name for column in member.columns),
                *member.attribute_columns,
                member.search_column,
            )
        )
    )
    clauses = [
        f"CREATE OR REPLACE CORTEX SEARCH SERVICE {target.sql}",
        f"  ON {_identifier(member.search_column)}",
    ]
    if member.attribute_columns:
        clauses.append(f"  ATTRIBUTES {', '.join(_identifier(value) for value in member.attribute_columns)}")
    clauses.extend(
        (
            f"  WAREHOUSE = {_identifier(member.warehouse)}",
            f"  TARGET_LAG = {string_literal(member.target_lag)}",
        )
    )
    if member.embedding_model:
        clauses.append(f"  EMBEDDING_MODEL = {string_literal(member.embedding_model)}")
    if member.description:
        clauses.append(f"  COMMENT = {string_literal(_one_line(member.description))}")
    query = f"SELECT {', '.join(_identifier(value) for value in selected)} FROM {source_relation.sql}"
    if member.where:
        query += f" WHERE {member.where}"
    clauses.append(f"  AS {query}")
    ddl = "\n".join(clauses)
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=ddl,
        object_type="CORTEX SEARCH SERVICE",
        grant_preservation=GrantPreservation.REPLAY,
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                f"DESCRIBE CORTEX SEARCH SERVICE {target.sql}",
            ),
        ),
        required_relations=(source_relation,),
    )


def _render_routine(member: ToolMember, target: QualifiedName, *, procedure: bool) -> RenderedArtifact:
    if not member.language or member.body is None or not member.returns or (procedure and not member.warehouse):
        raise ValueError(f"routine {member.name!r} is missing required render fields")
    object_type = "PROCEDURE" if procedure else "FUNCTION"
    signature = ", ".join(f"{_identifier(parameter.name)} {parameter.type}" for parameter in member.signature)
    clauses = [
        f"CREATE OR REPLACE {object_type} {target.sql}({signature})",
        "  COPY GRANTS",
        f"  RETURNS {member.returns}",
        f"  LANGUAGE {member.language.upper()}",
    ]
    if member.language.casefold() != "sql":
        if not member.runtime_version or not member.handler:
            raise ValueError(f"routine {member.name!r} requires runtime_version and handler")
        clauses.append(f"  RUNTIME_VERSION = {string_literal(member.runtime_version)}")
        clauses.append(f"  HANDLER = {string_literal(member.handler)}")
    if member.packages:
        clauses.append(f"  PACKAGES = ({', '.join(string_literal(value) for value in member.packages)})")
    if member.imports:
        clauses.append(f"  IMPORTS = ({', '.join(string_literal(value) for value in member.imports)})")
    if member.external_access_integrations:
        clauses.append(
            "  EXTERNAL_ACCESS_INTEGRATIONS = ("
            + ", ".join(_identifier(value) for value in member.external_access_integrations)
            + ")"
        )
    if member.secrets:
        clauses.append(
            "  SECRETS = ("
            + ", ".join(
                f"{string_literal(key)} = {QualifiedName.parse(value).sql}"
                for key, value in sorted(member.secrets.items())
            )
            + ")"
        )
    if procedure:
        clauses.append(f"  EXECUTE AS {(member.execute_as or 'caller').upper()}")
    clauses.append(f"  AS {_body(member.body)}")
    ddl = "\n".join(clauses)
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=ddl,
        object_type=object_type,
        grant_preservation=GrantPreservation.CLAUSE,
        routine_signature=tuple(parameter.type for parameter in member.signature),
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                f"DESCRIBE {object_type} {target.sql}({', '.join(parameter.type for parameter in member.signature)})",
            ),
        ),
    )


def _render_stage(member: ToolMember, target: QualifiedName) -> RenderedArtifact:
    ddl = f"CREATE STAGE IF NOT EXISTS {target.sql}"
    if member.description:
        ddl += f" COMMENT = {string_literal(_one_line(member.description))}"
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=ddl,
        object_type="STAGE",
        grant_preservation=GrantPreservation.NONE,
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                f"DESCRIBE STAGE {target.sql}",
            ),
        ),
    )


def _identifier(value: str) -> str:
    return str(Identifier.parse(value).sql)


def _one_line(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _body(value: str) -> str:
    if "$$" in value:
        raise ValueError("routine body contains unsupported dollar-quote delimiter")
    return f"$$\n{value.rstrip()}\n$$"
