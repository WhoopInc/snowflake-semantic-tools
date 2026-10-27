"""Pure renderers for SST-managed Snowflake tool backing objects.

Every value a member declares reaches the DDL through a typed constructor: names as
identifiers, types against Snowflake's type grammar, `where:` through the expression guard,
languages and `execute_as` from a closed vocabulary, and the routine body dollar-quoted.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ProbeKind, PublishShape, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.tool import HANDLER_LANGUAGES, ToolKind, ToolMember
from snowflake_semantic_tools.domain.sql import (
    Sql,
    canonical,
    datatype,
    dollar_quoted,
    expr,
    guard_expression,
    ident,
    join,
    keyword,
    literal,
    qname,
    routine_ref,
    sql,
)

REFRESH_MODES = ("AUTO", "FULL", "INCREMENTAL")
_TABLE_RETURNS = re.compile(r"TABLE\s*\((?P<columns>.*)\)", re.IGNORECASE | re.DOTALL)


# The member kinds `render_tool` has a renderer for.
RENDERED_KINDS = frozenset(
    {ToolKind.CORTEX_SEARCH_SERVICE.value, ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value, ToolKind.STAGE.value}
)


def tool_render_checks(member: ToolMember, source_relation: QualifiedName | None) -> tuple[Diagnostic, ...]:
    """Report a member `render_tool` cannot render, once its resolved settings have applied.

    A kind may have no renderer, a member no source statement, or no warehouse or target lag.

    Diagnostics:
        SST-RND040: the member's kind has no renderer.
        SST-RND041: a search service has no relation to select from, or a routine has no body.
        SST-RND042: a search service has no `warehouse` or `target_lag`, or a procedure no
            `warehouse`.
    """
    key = artifact_key("tool", member.name.casefold())
    if member.type not in RENDERED_KINDS:
        return (D("SST-RND040", origin=member.origin, subject=key, artifact=member.name, found=member.type),)
    searching = member.type == ToolKind.CORTEX_SEARCH_SERVICE.value
    routine = member.type in (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value)
    found: list[Diagnostic] = []
    if (searching and source_relation is None) or (routine and not (member.body or "").strip()):
        found.append(D("SST-RND041", origin=member.origin, subject=key, artifact=member.name))
    defaulted: tuple[tuple[str, str | None], ...] = ()
    if searching:
        defaulted = (("warehouse", member.warehouse), ("target_lag", member.target_lag))
    elif member.type == ToolKind.PROCEDURE.value:
        defaulted = (("warehouse", member.warehouse),)
    for field, value in defaulted:
        if not value:
            found.append(D("SST-RND042", origin=member.origin, subject=key, artifact=member.name, field=field))
    return tuple(found)


def render_tool(member: ToolMember, target: QualifiedName, source_relation: QualifiedName | None) -> RenderedArtifact:
    """Render a defined member as the one object it publishes: a search service, routine, or stage.

    Args:
        target: The name the object is created under.
        source_relation: The relation a search service's `on:` model resolves to, which only a
            search service reads; None when there is none.

    Raises:
        ValueError: the member's type is not publishable; a search service or routine lacks a
            field it needs; a routine body holds `$$`; a name it renders as an identifier or a
            three-part name is not one; a type is not a Snowflake type; a language or
            `execute_as` is unknown; or `where:` is not one expression.
    """
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        return _render_search(member, target, source_relation)
    if member.type == ToolKind.PROCEDURE.value:
        return _render_routine(member, target, procedure=True)
    if member.type == ToolKind.FUNCTION.value:
        return _render_routine(member, target, procedure=False)
    if member.type == ToolKind.STAGE.value:
        return _render_stage(member, target)
    raise ValueError(f"managed tool kind {member.type!r} is not publishable")


def search_service_statement(
    member: ToolMember,
    target: QualifiedName,
    source_relation: QualifiedName | None,
    *,
    comment: str | None = None,
) -> Sql:
    """Render `CREATE OR REPLACE CORTEX SEARCH SERVICE`, canonicalized as a rendered artifact is.

    The query selects the member's declared columns, then its attribute columns, then its search
    column, each once, from `source_relation`, and appends `where:` once it is guarded as one
    expression. The comment is the description with each run of whitespace collapsed to one
    space, unless `comment` gives the exact text, which is written as given.

    Raises:
        ValueError: there is no source relation, search column, warehouse, or target lag; a
            column or the warehouse is not a valid identifier; or `where:` is not one expression.

    Example:
        CREATE OR REPLACE CORTEX SEARCH SERVICE DB.S.DOCS_SEARCH
          ON BODY
          ATTRIBUTES CATEGORY
          WAREHOUSE = WH
          TARGET_LAG = '1 hour'
          AS SELECT CATEGORY, BODY FROM DB.S.DOCS WHERE IS_PUBLIC
    """
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
        sql("CREATE OR REPLACE CORTEX SEARCH SERVICE {target}", target=qname(target)),
        sql("  ON {column}", column=_identifier(member.search_column)),
    ]
    if member.attribute_columns:
        clauses.append(sql("  ATTRIBUTES {columns}", columns=_identifiers(member.attribute_columns)))
    clauses.append(sql("  WAREHOUSE = {warehouse}", warehouse=_identifier(member.warehouse)))
    clauses.append(sql("  TARGET_LAG = {lag}", lag=literal(member.target_lag)))
    if member.embedding_model:
        clauses.append(sql("  EMBEDDING_MODEL = {model}", model=literal(member.embedding_model)))
    if member.refresh_mode:
        if member.refresh_mode.upper() not in REFRESH_MODES:
            raise ValueError(f"search service {member.name!r} has an unknown refresh mode {member.refresh_mode!r}")
        clauses.append(sql("  REFRESH_MODE = {mode}", mode=keyword(member.refresh_mode.upper())))
    query = sql("SELECT {columns} FROM {source}", columns=_identifiers(selected), source=qname(source_relation))
    if member.where:
        query = sql("{query} WHERE {where}", query=query, where=expr(guard_expression(member.where)))
    as_clause = sql("  AS {query}", query=query)
    if comment is None:
        if member.description:
            clauses.append(sql("  COMMENT = {comment}", comment=literal(_one_line(member.description))))
        return canonical(join("\n", (*clauses, as_clause)))
    return join(
        "\n",
        (
            canonical(join("\n", clauses)),
            sql("  COMMENT = {comment}", comment=literal(comment)),
            canonical(as_clause, keep_indent=True),
        ),
    )


def _render_search(
    member: ToolMember,
    target: QualifiedName,
    source_relation: QualifiedName | None,
) -> RenderedArtifact:
    """Render a search service, whose grants are replayed after a replace; see `search_service_statement`."""
    statement = search_service_statement(member, target, source_relation)
    assert source_relation is not None
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=statement,
        shape=PublishShape("CORTEX SEARCH SERVICE", grant_preservation=GrantPreservation.REPLAY),
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                sql("DESCRIBE CORTEX SEARCH SERVICE {target}", target=qname(target)),
            ),
        ),
        required_relations=(source_relation,),
    )


def routine_signature(object_type: str, target: QualifiedName, types: tuple[str, ...]) -> Sql:
    """Name a procedure or function with its argument types, as DESCRIBE, ALTER, and GRANT address it.

    A routine with no arguments is named with empty parentheses, as `routine_ref` renders it.

    Example:
        DB.S.LOOKUP(NUMBER, VARCHAR)

    Raises:
        ValueError: `object_type` is not PROCEDURE or FUNCTION, or a type is not a Snowflake type.
    """
    if " ".join(object_type.upper().split()) not in ("PROCEDURE", "FUNCTION"):
        raise ValueError(f"{object_type!r} is not a routine type")
    return routine_ref(target, types)


def _handler_clauses(member: ToolMember) -> list[Sql]:
    """RUNTIME_VERSION, HANDLER, and when set PACKAGES and IMPORTS, for a Java, Python or Scala routine.

    Any other routine gets none: their grammars have no such clauses, and validation reports each
    of the four keys set on one (SST-VAL621).

    Raises:
        ValueError: a Java, Python or Scala routine lacks a runtime version or handler.
    """
    if (member.language or "").strip().casefold() not in HANDLER_LANGUAGES:
        return []
    if not member.runtime_version or not member.handler:
        raise ValueError(f"routine {member.name!r} requires runtime_version and handler")
    clauses = [
        sql("  RUNTIME_VERSION = {version}", version=literal(member.runtime_version)),
        sql("  HANDLER = {handler}", handler=literal(member.handler)),
    ]
    if member.packages:
        clauses.append(
            sql("  PACKAGES = ({packages})", packages=join(", ", (literal(value) for value in member.packages)))
        )
    if member.imports:
        clauses.append(sql("  IMPORTS = ({imports})", imports=join(", ", (literal(value) for value in member.imports))))
    return clauses


def _render_routine(member: ToolMember, target: QualifiedName, *, procedure: bool) -> RenderedArtifact:
    """Render `CREATE OR REPLACE PROCEDURE` or `FUNCTION` with `COPY GRANTS`, its body dollar-quoted.

    The clauses come in a fixed order: the signature, COPY GRANTS, RETURNS, and LANGUAGE; then,
    for Java, Python and Scala only, RUNTIME_VERSION and HANDLER, which they require, and when
    set PACKAGES and IMPORTS (the SQL and JavaScript grammars have none of the four); then, when
    set, EXTERNAL_ACCESS_INTEGRATIONS, and SECRETS sorted by name; then, when the member has a
    description, COMMENT with each run of whitespace collapsed to one space; then a procedure's
    EXECUTE AS, CALLER by default; then the body. A procedure also requires `warehouse`,
    which the DDL itself does not name. The comment is in the text the fingerprint digests, so
    an edit of only the description is a change; Snowflake refuses COMMENT after EXECUTE AS.
    Publishing then sets the comment again with the ownership marker after it. The relations
    the body's `ref()` and `source()` calls named are the ones apply checks exist first.

    Args:
        procedure: Render a procedure rather than a function.

    Raises:
        ValueError: the language, body, or return type is missing, or a procedure has no
            warehouse; a Java, Python or Scala routine lacks a runtime version or handler; the body
            holds `$$`; a parameter or integration name is not an identifier; a type is not a
            Snowflake type; the language or `execute_as` is unknown; or a secret is not a
            three-part name.

    Example:
        CREATE OR REPLACE PROCEDURE DB.S.LOOKUP(ORDER_ID NUMBER)
          COPY GRANTS
          RETURNS NUMBER
          LANGUAGE SQL
          COMMENT = 'Look an order up.'
          EXECUTE AS CALLER
          AS $$
        <body>
        $$
    """
    if not member.language or member.body is None or not member.returns or (procedure and not member.warehouse):
        raise ValueError(f"routine {member.name!r} is missing required render fields")
    object_type = "PROCEDURE" if procedure else "FUNCTION"
    signature = join(
        ", ",
        (
            sql("{name} {type}", name=_identifier(parameter.name), type=datatype(parameter.type))
            for parameter in member.signature
        ),
    )
    clauses = [
        sql(
            "CREATE OR REPLACE {kind} {target}({signature})",
            kind=keyword(object_type),
            target=qname(target),
            signature=signature,
        ),
        sql("  COPY GRANTS"),
        sql("  RETURNS {returns}", returns=_returns(member.returns)),
        sql("  LANGUAGE {language}", language=_language(member.language)),
    ]
    clauses.extend(_handler_clauses(member))
    if member.external_access_integrations:
        clauses.append(
            sql(
                "  EXTERNAL_ACCESS_INTEGRATIONS = ({integrations})",
                integrations=_identifiers(member.external_access_integrations),
            )
        )
    if member.secrets:
        secrets = join(
            ", ",
            (
                sql("{key} = {secret}", key=literal(key), secret=qname(QualifiedName.parse(value)))
                for key, value in sorted(member.secrets.items())
            ),
        )
        clauses.append(sql("  SECRETS = ({secrets})", secrets=secrets))
    if member.description:
        clauses.append(sql("  COMMENT = {comment}", comment=literal(_one_line(member.description))))
    if procedure:
        clauses.append(sql("  EXECUTE AS {rights}", rights=keyword(member.execute_as or "caller")))
    clauses.append(sql("  AS {body}", body=dollar_quoted(f"\n{member.body.rstrip()}\n")))
    types = tuple(parameter.type for parameter in member.signature)
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=join("\n", clauses),
        shape=PublishShape(object_type, grant_preservation=GrantPreservation.CLAUSE, routine_signature=types),
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                sql(
                    "DESCRIBE {kind} {routine}",
                    kind=keyword(object_type),
                    routine=routine_signature(object_type, target, types),
                ),
            ),
        ),
        required_relations=tuple(
            QualifiedName.parse(relation) for relation in sorted(set(member.body_relations.values()))
        ),
    )


def _render_stage(member: ToolMember, target: QualifiedName) -> RenderedArtifact:
    ddl = sql("CREATE STAGE IF NOT EXISTS {target}", target=qname(target))
    if member.description:
        ddl = sql("{ddl} COMMENT = {comment}", ddl=ddl, comment=literal(_one_line(member.description)))
    return RenderedArtifact.create(
        key=artifact_key("tool", member.name.casefold()),
        artifact_type="tool",
        target=target,
        ddl=ddl,
        shape=PublishShape("STAGE", grant_preservation=GrantPreservation.NONE),
        smoke=(
            SmokeProbe(
                f"{artifact_key('tool', member.name.casefold())}:describe",
                ProbeKind.DESCRIBE,
                sql("DESCRIBE STAGE {target}", target=qname(target)),
            ),
        ),
    )


def _identifier(value: str) -> Sql:
    return ident(Identifier.parse(value))


def _identifiers(values: tuple[str, ...]) -> Sql:
    return join(", ", (_identifier(value) for value in values))


def _language(value: str) -> Sql:
    """A routine's language: one of the languages Snowflake runs routines in."""
    if value.strip().upper() not in ("SQL", "PYTHON", "JAVA", "JAVASCRIPT", "SCALA"):
        raise ValueError(f"{value!r} is not a routine language")
    return keyword(value)


def _returns(value: str) -> Sql:
    """A routine's return type: a Snowflake type, or `TABLE (<column> <type>, ...)` for a table function.

    Raises:
        ValueError: the value is neither, or a column is not an identifier and a type.
    """
    match = _TABLE_RETURNS.fullmatch(value.strip())
    if match is None:
        return datatype(value)
    columns: list[Sql] = []
    for column in _top_level_split(match.group("columns")):
        name, _, column_type = column.strip().partition(" ")
        columns.append(sql("{name} {type}", name=_identifier(name), type=datatype(column_type)))
    return sql("TABLE ({columns})", columns=join(", ", columns))


def _top_level_split(text: str) -> list[str]:
    """Split at each comma outside parentheses, so `NUMBER(38, 2)` stays one type."""
    parts: list[str] = []
    depth = 0
    start = 0
    for index, character in enumerate(text):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return parts


def _one_line(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
