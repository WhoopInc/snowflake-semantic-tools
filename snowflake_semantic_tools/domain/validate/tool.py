"""Check a tool catalog: each member's fields and references, the SQL its DDL writes, and its name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import (
    EXECUTE_AS,
    HANDLER_LANGUAGES,
    KNOWN_TOOL_TYPES,
    NON_HANDLER_LANGUAGES,
    ToolCatalog,
    ToolKind,
    ToolMember,
    ToolOwnership,
    vocabulary_key,
)
from snowflake_semantic_tools.domain.sql import is_datatype
from snowflake_semantic_tools.domain.validate.shared import Emitter
from snowflake_semantic_tools.domain.validate.sql import (
    checked_expression,
    name_problem,
    qualified_name_problem,
    quoted_name_problem,
)


def validate_tool_catalog(catalog: ToolCatalog, dbt: DbtCatalog) -> DiagnosticBag:
    """Check every tool member, returning the catalog's load diagnostics with the new ones.

    Returns:
        Every diagnostic, sorted by origin file (none first), then code; ties keep the
        order they were found in.

    Diagnostics:
        SST-VAL606: a member's settings resolve `immutable` and it writes no `relations:`.
        SST-VAL603: a member's type is not a known tool type.
        SST-VAL604: a defined member declares neither `on:` nor `body:`, and is not a stage.
        SST-VAL605: a referenced member declares a creation key.
        SST-PRS002: a referenced member's `relations:` are empty; a defined search service has
            no `search_column`; a defined routine no `language` or `returns`, or, in Java,
            Python or Scala, no `runtime_version` or `handler`.
        SST-VAL621: a defined JavaScript or SQL routine sets `runtime_version`, `handler`,
            `packages` or `imports`; once per key.
        SST-REF018: a relation names a target `profiles.yml` does not declare, or none names the current one.
        SST-REF019: a relation is not a three-part name.
        SST-REF023: a mutable member's relations name one object for both dev and prod.
        SST-PRS032: a signature parameter has type OBJECT; only the first is reported.
        SST-VAL608: a defined search service's `on:` is not a dbt model.
        SST-VAL609: a search or attribute column is not on the search service's model.
        SST-PRS005: a name a defined member's DDL writes is not an identifier, or a secret not a
            three-part name.
        SST-PRS003: a defined member's parameter or return type is not a Snowflake data type.
        SST-PRS013: a defined member's language, `execute_as` or `refresh_mode` is not one
            Snowflake accepts.
        SST-VAL418: a defined member's `where:` is not one expression.
        SST-VAL001: a member name is declared twice anywhere in the project, ignoring case.
        SST-PRS005: a defined member's name is written quoted, as `quoted_name_problem` reports.
    """
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    for member in catalog.members:
        diagnostics.extend(_validate_member(member, catalog, dbt))
    diagnostics.extend(_quoted_names(catalog.members))
    diagnostics.extend(_duplicate_names(catalog.members))
    return DiagnosticBag(sorted(diagnostics, key=lambda item: (item.origin.file if item.origin else "", item.code)))


def _quoted_names(members: tuple[ToolMember, ...]) -> list[Diagnostic]:
    """Report each defined member whose name is written quoted, which SST would publish and never find."""
    found: list[Diagnostic] = []
    for member in members:
        subject = artifact_key("tool", member.name.casefold())
        problem = (
            quoted_name_problem(member.name, artifact=subject, subject=subject, origin=member.origin)
            if member.ownership is ToolOwnership.DEFINE
            else None
        )
        if problem is not None:
            found.append(problem)
    return found


def _duplicate_names(members: tuple[ToolMember, ...]) -> list[Diagnostic]:
    """Report each member name declared more than once, at its first declaration."""
    by_name: dict[str, list[ToolMember]] = {}
    for member in members:
        by_name.setdefault(member.name.casefold(), []).append(member)
    return [
        D(
            "SST-VAL001",
            type="tool member",
            name=duplicate[0].name,
            origin=duplicate[0].origin,
            related=tuple(member.origin for member in duplicate[1:]),
            subject=artifact_key("tool", duplicate[0].name.casefold()),
        )
        for duplicate in by_name.values()
        if len(duplicate) > 1
    ]


def _validate_member(member: ToolMember, catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Run every member rule, in this order; each reports against the member's key and origin."""
    subject = artifact_key("tool", member.name.casefold())
    return (
        *_known_type(member, subject),
        *_immutable(member, subject),
        *_define_fields(member, subject),
        *_reference_fields(member, subject),
        *_reference_relations(member, catalog, subject),
        *_object_parameter(member, subject),
        *_search_columns(member, dbt, subject),
        *_render_fields(member, subject),
        *_handler_keys(member, subject),
        *_sql_values(member, subject),
    )


def _known_type(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a type that is not a known tool type."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.type not in KNOWN_TOOL_TYPES:
        emit("SST-VAL603", name=member.name, found=member.type, expected=", ".join(sorted(KNOWN_TOOL_TYPES)))
    return emit.diagnostics


def _immutable(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a member whose settings make it immutable, which SST would nonetheless create."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.immutable and member.ownership is ToolOwnership.DEFINE:
        emit("SST-VAL606", name=member.name)
    return emit.diagnostics


def _define_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a defined member with nothing to create it from.

    What it declares counts, not what reading it gave: a `body:` the loader refused is reported
    there, once, and not again here.
    """
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE:
        return emit.diagnostics
    declared = member.on_model or member.body_file or "body" in member.creation_keys
    if not declared and member.type != ToolKind.STAGE.value:
        emit("SST-VAL604", name=member.name)
    return emit.diagnostics


def _reference_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a referenced member whose `relations:` are empty, then each creation key it declares."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is ToolOwnership.DEFINE:
        return emit.diagnostics
    if not member.relations:
        emit("SST-PRS002", artifact=subject, field="relations")
    for key in member.creation_keys:
        emit("SST-VAL605", name=member.name, key=key)
    return emit.diagnostics


def _reference_relations(member: ToolMember, catalog: ToolCatalog, subject: str) -> tuple[Diagnostic, ...]:
    """Check each relation of a referenced member, then its current target and shared object.

    Each relation's undeclared target is reported before its malformed name.
    """
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is ToolOwnership.DEFINE:
        return emit.diagnostics
    for target_name, value in member.relations.items():
        if target_name not in catalog.declared_targets:
            emit("SST-REF018", ref_function="tool target", name=target_name, target="profiles.yml")
        try:
            QualifiedName.parse(value)
        except ValueError:
            emit("SST-REF019", value=value)
    if catalog.target_name not in member.relations:
        emit("SST-REF018", ref_function="tool", name=member.name, target=catalog.target_name)
    if _shares_one_object(member):
        emit("SST-REF023", ref_function="tool", name=member.name, value=member.relations["dev"])
    return emit.diagnostics


def _shares_one_object(member: ToolMember) -> bool:
    """Return whether dev and prod name one object although the member's settings do not make it immutable."""
    return (
        not member.immutable
        and "dev" in member.relations
        and "prod" in member.relations
        and member.relations["dev"].casefold() == member.relations["prod"].casefold()
    )


def _object_parameter(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report the first signature parameter typed OBJECT, which tool input schemas cannot carry."""
    emit = Emitter(subject=subject, origin=member.origin)
    parameter = next((parameter for parameter in member.signature if parameter.type.casefold() == "object"), None)
    if parameter is not None:
        emit("SST-PRS032", artifact=subject, field=parameter.name, found=parameter.type)
    return emit.diagnostics


def _search_columns(member: ToolMember, dbt: DbtCatalog, subject: str) -> tuple[Diagnostic, ...]:
    """Check that a defined search service reads a dbt model that has its search and attribute columns."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE or member.type != ToolKind.CORTEX_SEARCH_SERVICE.value:
        return emit.diagnostics
    model = dbt.model(member.on_model or "")
    if model is None:
        emit("SST-VAL608", name=member.name, value=member.on_model or "")
        return emit.diagnostics
    for column in (member.search_column, *member.attribute_columns):
        if column and model.column(column) is None:
            emit("SST-VAL609", name=member.name, column=column, value=model.name)
    return emit.diagnostics


def _render_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a field a defined member's DDL needs that no setting can supply.

    A warehouse and a target lag are settings, which a folder route or the `tools:` root may
    set, so they are checked once the member's settings resolve, when it compiles (SST-RND042).
    """
    emit = Emitter(subject=subject, origin=member.origin, artifact=subject)
    if member.ownership is not ToolOwnership.DEFINE:
        return emit.diagnostics
    required: tuple[tuple[str, str | None], ...] = ()
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        required = (("search_column", member.search_column),)
    elif member.type in (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value):
        required = (("language", member.language), ("returns", member.returns))
        if member.language and member.language.strip().casefold() in HANDLER_LANGUAGES:
            required += (("runtime_version", member.runtime_version), ("handler", member.handler))
    for field, value in required:
        if not value:
            emit("SST-PRS002", field=field)
    return emit.diagnostics


def _handler_keys(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report each key only a Java, Python or Scala routine takes, set on one in another language.

    A language SST does not recognise is SST-PRS013's, so it is not checked here.
    """
    emit = Emitter(subject=subject, origin=member.origin, artifact=subject)
    language = (member.language or "").strip().casefold()
    if (
        member.ownership is not ToolOwnership.DEFINE
        or member.type not in (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value)
        or language not in NON_HANDLER_LANGUAGES
    ):
        return emit.diagnostics
    keys = (
        ("runtime_version", member.runtime_version),
        ("handler", member.handler),
        ("packages", member.packages),
        ("imports", member.imports),
    )
    for key, value in keys:
        if value:
            emit("SST-VAL621", name=member.name, value=language, key=key)
    return emit.diagnostics


_LANGUAGES = ("java", "javascript", "python", "scala", "sql")
_REFRESH_MODES = ("auto", "full", "incremental")
# dbt materializations that rebuild the relation on every run instead of merging into it.
_REPLACING_MATERIALIZATIONS = frozenset(("table", "view"))


def rebuilt_source(member: ToolMember, relation: str, materialized: str | None) -> Diagnostic | None:
    """Report a search service whose source relation dbt rebuilds, unless it refreshes FULL anyway.

    A rebuilt relation loses the change tracking an incremental refresh reads, so every dbt
    run re-embeds the whole index; a FULL refresh pays that cost by choice.

    Args:
        relation: The relation the service's `on:` model resolves to.
        materialized: The model's dbt materialization; None when the manifest does not say.

    Diagnostics:
        SST-VAL617: the model is materialized as a table or a view and the service does not
            refresh FULL.
    """
    replaced = (materialized or "").casefold() in _REPLACING_MATERIALIZATIONS
    if member.type != ToolKind.CORTEX_SEARCH_SERVICE.value or not replaced:
        return None
    if (member.refresh_mode or "").upper() == "FULL":
        return None
    return D(
        "SST-VAL617",
        name=member.name,
        value=relation,
        detail=materialized,
        origin=member.origin,
        subject=artifact_key("tool", member.name.casefold()),
    )


def reference_ddl(members: tuple[ToolMember, ...]) -> tuple[Diagnostic, ...]:
    """Report each member DDL was rendered for although it is only referenced.

    SST never creates, replaces or prunes an object it references; compiling one would.

    Diagnostics:
        SST-VAL612: a referenced member is among the members rendered.
    """
    return tuple(
        D("SST-VAL612", name=member.name, origin=member.origin, subject=artifact_key("tool", member.name.casefold()))
        for member in members
        if member.ownership is ToolOwnership.REFERENCE
    )


def _sql_values(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Check each value a defined member's DDL writes unquoted, before anything renders it.

    A value still holding a `{{ ... }}` template is left to the resolver that fills it.

    Diagnostics:
        SST-PRS005: a column, parameter, warehouse, or integration name is not an identifier, or
            a secret is not a three-part name.
        SST-PRS003: a parameter or return type is not a Snowflake data type.
        SST-PRS013: the language, `execute_as` or `refresh_mode` is not one Snowflake accepts.
        SST-VAL418: `where:` is not one expression.
    """
    if member.ownership is not ToolOwnership.DEFINE:
        return ()
    emit = Emitter(subject=subject, origin=member.origin, artifact=subject)
    names = (
        member.search_column,
        *member.attribute_columns,
        *(column.name for column in member.columns),
        member.warehouse,
        member.configured_warehouse,
        *(parameter.name for parameter in member.signature),
        *member.external_access_integrations,
    )
    for name in names:
        if name and "{{" not in name and name_problem(name, artifact=subject, subject=subject) is not None:
            emit("SST-PRS005", value=name)
    for secret in member.secrets.values():
        if qualified_name_problem(secret, artifact=subject, subject=subject) is not None:
            emit("SST-PRS005", value=secret)
    for key, value in (
        *((f"signature.{item.name}", item.type) for item in member.signature),
        ("returns", member.returns),
    ):
        # A table function's TABLE (...) return is checked column by column when it renders.
        if value and not is_datatype(value) and not value.strip().upper().startswith("TABLE"):
            emit("SST-PRS003", field=key, expected="a Snowflake data type", found=repr(value))
    for key, value, allowed in (
        ("language", member.language, _LANGUAGES),
        ("execute_as", member.execute_as, EXECUTE_AS),
        ("refresh_mode", member.refresh_mode, _REFRESH_MODES),
    ):
        if value and vocabulary_key(value) not in allowed:
            emit("SST-PRS013", field=key, found=value, expected=", ".join(allowed))
    if not member.where or "{{" in member.where:
        return emit.diagnostics
    found = checked_expression(member.where, kind="tool", name=member.name, subject=subject, origin=member.origin)
    return (*emit.diagnostics, found) if isinstance(found, Diagnostic) else emit.diagnostics
