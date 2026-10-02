"""Check a tool catalog: its groups, each member's fields and references, and the SQL its DDL writes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import (
    KNOWN_TOOL_TYPES,
    ToolCatalog,
    ToolGroup,
    ToolKind,
    ToolMember,
    ToolOwnership,
)
from snowflake_semantic_tools.domain.sql import is_datatype
from snowflake_semantic_tools.domain.validate.shared import Emitter
from snowflake_semantic_tools.domain.validate.sql import checked_expression, name_problem, qualified_name_problem


def validate_tool_catalog(catalog: ToolCatalog, dbt: DbtCatalog) -> DiagnosticBag:
    """Check every tool group and member, returning the catalog's load diagnostics with the new ones.

    Returns:
        Every diagnostic, sorted by origin file (none first), then code; ties keep the
        order they were found in.

    Diagnostics:
        SST-VAL606: an immutable group declares `define:` members.
        SST-VAL603: a member's type is not a known tool type.
        SST-VAL604: a `define:` member declares neither `on:` nor `body_file:`, and is not a stage.
        SST-VAL605: a `define:` member declares `relations`, or a `reference:` member a creation key.
        SST-PRS002: a `reference:` member declares no `relations`.
        SST-REF018: a relation names a target `profiles.yml` does not declare, or none names the current one.
        SST-REF019: a relation is not a three-part name.
        SST-REF023: a mutable group's reference resolves to one object for both dev and prod.
        SST-PRS032: a signature parameter has type OBJECT; only the first is reported.
        SST-VAL608: a defined search service's `on:` is not a dbt model.
        SST-VAL609: a search or attribute column is not on the search service's model.
        SST-LOD018: a defined member's `body_file:` does not exist.
        SST-LOD019: a defined member's `body_file:` is empty.
        SST-PRS005: a name a defined member's DDL writes is not an identifier, or a secret not a
            three-part name.
        SST-PRS003: a defined member's parameter or return type is not a Snowflake data type.
        SST-PRS013: a defined member's language, `execute_as` or `refresh_mode` is not one
            Snowflake accepts.
        SST-VAL418: a defined member's `where:` is not one expression.
        SST-VAL601: a group declares one member name twice, ignoring case, under one of `define:` and
            `reference:`.
        SST-CFG019: a group declares one member name under both `define:` and `reference:`.
        SST-VAL001: a group name is declared twice, ignoring case.
        SST-VAL602: one member name is declared in two groups.
    """
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    for group in catalog.groups:
        diagnostics.extend(_validate_group(group, catalog, dbt))
    diagnostics.extend(_duplicate_groups(catalog.groups))
    diagnostics.extend(_names_in_several_groups(catalog.groups))
    return DiagnosticBag(sorted(diagnostics, key=lambda item: (item.origin.file if item.origin else "", item.code)))


def _validate_group(group: ToolGroup, catalog: ToolCatalog, dbt: DbtCatalog) -> list[Diagnostic]:
    """Check one group, then each of its members, then member names it repeats."""
    diagnostics: list[Diagnostic] = []
    if group.immutable and any(member.ownership is ToolOwnership.DEFINE for member in group.members):
        diagnostics.append(D("SST-VAL606", a=group.name, origin=group.origin, subject=f"tool_group:{group.name}"))
    within: dict[str, list[ToolMember]] = {}
    for member in group.members:
        within.setdefault(member.name.casefold(), []).append(member)
        diagnostics.extend(_validate_member(member, catalog, dbt))
    for duplicate in within.values():
        if len(duplicate) > 1 and len({member.ownership for member in duplicate}) > 1:
            # Owned and referenced at once: which of the two the member is cannot be decided.
            diagnostics.append(
                D(
                    "SST-CFG019",
                    name=duplicate[0].name,
                    origin=duplicate[0].origin,
                    related=tuple(member.origin for member in duplicate[1:]),
                    subject=f"tool_group:{group.name}",
                )
            )
        elif len(duplicate) > 1:
            diagnostics.append(
                D(
                    "SST-VAL601",
                    a=group.name,
                    name=duplicate[0].name,
                    origin=duplicate[0].origin,
                    related=tuple(member.origin for member in duplicate[1:]),
                )
            )
    return diagnostics


def _duplicate_groups(groups: tuple[ToolGroup, ...]) -> list[Diagnostic]:
    """Report each group name declared more than once, at its first declaration."""
    groups_by_name: dict[str, list[ToolGroup]] = {}
    for group in groups:
        groups_by_name.setdefault(group.name.casefold(), []).append(group)
    return [
        D(
            "SST-VAL001",
            type="tool group",
            name=duplicate_groups[0].name,
            origin=duplicate_groups[0].origin,
            related=tuple(group.origin for group in duplicate_groups[1:]),
        )
        for duplicate_groups in groups_by_name.values()
        if len(duplicate_groups) > 1
    ]


def _names_in_several_groups(groups: tuple[ToolGroup, ...]) -> list[Diagnostic]:
    """Report each member name declared in more than one group, naming the first two groups."""
    members_by_name: dict[str, list[ToolMember]] = {}
    for group in groups:
        for member in group.members:
            members_by_name.setdefault(member.name.casefold(), []).append(member)
    diagnostics: list[Diagnostic] = []
    for duplicate_members in members_by_name.values():
        owners = tuple(dict.fromkeys(member.group for member in duplicate_members))
        if len(owners) > 1:
            diagnostics.append(
                D(
                    "SST-VAL602",
                    name=duplicate_members[0].name,
                    a=owners[0],
                    b=owners[1],
                    origin=duplicate_members[0].origin,
                )
            )
    return diagnostics


def _validate_member(member: ToolMember, catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Run every member rule, in this order; each reports against the member's key and origin."""
    subject = artifact_key("tool", member.name.casefold())
    return (
        *_known_type(member, subject),
        *_define_fields(member, subject),
        *_reference_fields(member, subject),
        *_reference_relations(member, catalog, subject),
        *_object_parameter(member, subject),
        *_search_columns(member, dbt, subject),
        *_body_file(member, subject),
        *_sql_values(member, subject),
    )


def _known_type(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a type that is not a known tool type."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.type not in KNOWN_TOOL_TYPES:
        emit("SST-VAL603", name=member.name, found=member.type, expected=", ".join(sorted(KNOWN_TOOL_TYPES)))
    return emit.diagnostics


def _define_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a `define:` member with nothing to create it from, or with `relations`."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE:
        return emit.diagnostics
    if not member.on_model and not member.body_file and member.type != ToolKind.STAGE.value:
        emit("SST-VAL604", name=member.name)
    if member.relations:
        emit("SST-VAL605", name=member.name, key="relations")
    return emit.diagnostics


def _reference_fields(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a `reference:` member without `relations`, then each creation key it declares."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is ToolOwnership.DEFINE:
        return emit.diagnostics
    if not member.relations:
        emit("SST-PRS002", artifact=subject, field="relations")
    for key in member.creation_keys:
        emit("SST-VAL605", name=member.name, key=key)
    return emit.diagnostics


def _reference_relations(member: ToolMember, catalog: ToolCatalog, subject: str) -> tuple[Diagnostic, ...]:
    """Check each relation of a `reference:` member, then its current target and shared object.

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
        emit("SST-REF018", ref_function="tool", name=f"{member.group}', '{member.name}", target=catalog.target_name)
    if _shares_one_object(member, catalog):
        emit("SST-REF023", ref_function="tool", name=f"{member.group}', '{member.name}", value=member.relations["dev"])
    return emit.diagnostics


def _shares_one_object(member: ToolMember, catalog: ToolCatalog) -> bool:
    """Return whether dev and prod name one object although the member's group is not immutable.

    Mutability comes from the first group declared under the member's exact group name.
    """
    return (
        not next(group.immutable for group in catalog.groups if group.name == member.group)
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


def _body_file(member: ToolMember, subject: str) -> tuple[Diagnostic, ...]:
    """Report a defined member's `body_file:` that could not be read, or that is blank."""
    emit = Emitter(subject=subject, origin=member.origin)
    if member.ownership is not ToolOwnership.DEFINE or not member.body_file:
        return emit.diagnostics
    if member.body is None:
        emit("SST-LOD018", file=member.source_file, path=member.body_file)
    elif not member.body.strip():
        emit("SST-LOD019", path=member.body_file, file=member.source_file)
    return emit.diagnostics


_LANGUAGES = ("java", "javascript", "python", "scala", "sql")
_EXECUTE_AS = ("caller", "owner", "restricted caller")
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
        SST-VAL612: a `reference:` member is among the members rendered.
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
        ("execute_as", member.execute_as, _EXECUTE_AS),
        ("refresh_mode", member.refresh_mode, _REFRESH_MODES),
    ):
        if value and " ".join(value.casefold().split()) not in allowed:
            emit("SST-PRS013", field=key, found=value, expected=", ".join(allowed))
    if not member.where or "{{" in member.where:
        return emit.diagnostics
    found = checked_expression(member.where, kind="tool", name=member.name, subject=subject, origin=member.origin)
    return (*emit.diagnostics, found) if isinstance(found, Diagnostic) else emit.diagnostics
