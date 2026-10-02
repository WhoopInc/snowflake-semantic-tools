"""Compile managed tool declarations into generic lifecycle artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact, compile_each
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, RenderedArtifact
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolKind, ToolMember
from snowflake_semantic_tools.domain.render.tool import render_tool, routine_signature, search_service_statement
from snowflake_semantic_tools.domain.sql import keyword, literal, qname, sql


@dataclass(frozen=True, slots=True)
class CompiledTool(StandaloneArtifact):
    """One `define:` tool member, with its configured defaults applied, and the object it renders to."""

    member: ToolMember
    rendered: RenderedArtifact

    @property
    def name(self) -> str:
        return str(self.member.name)

    @property
    def artifact_key(self) -> str:
        return self.rendered.key

    @property
    def artifact_type(self) -> str:
        return "tool"

    @property
    def source_files(self) -> tuple[str, ...]:
        files = [self.member.source_file]
        if self.member.body_file:
            files.append(self.member.body_file)
        return tuple(dict.fromkeys(files))

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Return the model a search service reads, if the member names one."""
        return (self.member.on_model,) if self.member.on_model else ()

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        """Return the model with the relation the search service renders over; empty without one."""
        if not self.member.on_model or not self.rendered.required_relations:
            return ()
        return ((self.member.on_model, self.rendered.required_relations[0].sql),)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self.rendered

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Carry the ownership marker, then the member's description, in the object's COMMENT.

        A search service takes the comment inside its CREATE statement; a procedure, a
        function, or a stage gets one more statement that sets it. Any other object type
        keeps its statements. Apply expects the marker either way.
        """
        marker = OwnershipMarker(manifest_id, self.rendered.fingerprint).text
        description = self.member.description
        comment_value = f"{marker} {description}" if description else marker
        statements = self.rendered.statements
        if self.rendered.object_type == "CORTEX SEARCH SERVICE":
            source = self.rendered.required_relations[0] if self.rendered.required_relations else None
            statements = (search_service_statement(self.member, self.rendered.target, source, comment=comment_value),)
        elif self.rendered.object_type in {"PROCEDURE", "FUNCTION", "STAGE"}:
            object_name = qname(self.rendered.target)
            if self.rendered.routine_signature:
                object_name = routine_signature(
                    self.rendered.object_type, self.rendered.target, self.rendered.routine_signature
                )
            statements = (
                *statements,
                sql(
                    "ALTER {kind} {name} SET COMMENT = {comment}",
                    kind=keyword(self.rendered.object_type),
                    name=object_name,
                    comment=literal(comment_value),
                ),
            )
        return replace(
            self.rendered,
            statements=statements,
            expected_marker=OwnershipMarker(manifest_id, self.rendered.fingerprint),
        )


class CompileTools:
    """Compile each `define:` member of a tool catalog; `reference:` members compile to nothing.

    The `tools:` defaults fill what a member leaves unset: warehouse, target lag and
    embedding model for a search service, warehouse and `execute_as` for a procedure.
    """

    def __init__(
        self,
        catalog: ToolCatalog,
        *,
        database: str,
        schema: str,
        warehouse: str | None,
        target_lag: str | None,
        embedding_model: str | None,
        execute_as: str | None,
        dbt_relations: dict[str, str],
    ) -> None:
        self._catalog = catalog
        self._database = database
        self._schema = schema
        self._warehouse = warehouse
        self._target_lag = target_lag
        self._embedding_model = embedding_model
        self._execute_as = execute_as
        self._dbt_relations = dbt_relations

    def run_result(self) -> CompileResult:
        """Compile the managed members in casefolded name order.

        The diagnostics are the catalog's, then any SST-INT902. A member that any catalog
        diagnostic names, whatever its severity, is not compiled.

        Diagnostics:
            SST-INT902: rendering a member raised KeyError, TypeError or ValueError.
        """
        poisoned = {diagnostic.subject for diagnostic in self._catalog.diagnostics if diagnostic.subject}
        return compile_each(
            sorted(self._catalog.managed, key=lambda item: item.name.casefold()),
            key=lambda member: artifact_key("tool", member.name.casefold()),
            render=self._compile,
            diagnostics=self._catalog.diagnostics,
            # Not the default rule: a warning poisons a member too, and only the catalog's
            # diagnostics count, so a rendering failure never skips a later member.
            skip=lambda subject, _: subject in poisoned,
            origin=lambda member: member.origin,
        )

    def _compile(self, member: ToolMember) -> CompiledTool:
        effective = _defaults(
            member,
            warehouse=self._warehouse,
            target_lag=self._target_lag,
            embedding_model=self._embedding_model,
            execute_as=self._execute_as,
        )
        target = QualifiedName.from_parts(self._database, self._schema, member.name)
        source = (
            QualifiedName.parse(self._dbt_relations[member.on_model])
            if member.on_model in self._dbt_relations
            else None
        )
        return CompiledTool(effective, render_tool(effective, target, source))


def _defaults(
    member: ToolMember,
    *,
    warehouse: str | None,
    target_lag: str | None,
    embedding_model: str | None,
    execute_as: str | None,
) -> ToolMember:
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        return replace(
            member,
            warehouse=member.warehouse or warehouse,
            target_lag=member.target_lag or target_lag,
            embedding_model=member.embedding_model or embedding_model,
        )
    if member.type == ToolKind.PROCEDURE.value:
        return replace(
            member,
            warehouse=member.warehouse or warehouse,
            execute_as=member.execute_as or execute_as or "caller",
        )
    return member
