"""Compile managed tool declarations into generic lifecycle artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact, compile_checked
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, RenderedArtifact, marked_comment
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolKind, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.parse.names import NAME_WARNING_CODES
from snowflake_semantic_tools.domain.render.tool import (
    render_tool,
    routine_signature,
    search_service_statement,
    tool_render_checks,
)
from snowflake_semantic_tools.domain.resolve.stage_path import resolve_stage_path
from snowflake_semantic_tools.domain.sql import keyword, literal, qname, sql
from snowflake_semantic_tools.domain.validate.tool import rebuilt_source, reference_ddl


@dataclass(frozen=True, slots=True)
class ToolPlacement:
    """Where one defined tool member publishes, and the settings its DDL renders, resolved for the target.

    Attributes:
        warehouse, target_lag, embedding_model, execute_as: The member's resolved settings, its
            own `config:` over the closest folder route over the `tools:` root; None for one no
            step sets and that has no built-in default.
        settings: Every setting it resolved, with where each came from, which the manifest records.
    """

    database: str
    schema: str
    warehouse: str | None
    target_lag: str | None
    embedding_model: str | None
    execute_as: str | None
    settings: tuple[Setting, ...] = ()


@dataclass(frozen=True, slots=True)
class CompiledTool(StandaloneArtifact):
    """One defined tool member, with its resolved settings applied, and the object it renders to."""

    member: ToolMember
    rendered: RenderedArtifact
    settings: tuple[Setting, ...] = ()

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
        """Return the model a search service reads, or the models and sources a routine's body names."""
        if self.member.on_model:
            return (self.member.on_model,)
        return tuple(self.member.body_relations)

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        """Return each model with the relation the tool reads; empty when it reads none.

        A search service reads the one it renders over, and a routine those its body names.
        """
        if self.member.body_relations:
            return tuple(self.member.body_relations.items())
        if not self.member.on_model or not self.rendered.required_relations:
            return ()
        return ((self.member.on_model, self.rendered.required_relations[0].sql),)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self.rendered

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Carry the member's description, then the ownership marker, in the object's COMMENT.

        A search service takes the comment inside its CREATE statement; a procedure, a
        function, or a stage gets one more statement that sets it, a routine addressed with its
        argument types even when it has none. Any other object type keeps its statements. Apply
        expects the marker either way.
        """
        marker = OwnershipMarker(manifest_id, self.rendered.fingerprint).text
        comment_value = marked_comment(self.member.description, marker)
        statements = self.rendered.statements
        if self.rendered.object_type == "CORTEX SEARCH SERVICE":
            source = self.rendered.required_relations[0] if self.rendered.required_relations else None
            statements = (search_service_statement(self.member, self.rendered.target, source, comment=comment_value),)
        elif self.rendered.object_type in {"PROCEDURE", "FUNCTION", "STAGE"}:
            object_name = qname(self.rendered.target)
            if self.rendered.object_type != "STAGE":
                # A routine's bare name does not parse in ALTER; one with no arguments takes `()`.
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
    """Compile each defined member of a tool catalog; a referenced member compiles to nothing.

    Each member publishes where its placement says, and its placement fills what the member
    leaves unset: warehouse, target lag and embedding model for a search service, warehouse and
    `execute_as` for a procedure.
    """

    def __init__(
        self,
        catalog: ToolCatalog,
        *,
        default: ToolPlacement,
        dbt_relations: dict[str, str],
        placements: Mapping[str, ToolPlacement] | None = None,
        dbt_materializations: dict[str, str | None] | None = None,
    ) -> None:
        """Hold what compiling the catalog reads.

        Args:
            default: The placement of a member `placements` does not name.
            placements: Each defined member's placement, by casefolded member name.
        """
        self._catalog = catalog
        self._default = default
        self._placements = placements or MappingProxyType({})
        self._dbt_relations = dbt_relations
        self._dbt_materializations = dbt_materializations or {}

    def run_result(self) -> CompileResult:
        """Compile the managed members in casefolded name order, then check what compiled.

        The diagnostics are the catalog's, then each member's render checks or SST-INT902, then
        the checks of the compiled members. A member that any catalog diagnostic names,
        whatever its severity, is not compiled, unless it only warns of a risky name; nor is one
        a render check refuses; one rendered although it is only referenced is dropped.

        Diagnostics:
            Every code `resolve_stage_path` lists, for a routine's `imports:` entry.
            SST-RND040, SST-RND041, SST-RND042: as `tool_render_checks` reports them.
            SST-INT902: rendering a member raised KeyError, TypeError or ValueError.
            SST-VAL612: a referenced member was rendered.
            SST-VAL617: a search service indexes a dbt model that dbt rebuilds every run.
        """
        # A risky name still renders, quoted when it must be, so its warning leaves the member be.
        poisoned = {
            diagnostic.subject
            for diagnostic in self._catalog.diagnostics
            if diagnostic.subject and diagnostic.code not in NAME_WARNING_CODES
        }
        result = compile_checked(
            sorted(self._catalog.managed, key=lambda item: item.name.casefold()),
            key=lambda member: artifact_key("tool", member.name.casefold()),
            render=self._compile,
            diagnostics=self._catalog.diagnostics,
            # Not the default rule: a warning poisons a member too, and only the catalog's
            # diagnostics count, so a rendering failure never skips a later member.
            skip=lambda subject, _: subject in poisoned,
            origin=lambda member: member.origin,
        )
        compiled = tuple(item for item in result.compiled if isinstance(item, CompiledTool))
        leaked = reference_ddl(tuple(item.member for item in compiled))
        kept = tuple(
            item
            for item in result.compiled
            if not isinstance(item, CompiledTool) or item.member.ownership is ToolOwnership.DEFINE
        )
        rebuilt = (
            found
            for item in compiled
            for relation in item.dbt_relations
            if (found := rebuilt_source(item.member, relation[1], self._dbt_materializations.get(relation[0])))
        )
        return CompileResult(kept, DiagnosticBag((*result.diagnostics, *leaked, *rebuilt)))

    def _compile(self, member: ToolMember) -> tuple[CompiledTool | None, tuple[Diagnostic, ...]]:
        placement = self._placements.get(member.name.casefold(), self._default)
        imports = tuple(self._import(member, path) for path in member.imports)
        refused_imports = tuple(path for path in imports if isinstance(path, Diagnostic))
        if refused_imports:
            return None, refused_imports
        effective = replace(_defaults(member, placement), imports=tuple(str(path) for path in imports))
        target = QualifiedName.from_parts(placement.database, placement.schema, member.name)
        source = (
            QualifiedName.parse(self._dbt_relations[member.on_model])
            if member.on_model in self._dbt_relations
            else None
        )
        refused = tool_render_checks(effective, source)
        if refused:
            return None, refused
        return CompiledTool(effective, render_tool(effective, target, source), placement.settings), ()

    def _import(self, member: ToolMember, path: str) -> str | Diagnostic:
        """Resolve one `imports:` entry: a stage SST declares by `tool()`, else an external path as written.

        Diagnostics:
            Every code `resolve_stage_path` lists.
        """
        subject = artifact_key("tool", member.name.casefold())
        return resolve_stage_path(
            path, self._catalog, self._location, member.origin, field="imports", artifact=subject, subject=subject
        )

    def _location(self, member: ToolMember) -> QualifiedName:
        """Return where a defined member publishes: its placement, else the default."""
        placement = self._placements.get(member.name.casefold(), self._default)
        return QualifiedName.from_parts(placement.database, placement.schema, member.name)


def _defaults(member: ToolMember, placement: ToolPlacement) -> ToolMember:
    """Fill what the member's DDL renders and it leaves unset from its placement, by its kind."""
    if member.type == ToolKind.CORTEX_SEARCH_SERVICE.value:
        return replace(
            member,
            warehouse=member.warehouse or placement.warehouse,
            target_lag=member.target_lag or placement.target_lag,
            embedding_model=member.embedding_model or placement.embedding_model,
        )
    if member.type == ToolKind.PROCEDURE.value:
        return replace(
            member,
            warehouse=member.warehouse or placement.warehouse,
            execute_as=member.execute_as or placement.execute_as or "caller",
        )
    return member
