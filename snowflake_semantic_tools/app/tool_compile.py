"""Compile managed tool declarations into generic lifecycle artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..domain.model.artifact_key import artifact_key
from ..domain.model.diagnostic import D, DiagnosticBag
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import OwnershipMarker, RenderedArtifact
from ..domain.model.sql import string_literal
from ..domain.model.tool import ToolCatalog, ToolKind, ToolMember
from ..domain.render.tool import render_tool
from .compile import CompileResult


@dataclass(frozen=True, slots=True)
class CompiledTool:
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
    def member_keys(self) -> tuple[str, ...]:
        return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        return (self.member.on_model,) if self.member.on_model else ()

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        if not self.member.on_model or not self.rendered.required_relations:
            return ()
        return ((self.member.on_model, self.rendered.required_relations[0].sql),)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self.rendered

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        marker = OwnershipMarker(manifest_id, self.rendered.fingerprint).text
        statements = self.rendered.statements
        if self.rendered.object_type == "CORTEX SEARCH SERVICE":
            base = self.rendered.ddl.rstrip()
            description = self.member.description
            comment_value = f"{marker} {description}" if description else marker
            comment = f"COMMENT = {string_literal(comment_value)}"
            if "\n  COMMENT = " in base:
                start = base.index("\n  COMMENT = ")
                end = base.index("\n  AS ", start)
                base = base[:start] + f"\n  {comment}" + base[end:]
            else:
                base = base.replace("\n  AS ", f"\n  {comment}\n  AS ", 1)
            statements = (base,)
        elif self.rendered.object_type in {"PROCEDURE", "FUNCTION", "STAGE"}:
            object_name = self.rendered.target.sql
            if self.rendered.routine_signature:
                object_name += f"({', '.join(self.rendered.routine_signature)})"
            description = self.member.description
            comment_value = f"{marker} {description}" if description else marker
            statements = (
                *statements,
                f"ALTER {self.rendered.object_type} {object_name} SET COMMENT = {string_literal(comment_value)}",
            )
        return replace(
            self.rendered,
            statements=statements,
            expected_marker=OwnershipMarker(manifest_id, self.rendered.fingerprint),
        )


class CompileTools:
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
        diagnostics = self._catalog.diagnostics
        compiled: list[CompiledTool] = []
        poisoned = {diagnostic.subject for diagnostic in diagnostics if diagnostic.subject}
        for member in sorted(self._catalog.managed, key=lambda item: item.name.casefold()):
            if artifact_key("tool", member.name.casefold()) in poisoned:
                continue
            try:
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
                compiled.append(CompiledTool(effective, render_tool(effective, target, source)))
            except (KeyError, TypeError, ValueError) as exc:
                diagnostics = DiagnosticBag(
                    (
                        *diagnostics,
                        D(
                            "SST-INT902",
                            subject=artifact_key("tool", member.name.casefold()),
                            detail=str(exc),
                            origin=member.origin,
                        ),
                    )
                )
        return CompileResult(tuple(compiled), diagnostics)


def _defaults(
    member: ToolMember,
    *,
    warehouse: str | None,
    target_lag: str | None,
    embedding_model: str | None,
    execute_as: str | None,
) -> ToolMember:
    from dataclasses import replace

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
