"""The compile use case: obtain semantic views, render each to DDL.

Receives its source as a CONSTRUCTOR ARGUMENT and never builds one. That is the
whole reason this ring may not import `adapters/` -- wiring happens in `cli/`, so a
test drives this class with an in-memory source and needs no patching.

Compilation owns rendering only. Planning, applying and connecting remain
separate use cases over the rendered values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Protocol

from ..domain.model.artifact_key import artifact_key
from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from ..domain.model.semantic_view import SemanticView
from ..domain.ports.semantic_view_source import SemanticViewSource
from ..domain.render.semantic_view import render


class CompiledArtifact(Protocol):
    """Manifest and lifecycle projection shared by compiled artifact types."""

    @property
    def name(self) -> str: ...

    @property
    def artifact_key(self) -> str: ...

    @property
    def artifact_type(self) -> str: ...

    @property
    def source_files(self) -> tuple[str, ...]: ...

    @property
    def member_keys(self) -> tuple[str, ...]: ...

    @property
    def referenced_models(self) -> tuple[str, ...]: ...

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]: ...

    @property
    def rendered_artifact(self) -> RenderedArtifact: ...

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact: ...


@dataclass(frozen=True)
class CompiledView:
    """One rendered view, paired with the model it came from.

    Both are kept because the caller needs the name for output and the DDL for
    comparison, and re-deriving either from the other would be a second source of
    truth.
    """

    view: SemanticView
    ddl: str

    @property
    def name(self) -> str:
        """The unqualified view name -- the last part of the FQN."""
        return str(self.view.fqn.rsplit(".", 1)[-1])

    @property
    def canonical_ddl(self) -> str:
        """Canonical bytes hashed by manifests and compared by plans."""
        return "\n".join(line.rstrip() for line in self.ddl.splitlines()).strip() + "\n"

    @property
    def byte_length(self) -> int:
        return len(self.canonical_ddl.encode("utf-8"))

    @property
    def fingerprint(self) -> str:
        return sha256(self.canonical_ddl.encode("utf-8")).hexdigest()

    @property
    def artifact_key(self) -> str:
        return artifact_key("semantic_view", self.name.casefold())

    @property
    def artifact_type(self) -> str:
        return "semantic_view"

    @property
    def source_files(self) -> tuple[str, ...]:
        return self.view.source_files or ((self.view.source_path,) if self.view.source_path else ())

    @property
    def member_keys(self) -> tuple[str, ...]:
        keys: list[str] = []
        keys.extend(
            artifact_key("relationship", value.name.casefold())
            for value in sorted(self.view.relationships, key=lambda item: item.name)
        )
        keys.extend(
            artifact_key("fact", value.qualified_name.casefold())
            for value in sorted(self.view.facts, key=lambda item: item.qualified_name)
        )
        keys.extend(
            artifact_key("filter", value.name.casefold())
            for value in sorted(self.view.dimensions, key=lambda item: item.qualified_name)
            if value.kind.value == "filter"
        )
        keys.extend(
            artifact_key("dimension", value.qualified_name.casefold())
            for value in sorted(self.view.dimensions, key=lambda item: item.qualified_name)
            if value.kind.value != "filter"
        )
        keys.extend(
            artifact_key("metric", value.name.casefold())
            for value in sorted(self.view.metrics, key=lambda item: item.qualified_name)
        )
        keys.extend(artifact_key("verified_query", value.name.casefold()) for value in self.view.verified_queries)
        keys.extend(artifact_key("custom_instruction", name.casefold()) for name in self.view.custom_instruction_names)
        return tuple(keys)

    @property
    def referenced_models(self) -> tuple[str, ...]:
        return tuple(self.view.referenced_models)

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        by_model = {table.logical_name.casefold(): table.fqn for table in self.view.tables}
        return tuple((model, by_model.get(model, "")) for model in self.referenced_models)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self._rendered_artifact(self.ddl)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        marked_view = replace(
            self.view,
            ownership_marker=OwnershipMarker(manifest_id, self.fingerprint).text,
        )
        marked_ddl = render(marked_view)
        artifact = self._rendered_artifact(marked_ddl)
        return replace(artifact, fingerprint=self.fingerprint)

    def _rendered_artifact(self, ddl: str) -> RenderedArtifact:
        target = QualifiedName.parse(self.view.fqn)
        view_probe = _view_probe(self.view, target)
        probes = [
            SmokeProbe(
                key=f"{self.artifact_key}:view",
                kind=ProbeKind.VIEW,
                sql=view_probe,
            )
        ]
        probes.extend(
            SmokeProbe(
                key=artifact_key("metric", metric.qualified_name.casefold()),
                kind=ProbeKind.METRIC,
                sql=(
                    f"SELECT SV.{metric.name} FROM "
                    f"SEMANTIC_VIEW({target.sql} METRICS {metric.qualified_name}"
                    f"{_required_dimension_clause(metric.expr)}) AS SV LIMIT 1"
                ),
            )
            for metric in self.view.metrics
            if metric.access_modifier != "private_access"
        )
        probes.extend(
            SmokeProbe(
                key=artifact_key("verified_query", query.name.casefold()),
                kind=ProbeKind.VERIFIED_QUERY,
                sql=f"SELECT * FROM ({query.sql.rstrip(';')}) LIMIT 0",
            )
            for query in self.view.verified_queries
        )
        return RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="semantic_view",
            target=target,
            ddl=ddl,
            smoke=tuple(probes),
            required_relations=tuple(QualifiedName.parse(table.fqn) for table in self.view.tables),
        )


def _view_probe(view: SemanticView, target: QualifiedName) -> str:
    if view.dimensions:
        dimension = sorted(view.dimensions, key=lambda item: item.qualified_name)[0]
        return (
            f"SELECT SV.{dimension.name} FROM "
            f"SEMANTIC_VIEW({target.sql} DIMENSIONS {dimension.qualified_name}) AS SV LIMIT 0"
        )
    public_metrics = tuple(metric for metric in view.metrics if metric.access_modifier != "private_access")
    if public_metrics:
        metric = sorted(public_metrics, key=lambda item: item.qualified_name)[0]
        return (
            f"SELECT SV.{metric.name} FROM "
            f"SEMANTIC_VIEW({target.sql} METRICS {metric.qualified_name}"
            f"{_required_dimension_clause(metric.expr)}) AS SV LIMIT 0"
        )
    raise ValueError(f"{view.fqn} has no public dimension or metric for its view smoke probe")


def _required_dimension_clause(expression: str) -> str:
    match = re.search(
        r"\bPARTITION\s+BY\s+EXCLUDING\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\s*,\s*[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)*)",
        expression,
        flags=re.IGNORECASE,
    )
    return f" DIMENSIONS {match.group(1)}" if match else ""


@dataclass(frozen=True)
class CompileResult:
    """Rendered views plus compiler diagnostics that callers can format once."""

    compiled: tuple[CompiledArtifact, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def success(self) -> bool:
        return not self.diagnostics.has_errors

    @property
    def rendered(self) -> tuple[RenderedArtifact, ...]:
        return tuple(compiled.rendered_artifact for compiled in self.compiled)

    def rendered_for_publish(self, manifest_id: str) -> tuple[RenderedArtifact, ...]:
        return tuple(compiled.rendered_for_publish(manifest_id) for compiled in self.compiled)


class ArtifactCompiler(Protocol):
    def run_result(self) -> CompileResult: ...


class CompileArtifacts:
    """Combine typed compilers into one stable artifact stream."""

    def __init__(self, compilers: tuple[ArtifactCompiler, ...], positions: dict[str, int]) -> None:
        self._compilers = compilers
        self._positions = positions

    def run_result(self) -> CompileResult:
        compiled: list[CompiledArtifact] = []
        diagnostics = DiagnosticBag()
        for compiler in self._compilers:
            result = compiler.run_result()
            compiled.extend(result.compiled)
            diagnostics = DiagnosticBag((*diagnostics, *result.diagnostics))
        ordered = tuple(sorted(compiled, key=lambda item: (self._positions[item.artifact_type], item.artifact_key)))
        return CompileResult(ordered, DiagnosticBag((*diagnostics, *_shared_targets(ordered))))


def _shared_targets(compiled: tuple[CompiledArtifact, ...]) -> tuple[Diagnostic, ...]:
    """Two artifacts of different types that publish to one Snowflake name.

    Whether the object types share a namespace is not documented for every pair,
    so this warns rather than refuses; it always makes a report ambiguous.
    """
    by_target: dict[tuple[str, str, str], list[CompiledArtifact]] = {}
    for item in compiled:
        by_target.setdefault(item.rendered_artifact.target.folded, []).append(item)
    # Profiles share the registry table by design, so only a clash across types counts.
    return tuple(
        D(
            "SST-VAL843",
            subject=items[1].artifact_key,
            a=items[0].artifact_key,
            b=items[1].artifact_key,
            target=items[0].rendered_artifact.target.sql,
        )
        for items in by_target.values()
        if len({item.artifact_type for item in items}) > 1
    )


class CompileSemanticViews:
    """Render every semantic view a source offers."""

    def __init__(self, source: SemanticViewSource) -> None:
        self._source = source

    def run_result(self) -> CompileResult:
        """Compile without turning one rendering invariant into process failure."""
        project = self._source.load_project()
        views = sorted(project.views, key=lambda view: view.fqn)
        compiled: list[CompiledView] = []
        diagnostics = project.diagnostics
        for view in views:
            try:
                compiled.append(CompiledView(view=view, ddl=render(view)))
            except (TypeError, ValueError) as exc:
                diagnostics = DiagnosticBag(
                    (*diagnostics, D("SST-INT902", subject=artifact_key("semantic_view", view.fqn), detail=str(exc)))
                )
        return CompileResult(tuple(item for item in compiled), diagnostics)
