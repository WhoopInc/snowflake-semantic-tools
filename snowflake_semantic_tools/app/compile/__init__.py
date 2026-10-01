"""The compile use case: obtain semantic views, render each to DDL.

Receives its source as a CONSTRUCTOR ARGUMENT and never builds one. That is the
whole reason this ring may not import `adapters/` -- wiring happens in `cli/`, so a
test drives this class with an in-memory source and needs no patching.

Compilation owns rendering only. Planning, applying and connecting remain
separate use cases over the rendered values.

`base` holds what every typed compiler shares, and this module re-exports it; the
other typed compilers live in `tools`, `skills`, `profiles`, `agents`, and `evals`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from hashlib import sha256

from ...domain.model.artifact_key import artifact_key
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag
from ...domain.model.identifier import QualifiedName
from ...domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from ...domain.model.semantic_view import SemanticView
from ...domain.ports.semantic_view_source import SemanticViewSource
from ...domain.render.semantic_view import render
from .base import (
    RENDER_ERRORS,
    ArtifactCompiler,
    CompiledArtifact,
    CompileResult,
    StandaloneArtifact,
    compile_each,
    has_error,
)

__all__ = [
    "RENDER_ERRORS",
    "ArtifactCompiler",
    "CompileArtifacts",
    "CompiledArtifact",
    "CompiledView",
    "CompileResult",
    "CompileSemanticViews",
    "StandaloneArtifact",
    "compile_each",
    "has_error",
]


@dataclass(frozen=True)
class CompiledView(CompiledArtifact):
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
        """Return the size of `canonical_ddl` in UTF-8 bytes."""
        return len(self.canonical_ddl.encode("utf-8"))

    @property
    def fingerprint(self) -> str:
        """Return the SHA-256 hex digest of `canonical_ddl`, which the marked DDL keeps as its own."""
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
        """Return the view's member keys, grouped by kind in the order the manifest lists them.

        Relationships, facts, filters, dimensions, and metrics, each sorted by name, then
        verified queries and custom instructions in authored order.
        """
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
        """Render the view again with the ownership marker in its DDL, keeping the unmarked fingerprint."""
        marked_view = replace(
            self.view,
            ownership_marker=OwnershipMarker(manifest_id, self.fingerprint).text,
        )
        marked_ddl = render(marked_view)
        artifact = self._rendered_artifact(marked_ddl)
        return replace(artifact, fingerprint=self.fingerprint)

    def _rendered_artifact(self, ddl: str) -> RenderedArtifact:
        """Wrap `ddl` as this view's artifact, with a smoke probe for the view and each public member.

        The probes are the view itself, then each metric that is not private, then each
        verified query, in authored order.

        Raises:
            ValueError: the FQN does not parse, or the view has no public dimension or metric
                for its view probe.
        """
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


class CompileArtifacts:
    """Combine typed compilers into one stable artifact stream."""

    def __init__(self, compilers: tuple[ArtifactCompiler, ...], positions: dict[str, int]) -> None:
        self._compilers = compilers
        self._positions = positions

    def run_result(self) -> CompileResult:
        """Run each compiler in order, then sort the artifacts by type position and key.

        Diagnostics keep the compilers' order, followed by one SST-VAL843 per shared target.

        Diagnostics:
            SST-VAL843: two artifacts of different types publish to one Snowflake name.
        """
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
        """Compile without turning one rendering invariant into process failure.

        Every view renders, in FQN order, whatever the source reported about it; only
        TypeError and ValueError are caught.

        Diagnostics:
            SST-INT902: rendering a view raised TypeError or ValueError.
        """
        project = self._source.load_project()
        return compile_each(
            sorted(project.views, key=lambda view: view.fqn),
            key=lambda view: artifact_key("semantic_view", view.fqn),
            render=_compiled_view,
            diagnostics=project.diagnostics,
            skip=None,
            errors=(TypeError, ValueError),
        )


def _compiled_view(view: SemanticView) -> CompiledView:
    return CompiledView(view=view, ddl=render(view))
