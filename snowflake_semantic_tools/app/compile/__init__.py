"""The compile use case: obtain semantic views, render each to DDL.

Receives its source as a CONSTRUCTOR ARGUMENT and never builds one. That is the
whole reason this ring may not import `adapters/` -- wiring happens in `cli/`, so a
test drives this class with an in-memory source and needs no patching.

Compilation owns rendering only. Planning, applying and connecting remain
separate use cases over the rendered values.

`base` holds what every typed compiler shares, and this module re-exports it; the
other typed compilers live in `tools`, `skills`, `profiles`, `agents`, and `evals`, and
`project` runs them all over a project's inputs.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.base import (
    RENDER_ERRORS,
    ArtifactCompiler,
    CompiledArtifact,
    CompileResult,
    StandaloneArtifact,
    compile_each,
    has_error,
)
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView
from snowflake_semantic_tools.domain.ports.semantic_view_source import SemanticViewSource
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.sql import AuthoredExpression, Sql, ident, join, qname, query_text, sql
from snowflake_semantic_tools.domain.validate.targets import shared_targets

# A dimension as a window's PARTITION BY EXCLUDING names it: `TABLE.DIMENSION`, both unquoted.
_DIMENSION_REFERENCE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*")

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
    ddl: Sql

    @property
    def name(self) -> str:
        """The unqualified view name -- the last part of the FQN."""
        return str(self.view.fqn.rsplit(".", 1)[-1])

    @property
    def canonical_ddl(self) -> str:
        """Canonical bytes hashed by manifests and compared by plans."""
        return "\n".join(line.rstrip() for line in str(self.ddl).splitlines()).strip() + "\n"

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

    def _rendered_artifact(self, ddl: Sql) -> RenderedArtifact:
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
            probe
            for metric in self.view.metrics
            if metric.access_modifier != "private_access" and (probe := _metric_probe(target, metric)) is not None
        )
        probes.extend(
            SmokeProbe(
                key=artifact_key("verified_query", query.name.casefold()),
                kind=ProbeKind.VERIFIED_QUERY,
                sql=sql("SELECT * FROM ({query}) LIMIT 0", query=query_text(query.sql)),
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


def _metric_probe(target: QualifiedName, metric: Metric) -> SmokeProbe | None:
    """Return the probe that queries one metric; None when no query can name it, which smoke reports.

    A window metric's query must request each dimension its PARTITION BY EXCLUDING names, so
    a key there that is not a `TABLE.DIMENSION` reference leaves the metric unqueryable.
    """
    excluded = tuple(key.text.strip() for key in metric.window.partition_excluding) if metric.window else ()
    if any(_DIMENSION_REFERENCE.fullmatch(key) is None for key in excluded):
        return None
    query = _member_probe(target, "METRICS", metric.table, metric.name, metric.expr, limit=True, excluding=excluded)
    return SmokeProbe(key=artifact_key("metric", metric.qualified_name.casefold()), kind=ProbeKind.METRIC, sql=query)


def _view_probe(view: SemanticView, target: QualifiedName) -> Sql:
    if view.dimensions:
        dimension = sorted(view.dimensions, key=lambda item: item.qualified_name)[0]
        return _member_probe(target, "DIMENSIONS", dimension.table, dimension.name, None, limit=False)
    public_metrics = tuple(metric for metric in view.metrics if metric.access_modifier != "private_access")
    if public_metrics:
        metric = sorted(public_metrics, key=lambda item: item.qualified_name)[0]
        return _member_probe(target, "METRICS", metric.table, metric.name, metric.expr, limit=False)
    raise ValueError(f"{view.fqn} has no public dimension or metric for its view smoke probe")


def _member_probe(
    target: QualifiedName,
    clause: str,
    table: str | None,
    name: str,
    expression: AuthoredExpression | None,
    *,
    limit: bool,
    excluding: tuple[str, ...] = (),
) -> Sql:
    """Query one member through SEMANTIC_VIEW, with the dimensions a windowed metric requires.

    `excluding` holds the `TABLE.DIMENSION` keys a structured window excludes, which the query
    requests besides those its expression names.

    Example:
        SELECT SV.ORDER_COUNT FROM SEMANTIC_VIEW(DB.S.V METRICS ORDERS.ORDER_COUNT) AS SV LIMIT 1
    """
    member = ident(Identifier.parse(name))
    qualified = (
        member if table is None else sql("{table}.{member}", table=ident(Identifier.parse(table)), member=member)
    )
    named = _required_dimension_clause(expression.text) if expression is not None else ()
    required = (*named, *(_dimension(*key.split(".")) for key in excluding))
    dimensions = sql(" DIMENSIONS {names}", names=join(", ", required)) if required else sql("")
    parts = {
        "member": member,
        "view": qname(target),
        "qualified": qualified,
        "dimensions": dimensions,
        "limit": sql("1") if limit else sql("0"),
    }
    if clause == "DIMENSIONS":
        return sql(
            "SELECT SV.{member} FROM SEMANTIC_VIEW({view} DIMENSIONS {qualified}{dimensions}) AS SV LIMIT {limit}",
            **parts,
        )
    return sql(
        "SELECT SV.{member} FROM SEMANTIC_VIEW({view} METRICS {qualified}{dimensions}) AS SV LIMIT {limit}", **parts
    )


def _dimension(table: str, name: str) -> Sql:
    return sql("{table}.{name}", table=ident(Identifier.parse(table)), name=ident(Identifier.parse(name)))


def _required_dimension_clause(expression: str) -> tuple[Sql, ...]:
    """The dimensions a window's PARTITION BY EXCLUDING names, which a query of the metric must request."""
    match = re.search(
        r"\bPARTITION\s+BY\s+EXCLUDING\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\s*,\s*[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)*)",
        expression,
        flags=re.IGNORECASE,
    )
    if match is None:
        return ()
    return tuple(
        sql("{table}.{name}", table=ident(Identifier.parse(table)), name=ident(Identifier.parse(name)))
        for table, name in (part.strip().split(".") for part in match.group(1).split(","))
    )


class CompileArtifacts:
    """Combine typed compilers into one stable artifact stream."""

    def __init__(self, compilers: tuple[ArtifactCompiler, ...], positions: dict[str, int]) -> None:
        self._compilers = compilers
        self._positions = positions

    def run_result(self) -> CompileResult:
        """Run each compiler in order, then merge their results as `merge` does."""
        return self.merge((compiler.run_result() for compiler in self._compilers), self._positions)

    @staticmethod
    def merge(results: Iterable[CompileResult], positions: Mapping[str, int]) -> CompileResult:
        """Merge compile results into one stream, sorted by type position and then by key.

        `results` is consumed in order, so a generator runs each compiler only when the one
        before it has finished. Diagnostics keep the results' order, followed by one
        SST-VAL843 per shared target.

        Args:
            positions: Each artifact type's position in the stream, by type name.

        Diagnostics:
            SST-VAL843: two artifacts of different types publish to one Snowflake name.
        """
        compiled: list[CompiledArtifact] = []
        diagnostics = DiagnosticBag()
        for result in results:
            compiled.extend(result.compiled)
            diagnostics = DiagnosticBag((*diagnostics, *result.diagnostics))
        ordered = tuple(sorted(compiled, key=lambda item: (positions[item.artifact_type], item.artifact_key)))
        return CompileResult(ordered, DiagnosticBag((*diagnostics, *_shared_targets(ordered))))


def _shared_targets(compiled: tuple[CompiledArtifact, ...]) -> tuple[Diagnostic, ...]:
    return shared_targets(
        tuple((item.artifact_type, item.artifact_key, item.rendered_artifact.target) for item in compiled)
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
