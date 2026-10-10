"""Validation orchestration over compiled artifacts.

The offline checks live in `domain.validate`. The connected checks stay here because they
run SQL through the `ExecutionPort`: the syntax check (SST-VAL418) compiles each expression
over columns typed as the target describes its tables (`app.explain_projection`), the
verified-query row count (SST-VAL415) runs each query that compiles, and the spot checks
(SST-VAL212, SST-VAL218) read the joined tables. SST-VAL020 reports each one skipped. The
live-object checks of agents and tools are in `app.compile.agents.observe`. Each distinct
statement the views' checks run is one unit a `Fanout` may run on a session of its own, and
runs once however many views or members ask it; the views' findings are then read from the
answers in compile order, a member's finding once however many views hold it.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.agents.observe import ObserveLiveObjects
from snowflake_semantic_tools.app.explain_projection import (
    ObservedTypes,
    column_types,
    describe_columns,
    explain_expression,
)
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.verify_schema import verify_columns
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.policy import SeverityPolicy, apply_policy
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult, RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import Relationship, SemanticView, Table, VerifiedQuery
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import (
    Sql,
    ident,
    join,
    qname,
    query_text,
    sql,
)
from snowflake_semantic_tools.domain.validate.shared import reference_cycle


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """What validation found: the compile's rendered artifacts and every diagnostic, strictness applied.

    Attributes:
        rendered: Every rendered artifact of the compile, in compile order, whatever was found.
        diagnostics: The compile's diagnostics, then validation's own, after strict promotion.
        promoted: How many warnings strict mode made errors; 0 without it.
        rules_run: How many of the registry's validation rules ran: every one, less the
            connected rules skipped.
        artifacts_checked: How many compiled artifacts were validated.
    """

    rendered: tuple[RenderedArtifact, ...]
    diagnostics: DiagnosticBag
    promoted: int = 0
    rules_run: int = 0
    artifacts_checked: int = 0

    @property
    def success(self) -> bool:
        """Report whether validation passed: no diagnostic is an error once strict promotion applied."""
        return not self.diagnostics.has_errors


class ValidateArtifacts:
    """Validate a compile result offline or, with a port, against Snowflake.

    The connected checks only EXPLAIN, count a verified query's rows, and read what exists,
    so they never write.

    Args:
        catalog: Reads the live objects the compiled agents and tools publish over or call;
            None skips those checks.
        target: The `profiles.yml` target being validated, which a missing object names.
        clock: Times each verified query's count; None reports it as taking 0ms.
        readers: Sessions to run the checks' statements on concurrently, opened from `port`;
            None runs them on `port`, one at a time. Either way the findings are the same.
    """

    def __init__(
        self,
        port: ExecutionPort | None = None,
        *,
        catalog: CatalogPort | None = None,
        target: str = "",
        clock: ClockPort | None = None,
        readers: Fanout[ExecutionPort] | None = None,
    ) -> None:
        self._port = port
        self._catalog = catalog
        self._target = target
        self._clock = clock
        self._readers = readers

    def run(
        self,
        compiled: CompileResult,
        *,
        strict: bool,
        connected: bool,
        verify_schema: bool = False,
        overrides: Mapping[str, Severity] | None = None,
        reaches: Callable[[str | None], bool] | None = None,
    ) -> ValidationResult:
        """Validate a compile result, asking Snowflake to check each semantic view when connected.

        Offline, the compiled artifacts' dependencies are checked for a cycle. Connected, each
        table the compiled semantic views read is described once, and each view's metrics, its
        dimensions (time dimensions and filters included, facts not), and its verified queries
        are EXPLAINed: an expression over a projection with no rows, in which each qualified
        column reference is a NULL of the column's described type (else its declared one, else
        untyped) and variables and metric names are NULL; a verified query as written. A
        verified query that compiles is then run under a row count. Then each equality
        relationship's target is read for a repeated join key, and each distinct range for
        overlapping ranges. A statement two views or members ask, such as a shared verified
        query's, runs once, and both read its answer. A compile or row-count finding repeated
        by every view that holds the member is reported once, naming those views. With a
        catalog, the compiled agents
        and tools are checked against their live objects. The project's severity overrides
        then apply, and strict mode promotes every warning to an error.

        Args:
            strict: Promote every warning, the compile's included, to an error; with `reaches`,
                every warning it reaches.
            connected: Run the Snowflake checks; False skips them even with a port.
            verify_schema: With a catalog, also look each column a view reads up in the
                warehouse, as `verify_columns` does.
            overrides: The severity each code the project overrides reports at; None for none.
            reaches: Whether strict mode reaches a diagnostic about a subject, as the run's
                selection decides; None reaches every one.

        Diagnostics:
            SST-VAL010: the compiled artifacts depend on one another in a cycle.
            SST-VAL020: once per connected rule, when the Snowflake checks were skipped:
                disabled, or no port was given.
            SST-VAL418: Snowflake would not compile an expression or a verified query.
            SST-VAL415: a verified query ran and returned no rows.
            SST-VAL212: a relationship's target holds more than one row for a join key.
            SST-VAL218: a distinct range's rows overlap.
            Those of `ObserveLiveObjects`, with a catalog.
            Those of `verify_columns`, with `verify_schema` and a catalog.
        """
        found = [*compiled.diagnostics, *_cycle_diagnostics(compiled.rendered)]
        skipped = 0
        if not connected or self._port is None:
            detail = (
                "Snowflake syntax checking was disabled" if not connected else "no Snowflake connection was provided"
            )
            found.extend(D("SST-VAL020", rule_id=rule, detail=detail) for rule in CONNECTED_RULES)
            skipped = len(CONNECTED_RULES)
        else:
            views = tuple(item for item in compiled.compiled if isinstance(item, CompiledView))
            found.extend(_once_per_member(self._connected_checks(self._readers or Fanout(self._port), views)))
            if self._catalog is not None:
                found.extend(ObserveLiveObjects(self._catalog, target=self._target).run(compiled))
        if verify_schema and self._catalog is not None:
            found.extend(verify_columns(self._catalog, compiled))
        policy = SeverityPolicy(MappingProxyType(dict(overrides or {})), strict, reaches)
        resolved, promoted = apply_policy(DiagnosticBag(found), policy)
        return ValidationResult(
            compiled.rendered,
            resolved,
            promoted,
            rules_run=len(VALIDATION_RULES) - skipped,
            artifacts_checked=len(compiled.compiled),
        )

    def _connected_checks(self, readers: Fanout[ExecutionPort], views: tuple[CompiledView, ...]) -> list[Diagnostic]:
        """Run the views' connected checks, each distinct statement once, and report in view order.

        Three rounds of reads, each on `readers`: every table the views read is described;
        then every EXPLAIN is run; then each verified query that compiled is counted, and
        each relationship spot-checked. Each view's findings are read from the answers.
        """
        relations = tuple(sorted({table.fqn for item in views for table in item.view.tables}))
        observed = MappingProxyType(dict(zip(relations, readers.map(describe_columns, relations), strict=True)))
        compiles = {item.artifact_key: _compile_checks(item, observed) for item in views}
        explained = self._answers(readers, (check.read for checks in compiles.values() for check in checks))
        diagnostics = {
            item.artifact_key: _compile_findings(item, compiles[item.artifact_key], explained) for item in views
        }
        counted = {item.artifact_key: _counted_queries(item, diagnostics[item.artifact_key]) for item in views}
        spots = {item.artifact_key: _spot_checks(item) for item in views}
        answered = self._answers(
            readers,
            (
                *(_count_read(query) for queries in counted.values() for query in queries),
                *(spot[2] for checks in spots.values() for spot in checks),
            ),
        )
        found: list[Diagnostic] = []
        for item in views:
            found.extend(diagnostics[item.artifact_key])
            for query in counted[item.artifact_key]:
                empty = _row_count(item, query, answered[_count_read(query)])
                if empty is not None:
                    found.append(empty)
            found.extend(_spot_findings(item, spots[item.artifact_key], answered))
        return found

    def _answers(self, readers: Fanout[ExecutionPort], reads: Iterable[_Read]) -> Mapping[_Read, _Answer]:
        """Run each distinct read once on `readers`, returning every answer by its read."""
        distinct = tuple(dict.fromkeys(reads))
        return MappingProxyType(dict(zip(distinct, readers.map(self._answer, distinct), strict=True)))

    def _answer(self, port: ExecutionPort, read: _Read) -> _Answer:
        """Run one read on `port`, in its scope when it has one, keeping a refusal as its answer."""
        started = self._clock.monotonic_ms() if self._clock is not None else 0
        try:
            result = (
                port.query(read.statement) if read.scope is None else port.query_in_context(read.scope, read.statement)
            )
        except SnowflakePortError as exc:
            return _Answer(None, exc, 0)
        elapsed = (self._clock.monotonic_ms() - started) if self._clock is not None else 0
        return _Answer(result, None, elapsed)


@dataclass(frozen=True, slots=True)
class _Read:
    """One statement a connected check runs: on the session, or in `scope` when it names one.

    Two reads are one when their statement text and scope are, so it runs once.
    """

    statement: Sql
    scope: SchemaScope | None = None


@dataclass(frozen=True, slots=True)
class _Answer:
    """What Snowflake answered a read: its result, else the refusal, and how long it took."""

    result: QueryResult | None
    error: SnowflakePortError | None
    elapsed_ms: int


@dataclass(frozen=True, slots=True)
class _CompileCheck:
    """One member a view EXPLAINs (SST-VAL418): its kind, name and origin, and the read."""

    kind: str
    name: str
    origin: Origin | None
    read: _Read


def _compile_checks(compiled_view: CompiledView, observed: ObservedTypes) -> list[_CompileCheck]:
    """Return the EXPLAIN of each metric, dimension and verified query of one view, in that order."""
    view = compiled_view.view
    types = column_types(view, observed)
    checks = [
        _CompileCheck(
            "metric",
            metric.qualified_name,
            _origin(view, "metric", metric.name),
            _Read(explain_expression(metric.expr, view, types)),
        )
        for metric in view.metrics
    ]
    checks.extend(
        _CompileCheck(
            "filter" if column.kind.value == "filter" else "dimension",
            column.qualified_name,
            _origin(view, "filter", column.name)
            if column.kind.value == "filter"
            else _origin(view, "dimension", column.qualified_name),
            _Read(explain_expression(column.expr, view, types)),
        )
        for column in view.dimensions
    )
    # A verified query is authored SQL that may name objects relative to the view's schema,
    # so it alone is explained in that scope.
    view_scope = SchemaScope.from_qualified_name(QualifiedName.parse(view.fqn))
    checks.extend(
        _CompileCheck(
            "verified_query",
            query.name,
            _origin(view, "verified_query", query.name),
            _Read(sql("EXPLAIN {query}", query=query_text(query.sql)), view_scope),
        )
        for query in view.verified_queries
    )
    return checks


def _compile_findings(
    compiled_view: CompiledView, checks: list[_CompileCheck], answers: Mapping[_Read, _Answer]
) -> list[Diagnostic]:
    """Report each of one view's EXPLAINs that Snowflake refused (SST-VAL418), in check order."""
    return [
        D(
            "SST-VAL418",
            type=check.kind,
            name=check.name,
            detail=str(error),
            view=compiled_view.name.casefold(),
            origin=check.origin,
            subject=compiled_view.artifact_key,
        )
        for check in checks
        if (error := answers[check.read].error) is not None
    ]


def _counted_queries(compiled_view: CompiledView, compile_findings: list[Diagnostic]) -> list[VerifiedQuery]:
    """Return the view's verified queries that compiled, which are then counted, in order."""
    failed = {item.context["name"] for item in compile_findings if item.context["type"] == "verified_query"}
    return [query for query in compiled_view.view.verified_queries if query.name not in failed]


def _count_read(query: VerifiedQuery) -> _Read:
    """Return the row count of a verified query, which runs on the session, whatever view holds it."""
    return _Read(sql("SELECT COUNT(*) AS ROW_COUNT FROM ({query}) AS SST_VQ", query=query_text(query.sql)))


def _row_count(compiled_view: CompiledView, query: VerifiedQuery, answer: _Answer) -> Diagnostic | None:
    """Report a verified query whose row count is zero (SST-VAL415).

    A query whose count fails is left to its EXPLAIN, which already passed; the failure is
    not reported a second time.
    """
    rows = answer.result.rows if answer.result is not None else ()
    count = rows[0][0] if rows and rows[0] else None
    if not isinstance(count, (int, float)) or count:
        return None
    return D(
        "SST-VAL415",
        member=query.name,
        row_count=int(count),
        elapsed_ms=answer.elapsed_ms,
        view=compiled_view.name.casefold(),
        origin=_origin(compiled_view.view, "verified_query", query.name),
        subject=compiled_view.artifact_key,
    )


def _spot_checks(compiled_view: CompiledView) -> list[tuple[Relationship, Table, _Read]]:
    """Return each spot check of one view, by relationship name: the relationship, its target, the read.

    An equality join's target is read for repeated keys, and a range join's distinct range
    for overlaps; an ASOF join, or a range join whose target declares no distinct range, is
    not checked.
    """
    view = compiled_view.view
    tables = {table.logical_name: table for table in view.tables}
    checks: list[tuple[Relationship, Table, _Read]] = []
    for relationship in sorted(view.relationships, key=lambda item: item.name):
        target = tables.get(relationship.to_table)
        if target is None or not view.scope.admits_relationship(relationship.name):
            continue
        if relationship.range_bounds is not None:
            if target.distinct_range is not None:
                checks.append((relationship, target, _Read(_overlap_query(target))))
        elif relationship.asof_index is None:
            checks.append((relationship, target, _Read(_duplicate_key_query(relationship, target))))
    return checks


def _spot_findings(
    compiled_view: CompiledView,
    checks: list[tuple[Relationship, Table, _Read]],
    answers: Mapping[_Read, _Answer],
) -> list[Diagnostic]:
    """Report what one view's spot checks read.

    A read that failed is not reported here: the EXPLAIN checks report a relation they
    cannot reach.
    """
    found = (
        _spot_finding(relationship, target, result.rows)
        for relationship, target, read in checks
        if (result := answers[read].result) is not None
    )
    return [replace(item, subject=compiled_view.artifact_key) for item in found if item is not None]


def _spot_finding(relationship: Relationship, target: Table, rows: tuple[tuple[object, ...], ...]) -> Diagnostic | None:
    """Read one relationship's spot check: duplicate keys, or overlapping ranges for a range join."""
    name = relationship.name.casefold()
    if relationship.range_bounds is not None:
        assert target.distinct_range is not None
        if not rows:
            return None
        start, end = target.distinct_range
        example = f"[{rows[0][0]}, {rows[0][1]}) and [{rows[0][2]}, {rows[0][3]})"
        return D("SST-VAL218", relationship=name, name=target.logical_name, a=start, b=end, value=example)
    repeated = int(str(rows[0][0])) if rows and rows[0] and rows[0][0] is not None else 0
    if repeated <= 0:
        return None
    return D(
        "SST-VAL212",
        relationship=name,
        found="many-to-one",
        value=f"{repeated} rows of '{target.logical_name}' repeat a join key",
    )


# The connected rules, which offline validation skips and reports as skipped.
CONNECTED_RULES = ("SST-VAL418", "SST-VAL415", "SST-VAL212", "SST-VAL218")
# Every validation rule in the registry: one per VAL code, the connected rules among them.
VALIDATION_RULES = tuple(sorted(code for code, spec in ERROR_REGISTRY.items() if spec.subsystem == "VAL"))


def _cycle_diagnostics(rendered: tuple[RenderedArtifact, ...]) -> tuple[Diagnostic, ...]:
    """Report a cycle among the compiled artifacts' dependencies, which no publish order satisfies."""
    types = {artifact.key: artifact.artifact_type for artifact in rendered}
    cycle = reference_cycle(
        {artifact.key: tuple(key for key in artifact.depends_on if key in types) for artifact in rendered}
    )
    if not cycle:
        return ()
    return (D("SST-VAL010", subject=cycle[0], type=types[cycle[0]], cycle=" -> ".join(cycle)),)


def _duplicate_key_query(relationship: Relationship, target: Table) -> Sql:
    """Count the target rows beyond one per join key: zero when the join is many-to-one."""
    columns = join(", ", (ident(Identifier.parse(column)) for column in relationship.to_columns))
    return sql(
        "SELECT COUNT(*) - COUNT(DISTINCT {columns}) FROM {table}",
        columns=columns,
        table=qname(QualifiedName.parse(target.fqn)),
    )


def _overlap_query(target: Table) -> Sql:
    """Find one pair of distinct ranges of the target that overlap; no row when none do."""
    assert target.distinct_range is not None
    start, end = (ident(Identifier.parse(name)) for name in target.distinct_range)
    return sql(
        "SELECT A.{start}, A.{end}, B.{start}, B.{end} FROM {table} AS A JOIN {table} AS B"
        " ON A.{start} < B.{end} AND B.{start} < A.{end}"
        " AND (A.{start} < B.{start} OR (A.{start} = B.{start} AND A.{end} < B.{end})) LIMIT 1",
        start=start,
        end=end,
        table=qname(QualifiedName.parse(target.fqn)),
    )


def _origin(view: SemanticView, kind: str, name: str) -> Origin | None:
    """Return where the view's member of `kind` named `name` is declared; None when unrecorded."""
    return view.member_origins.get(artifact_key(kind, name.casefold()))


# The connected findings about one member, which each view holding the member repeats.
_PER_MEMBER = frozenset({"SST-VAL418", "SST-VAL415"})
# A Snowflake query id, which differs between two runs of one statement.
_QUERY_ID = re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.IGNORECASE)


def _once_per_member(found: Iterable[Diagnostic]) -> list[Diagnostic]:
    """Report each SST-VAL418 and SST-VAL415 once, naming every view that repeats it.

    Two findings repeat each other when they agree on their code, origin, and context, but
    for the view, the query id in a detail, and a row count's elapsed time. The first, in
    compile order, keeps its place and its subject, its `view` lists every view in order,
    and a VAL418 detail ends by naming them. Any other finding passes through in place.
    """
    groups: dict[tuple[object, ...], list[Diagnostic]] = {}
    order: list[tuple[object, ...] | Diagnostic] = []
    for diagnostic in found:
        if diagnostic.code not in _PER_MEMBER:
            order.append(diagnostic)
            continue
        key = _repeat_key(diagnostic)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(diagnostic)
    return [item if isinstance(item, Diagnostic) else _naming_views(groups[item]) for item in order]


def _repeat_key(diagnostic: Diagnostic) -> tuple[object, ...]:
    """Return what two repeats of one member's finding share: all but the view and the timing."""
    context = tuple(
        (name, _QUERY_ID.sub("", str(value)))
        for name, value in sorted(diagnostic.context.items())
        if name not in ("view", "elapsed_ms")
    )
    return (diagnostic.code, diagnostic.origin, context)


def _naming_views(repeats: list[Diagnostic]) -> Diagnostic:
    """Return the first of a member's repeated findings, naming every view it was found in."""
    first = repeats[0]
    views = ", ".join(dict.fromkeys(str(item.context["view"]) for item in repeats))
    context = {**first.context, "view": views}
    if first.code == "SST-VAL418":
        context["detail"] = f"{first.context['detail']} (in {views})"
    return replace(first, context=MappingProxyType(context))
