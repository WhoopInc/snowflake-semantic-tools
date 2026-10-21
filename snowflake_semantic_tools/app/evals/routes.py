"""Read which subagent a CoWork profile's router chose for each question of a run, and score it.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile's eval follows the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

An evaluation run of a profile leaves its spans in SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS:
one root span per record, and one span per routing call, whose
`snow.ai.observability.agent.tool.agent_router.subagent_name` attribute names the subagent the
router chose. One read returns both for the whole run, keyed by the record id
GET_AI_EVALUATION_DATA reports (see docs/design/snowflake-behaviour.md).

The read is filtered by the run's name and by the profile both. A run's spans outlive
`EXECUTE_AI_EVALUATION('DELETE')`, so a name used again would return an earlier run's records
too; each attempt's run name carries its timestamp, so SST never uses one twice, and the
profile filter keeps another object's run of that name out.

A record with a root span and no routing span was declined. A record with no root span has no
trace yet, as a span that lands after the run's terminal status would leave it: the read is
made again, the poll interval apart, at most `ROUTE_READS` times in all and never past the
attempt's deadline. A record still without one fails `routing_accuracy` (SST-SNO035) rather
than reading as a decline. A record the spans name and the results do not is ignored.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.poll import StatusPoll
from snowflake_semantic_tools.app.evals.retrieve import compiled_questions
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.evaluate.routing import expected_subagents, routing_result
from snowflake_semantic_tools.domain.model.eval import EvalResultRow
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakeTransientError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import sql

# The reads of a run's spans that look for a record still without a trace, the first included.
ROUTE_READS = 6
ROOT_SPAN = "record_root"
_ROUTE_COLUMNS = ("RECORD_ID", "SPAN_TYPE", "SUBAGENT")
# Snowflake private preview (PrPr): CoWork profiles. The attribute names are the ones the
# preview's spans carry; Snowflake does not document them.
_ROUTES_QUERY = sql(
    'SELECT record_attributes:"ai.observability.record_id"::string AS RECORD_ID,'
    ' record_attributes:"ai.observability.span_type"::string AS SPAN_TYPE,'
    ' record_attributes:"snow.ai.observability.agent.tool.agent_router.subagent_name"::string AS SUBAGENT'
    " FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS"
    ' WHERE record_attributes:"snow.ai.observability.run.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.database.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.schema.name"::string = %s'
    ' AND record_attributes:"snow.ai.observability.object.name"::string = %s'
    " AND (record_attributes:\"ai.observability.span_type\"::string = 'record_root'"
    ' OR record_attributes:"snow.ai.observability.agent.tool.agent_router.subagent_name" IS NOT NULL)'
)


@dataclass(frozen=True, slots=True)
class RoutingRead:
    """What one read of a run's spans found.

    Attributes:
        records_with_root: The record ids with a root span: those the run has traced.
        routes: Each traced record's routes, by record id: the subagent of each routing call,
            sorted, repeats kept; a record with no routing call is absent.
    """

    records_with_root: frozenset[str]
    routes: Mapping[str, tuple[str, ...]]

    def routed(self, record_id: str) -> tuple[str, ...] | None:
        """Return a record's routes, `()` for a decline; None when the run has not traced it."""
        if record_id not in self.records_with_root:
            return None
        return self.routes.get(record_id, ())


@dataclass(frozen=True, slots=True)
class RoutingScored:
    """A run's rows with their routing results, and what reading the routes reported."""

    rows: tuple[EvalResultRow, ...]
    diagnostics: tuple[Diagnostic, ...] = ()


def read_routes(
    port: ExecutionPort,
    compiled: CompiledEval,
    run_name: str,
    *,
    timeout_seconds: int | None = None,
) -> RoutingRead:
    """Read a run's root and routing spans for the profile it evaluates, in one query.

    The run's name and the profile's database, schema and name are bound, each as Snowflake
    resolves it, so the spans match whatever the profile's spelling.

    Raises:
        SnowflakeTransientError: the read failed in transit, or took longer than `timeout_seconds`.
        SnowflakePortError: the read failed otherwise, as a role that cannot read the events
            table makes it.
        ValueError: a column is missing, or a span names no record.
    """
    target = compiled.agent_target
    result = port.query_in_context(
        SchemaScope(target.database, target.schema),
        _ROUTES_QUERY,
        (run_name, *target.folded),
        timeout_seconds=timeout_seconds,
    )
    names = tuple(name.upper() for name in result.columns)
    missing = tuple(name for name in _ROUTE_COLUMNS if name not in names)
    if missing:
        raise ValueError(f"router spans omitted columns: {', '.join(missing)}")
    roots: set[str] = set()
    routes: dict[str, list[str]] = {}
    for values in result.rows:
        row = dict(zip(names, values, strict=True))
        record_id = _text(row.get("RECORD_ID"))
        if record_id is None:
            raise ValueError("a router span names no record")
        if _text(row.get("SPAN_TYPE")) == ROOT_SPAN:
            roots.add(record_id)
        subagent = _text(row.get("SUBAGENT"))
        if subagent is not None:
            routes.setdefault(record_id, []).append(subagent)
    return RoutingRead(
        frozenset(roots), MappingProxyType({record: tuple(sorted(chosen)) for record, chosen in routes.items()})
    )


def score_routing(
    port: ExecutionPort,
    clock: ClockPort,
    options: EvalRunOptions,
    poll: StatusPoll,
    compiled: CompiledEval,
    run_name: str,
    rows: tuple[EvalResultRow, ...],
) -> RoutingScored:
    """Score each routing metric of a profile's eval on every row of a run, from its spans.

    Each row gains its expected and routed subagents and one result per routing metric, its
    metrics kept in name order. The spans are read again, the poll interval apart, while a
    row's record has no trace, as the module says; a read that fails in transit is retried
    the same way, and only the last one's failure escapes.

    Raises:
        SnowflakePortError: the spans could not be read.
        ValueError: the spans are malformed.

    Diagnostics:
        SST-SNO035: some rows' records still have no trace after the last read; each fails.
    """
    expected = {key: expected_subagents(truth) for key, _, truth in compiled_questions(compiled)}
    wanted = frozenset(row.record_id for row in rows if row.record_id is not None)
    found, reads = _read_until_traced(port, clock, options, poll, compiled, run_name, wanted)
    metrics = compiled.resolved.config.routing_metrics
    scored = []
    for row in rows:
        routed = found.routed(row.record_id) if row.record_id is not None else None
        accepted = expected.get(row.question_key)
        results = tuple(routing_result(row.question_key, metric, accepted, routed) for metric in metrics)
        scored.append(
            replace(
                row,
                metrics=tuple(sorted((*row.metrics, *results), key=lambda item: item.metric_name)),
                expected_subagents=accepted,
                routed_subagents=routed,
            )
        )
    untraced = sum(row.routed_subagents is None for row in scored)
    if not untraced:
        return RoutingScored(tuple(scored))
    diagnostic = D(
        "SST-SNO035",
        artifact=compiled.artifact_key,
        value=run_name,
        count=untraced,
        detail=f"{reads} read{'s' if reads != 1 else ''} of its spans",
        subject=compiled.artifact_key,
    )
    return RoutingScored(tuple(scored), (diagnostic,))


def _read_until_traced(
    port: ExecutionPort,
    clock: ClockPort,
    options: EvalRunOptions,
    poll: StatusPoll,
    compiled: CompiledEval,
    run_name: str,
    wanted: frozenset[str],
) -> tuple[RoutingRead, int]:
    """Read the spans until every wanted record is traced, or the reads or the deadline run out.

    Returns the last read that succeeded, and how many reads were made.

    Raises:
        SnowflakePortError: as `read_routes` raises it; one in transit only when the last read
            failed so and none before it succeeded.
        ValueError: as `read_routes` raises it.
    """
    last: RoutingRead | None = None
    reads = 0
    while True:
        reads += 1
        try:
            last = read_routes(port, compiled, run_name, timeout_seconds=poll.read_timeout_seconds())
        except SnowflakeTransientError:
            if reads >= ROUTE_READS or poll.remaining_ms() <= 0:
                if last is None:
                    raise
                return last, reads
        else:
            if wanted <= last.records_with_root or reads >= ROUTE_READS or poll.remaining_ms() <= 0:
                return last, reads
        wait = min(options.poll_interval_ms, poll.remaining_ms())
        if wait > 0:
            clock.sleep(wait)


def _text(value: object) -> str | None:
    """Return a value as stripped text; None when it is null or blank."""
    text = "" if value is None else str(value).strip()
    return text or None
