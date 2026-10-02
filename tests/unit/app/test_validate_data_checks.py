"""Connected validation's data checks, a saved plan's stale observation, and the pure-phase guard."""

from __future__ import annotations

import os
from collections.abc import Mapping

import pytest

from snowflake_semantic_tools.app.apply.observation import stale_observation
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.purity import ImpureCall, pure_phase
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import (
    Metric,
    Relationship,
    SemanticView,
    Table,
    VerifiedQuery,
    ViewScope,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import SavedPlan
from tests.helpers.app_ports import FixedClock, InMemorySnowflake
from tests.helpers.artifact_builders import target
from tests.helpers.sql_values import authored, authored_query


class Scripted(InMemorySnowflake):
    """A port that answers each read whose text holds a marker with that marker's rows, or fails it."""

    def __init__(self, answers: Mapping[str, tuple[tuple[object, ...], ...] | Exception]) -> None:
        super().__init__()
        self._answers = answers

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        for marker, rows in self._answers.items():
            if marker in text:
                if isinstance(rows, Exception):
                    raise rows
                return QueryResult(("A",), rows)
        return super().query(sql, params)


ORDERS = Table("ORDERS", "DB.S.ORDERS")
CUSTOMERS = Table("CUSTOMERS", "DB.S.CUSTOMERS", primary_key=("CUSTOMER_ID",))
RANGES = Table("RATES", "DB.S.RATES", distinct_range=("STARTS_AT", "ENDS_AT"))
EQUALITY = Relationship("TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",))
ASOF = Relationship("AS_OF", "ORDERS", ("PLACED_AT",), "CUSTOMERS", ("JOINED_AT",), asof_index=0)
RANGE = Relationship(
    "IN_RANGE", "ORDERS", ("PLACED_AT",), "RATES", ("STARTS_AT",), range_bounds=("STARTS_AT", "ENDS_AT")
)
UNBOUNDED = Relationship("NO_RANGE", "ORDERS", ("PLACED_AT",), "CUSTOMERS", ("JOINED_AT",), range_bounds=("A", "B"))
STRAY = Relationship("STRAY", "ORDERS", ("X",), "NOWHERE", ("X",))


def _codes(view: SemanticView, answers: Mapping[str, tuple[tuple[object, ...], ...] | Exception]) -> list[str]:
    compiled = CompileResult((CompiledView(view, render(view)),))
    result = ValidateArtifacts(Scripted(answers), clock=FixedClock()).run(compiled, strict=False, connected=True)
    return [item.code for item in result.diagnostics if item.code in ("SST-VAL212", "SST-VAL218", "SST-VAL415")]


def _view(*relationships: Relationship, **fields: object) -> SemanticView:
    return SemanticView(
        "DB.S.SALES",
        (ORDERS, CUSTOMERS, RANGES),
        metrics=(Metric("ROWS", authored("COUNT(1)"), "ORDERS"),),
        relationships=relationships,
        **fields,  # type: ignore[arg-type]
    )


def test_each_join_target_is_read_for_repeated_keys_and_each_range_for_overlaps() -> None:
    view = _view(EQUALITY, ASOF, RANGE, UNBOUNDED, STRAY)
    overlapping = {"COUNT(DISTINCT": ((2,),), "LIMIT 1": (("1", "5", "3", "9"),)}
    assert _codes(view, overlapping) == ["SST-VAL218", "SST-VAL212"]
    assert _codes(view, {"COUNT(DISTINCT": ((0,),), "LIMIT 1": ()}) == []
    assert _codes(view, {"COUNT(DISTINCT": ((None,),)}) == []
    # A read that fails reports nothing here; the EXPLAIN checks report what they cannot reach.
    assert _codes(view, {"COUNT(DISTINCT": SnowflakePortError("no access")}) == []


def test_a_relationship_the_view_scope_leaves_out_is_not_read() -> None:
    scoped = _view(EQUALITY, scope=ViewScope(exclude_relationships=("TO_CUSTOMERS",)))
    assert _codes(scoped, {"COUNT(DISTINCT": ((2,),)}) == []


def test_a_verified_query_that_compiles_and_returns_no_rows_is_reported() -> None:
    query = VerifiedQuery("RECENT", "Which orders are recent?", authored_query("SELECT 1"))
    view = _view(verified_queries=(query,))
    assert _codes(view, {"ROW_COUNT": ((0,),)}) == ["SST-VAL415"]
    assert _codes(view, {"ROW_COUNT": ((4,),)}) == []
    assert _codes(view, {"ROW_COUNT": ()}) == []
    assert _codes(view, {"ROW_COUNT": SnowflakePortError("timeout")}) == []
    # One that does not compile is SST-VAL418's, and is not counted.
    assert _codes(view, {"EXPLAIN SELECT 1": SnowflakePortError("bad"), "ROW_COUNT": ((0,),)}) == []
    unclocked = CompileResult((CompiledView(view, render(view)),))
    result = ValidateArtifacts(Scripted({"ROW_COUNT": ((0,),)})).run(unclocked, strict=False, connected=True)
    [empty] = [item for item in result.diagnostics if item.code == "SST-VAL415"]
    assert empty.context["elapsed_ms"] == 0


def _saved(observation_at: str) -> SavedPlan:
    return SavedPlan(1, "p" * 64, "m" * 64, target(), observation_at, "o" * 64, ())


def test_a_saved_observation_past_its_time_to_live_is_reported_as_ignored() -> None:
    fresh = _saved("2026-01-01T12:00:00+00:00")
    found = stale_observation(_saved("2026-01-01T09:55:00+00:00"), fresh, source="plan.json")
    assert found is not None and (found.code, found.context["detail"]) == ("SST-MAN030", "2h 05m")
    assert stale_observation(_saved("2026-01-01T11:30:00+00:00"), fresh, source="plan.json") is None
    assert stale_observation(_saved("not a time"), fresh, source="plan.json") is None
    assert stale_observation(_saved("2026-01-01T09:00:00"), fresh, source="plan.json") is None


def test_a_pure_phase_refuses_io_and_allows_it_outside(tmp_path: object) -> None:
    path = os.path.join(str(tmp_path), "x.txt")
    with pytest.raises(ImpureCall) as raised, pure_phase("rendering view"):
        open(path, "w").close()  # noqa: SIM115
    assert (raised.value.phase, raised.value.event) == ("rendering view", "open")
    open(path, "w").close()  # noqa: SIM115
