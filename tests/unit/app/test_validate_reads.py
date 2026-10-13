"""Connected validation runs each distinct statement once, and reports the same on any session count.

The warehouse here describes tables, refuses one metric's EXPLAIN as Snowflake refuses a SUM
over a timestamp, and answers a verified query's count and a join's key check; on leased
sessions it answers after a random pause, so concurrent statements finish out of order.
"""

from __future__ import annotations

import random
import threading
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    Table,
    VerifiedQuery,
)
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.sql_values import authored, authored_query, statement
from tests.helpers.val_codes import TypedWarehouse

TYPE_ERROR = (
    "002139 (42000): {qid}: SQL compilation error: Invalid argument types for function 'SUM': (TIMESTAMP_NTZ(9))"
)
ORDERS = (("ORDER_ID", "VARCHAR(16777216)"), ("ORDERED_AT", "TIMESTAMP_NTZ(9)"), ("CUSTOMER_ID", "NUMBER(38,0)"))
CUSTOMERS = (("CUSTOMER_ID", "NUMBER(38,0)"),)
TODAY = VerifiedQuery("orders_today", "How many orders today?", authored_query("SELECT COUNT(*) FROM DB.S.ORDERS"))
JOINED = Relationship("TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",))


class Warehouse(TypedWarehouse):
    """Answers as `TypedWarehouse` does, one statement at a time, logging each with its scope.

    A row count answers zero rows counted, and a key check two repeated keys.
    """

    def __init__(self) -> None:
        super().__init__({"DB.S.ORDERS": ORDERS, "DB.S.CUSTOMERS": CUSTOMERS}, {'SUM("ORDERS.ORDERED_AT")': TYPE_ERROR})
        self.ran: list[tuple[str | None, str]] = []
        self._guard = threading.Lock()

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        with self._guard:
            self.ran.append((getattr(self._in_scope, "scope", None), str(sql)))
            if "COUNT(DISTINCT" in str(sql):
                return QueryResult(("REPEATED",), ((2,),))
            if "AS ROW_COUNT" in str(sql):
                return QueryResult(("ROW_COUNT",), ((0,),))
            return super().query(sql, params)


class Session:
    """A leased session over the shared warehouse, which answers after a random pause."""

    def __init__(self, warehouse: Warehouse, threads: set[str]) -> None:
        self._warehouse = warehouse
        self._threads = threads

    def _pause(self) -> None:
        self._threads.add(threading.current_thread().name)
        time.sleep(random.uniform(0, 0.003))

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        self._pause()
        return self._warehouse.query(sql, params)

    def query_in_context(
        self, scope: SchemaScope, sql: Sql, params: object = None, *, timeout_seconds: int | None = None
    ) -> QueryResult:
        self._pause()
        return self._warehouse.query_in_context(scope, sql, params, timeout_seconds=timeout_seconds)


class Pool:
    """Lends a fresh session over one warehouse per lease, recording the threads that read."""

    def __init__(self, warehouse: Warehouse) -> None:
        self._warehouse = warehouse
        self.threads: set[str] = set()

    @contextmanager
    def lease(self) -> Iterator[ExecutionPort]:
        yield Session(self._warehouse, self.threads)  # type: ignore[misc]

    def halt(self, reason: str) -> None:
        del reason


def _view(fqn: str, *, metric: bool = True) -> SemanticView:
    """A view over ORDERS and CUSTOMERS holding `TODAY`; with `metric`, a SUM over a timestamp too."""
    return SemanticView(
        fqn,
        (Table("ORDERS", "DB.S.ORDERS"), Table("CUSTOMERS", "DB.S.CUSTOMERS", primary_key=("CUSTOMER_ID",))),
        columns=(Column("ORDERS", "ORDERED_AT", ColumnKind.DIMENSION, authored("ORDERS.ORDERED_AT")),),
        metrics=(Metric("LATEST", authored("SUM(ORDERS.ORDERED_AT)"), "ORDERS"),) if metric else (),
        relationships=(JOINED,),
        verified_queries=(TODAY,),
        member_origins=MappingProxyType({"metric:latest": Origin("semantic_models/metrics.yml", 4, 5)}),
    )


# Two views in one schema share every statement; the third, in another, explains the
# verified query in its own scope.
VIEWS = (_view("DB.S.SALES"), _view("DB.S.MENU"), _view("DB.T.OTHER", metric=False))


def _diagnostics(warehouse: Warehouse, readers: Fanout[ExecutionPort] | None) -> list[Diagnostic]:
    compiled = CompileResult(tuple(CompiledView(view, statement(f"CREATE SEMANTIC VIEW {view.fqn}")) for view in VIEWS))
    return list(ValidateArtifacts(warehouse, readers=readers).run(compiled, strict=False, connected=True).diagnostics)


def _validated(warehouse: Warehouse, readers: Fanout[ExecutionPort] | None) -> list[tuple[object, ...]]:
    return [_comparable(item) for item in _diagnostics(warehouse, readers)]


def _comparable(diagnostic: Diagnostic) -> tuple[object, ...]:
    return (diagnostic.code, diagnostic.message, diagnostic.subject, diagnostic.origin, dict(diagnostic.context))


def test_validation_reports_the_same_one_session_at_a_time_and_on_several_at_once() -> None:
    serial = _validated(Warehouse(), None)
    assert [item[0] for item in serial] == ["SST-VAL418", "SST-VAL415", "SST-VAL212", "SST-VAL212", "SST-VAL212"]
    for _attempt in range(5):
        warehouse = Warehouse()
        pool = Pool(warehouse)
        assert _validated(warehouse, Fanout(warehouse, pool, 4)) == serial
        assert any(name.startswith("sst-worker") for name in pool.threads)
        # Every statement ran once, on the leased sessions alone.
        assert max(Counter(warehouse.ran).values()) == 1


def test_each_distinct_statement_runs_once_however_many_views_ask_it() -> None:
    warehouse = Warehouse()
    found = _diagnostics(warehouse, None)
    assert max(Counter(warehouse.ran).values()) == 1
    texts = [text for _scope, text in warehouse.ran]
    # The verified query reads the same relations in every view, so it is counted once, and
    # explained once in each schema that holds it.
    bound = (
        "WITH ORDERS AS (SELECT * FROM DB.S.ORDERS), CUSTOMERS AS (SELECT * FROM DB.S.CUSTOMERS) "
        "SELECT * FROM (SELECT COUNT(*) FROM DB.S.ORDERS) AS SST_VQ"
    )
    assert texts.count(f"SELECT COUNT(*) AS ROW_COUNT FROM ({bound}) AS SST_COUNT") == 1
    assert [scope for scope, text in warehouse.ran if text == f"EXPLAIN {bound}"] == [
        "DB.S",
        "DB.T",
    ]
    assert len([text for text in texts if 'SUM("ORDERS.ORDERED_AT")' in text]) == 1
    # Each finding still names every view that holds the member.
    refused, empty = found[0], found[1]
    assert refused.context["view"] == "sales, menu"
    assert refused.context["detail"] == (
        TYPE_ERROR.replace("{qid}", "01bc0000-0000-0000-0000-000000000001") + " (in sales, menu)"
    )
    assert empty.message == "verified_query 'orders_today' executed and returned 0 rows in 0ms (in sales, menu, other)"


def test_one_session_at_a_time_reads_describe_then_explain_then_count_and_spot_check() -> None:
    warehouse = Warehouse()
    _validated(warehouse, Fanout(warehouse))
    kinds = [text.split(" ")[0] if not text.startswith("SELECT") else text[:20] for _scope, text in warehouse.ran]
    assert kinds == [
        "DESCRIBE",
        "DESCRIBE",
        *["EXPLAIN"] * 4,
        "SELECT COUNT(*) AS R",
        "SELECT COUNT(*) - CO",
    ]
