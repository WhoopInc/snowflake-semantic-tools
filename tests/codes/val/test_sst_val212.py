"""SST-VAL212: the connected spot check finds a join target repeating a join key."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Relationship, SemanticView, Table
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.sql_values import authored


class Scripted(InMemorySnowflake):
    """A port that answers the spot-check reads with one fixed row."""

    def __init__(self, answers: dict[str, tuple[object, ...]]) -> None:
        super().__init__()
        self._answers = answers

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        for marker, row in self._answers.items():
            if marker in text:
                return QueryResult(("A",), (row,))
        return super().query(sql, params)


def _validate(view: SemanticView, answers: dict[str, tuple[object, ...]]) -> list[Diagnostic]:
    compiled = CompileResult((CompiledView(view, render(view)),))
    return list(ValidateArtifacts(Scripted(answers)).run(compiled, strict=False, connected=True).diagnostics)


VIEW = SemanticView(
    "DB.S.SALES",
    (Table("ORDERS", "DB.S.ORDERS"), Table("CUSTOMERS", "DB.S.CUSTOMERS", primary_key=("CUSTOMER_ID",))),
    metrics=(Metric("ROWS", authored("COUNT(1)"), "ORDERS"),),
    relationships=(Relationship("ORDERS_TO_CUSTOMERS", "ORDERS", ("CUSTOMER_ID",), "CUSTOMERS", ("CUSTOMER_ID",)),),
)


def test_sst_val212_fires() -> None:
    [found] = [item for item in _validate(VIEW, {"COUNT(DISTINCT": (3,)}) if item.code == "SST-VAL212"]
    assert found.severity is Severity.WARNING
    assert found.message == (
        "relationship 'orders_to_customers' declares cardinality many-to-one; "
        "the spot-check found 3 rows of 'CUSTOMERS' repeat a join key"
    )
    assert found.subject == "semantic_view:sales"


def test_sst_val212_silent() -> None:
    assert [item for item in _validate(VIEW, {"COUNT(DISTINCT": (0,)}) if item.code == "SST-VAL212"] == []
