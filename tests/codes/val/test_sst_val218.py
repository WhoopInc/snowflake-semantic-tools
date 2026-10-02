"""SST-VAL218: the connected spot check finds a distinct range's rows overlapping."""

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
    "DB.S.MENU",
    (
        Table("ORDERS", "DB.S.ORDERS"),
        Table("PERIODS", "DB.S.PERIODS", primary_key=("PERIOD_ID",), distinct_range=("STARTS_AT", "ENDS_AT")),
    ),
    metrics=(Metric("ROWS", authored("COUNT(1)"), "ORDERS"),),
    relationships=(
        Relationship(
            "ORDERS_TO_PERIODS",
            "ORDERS",
            ("ORDERED_AT",),
            "PERIODS",
            ("STARTS_AT",),
            range_bounds=("STARTS_AT", "ENDS_AT"),
        ),
    ),
)


def test_sst_val218_fires() -> None:
    [found] = [item for item in _validate(VIEW, {" JOIN ": (1, 5, 3, 9)}) if item.code == "SST-VAL218"]
    assert found.severity is Severity.ERROR
    assert found.message == (
        "relationship 'orders_to_periods': 'PERIODS' declares distinct_range over (STARTS_AT, ENDS_AT) "
        "but the ranges overlap, for example [1, 5) and [3, 9)"
    )
    assert found.subject == "semantic_view:menu"


def test_sst_val218_silent() -> None:
    assert [item for item in _validate(VIEW, {}) if item.code == "SST-VAL218"] == []
