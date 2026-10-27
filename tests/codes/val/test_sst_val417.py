"""SST-VAL417: one question is a verified query, an agent sample question, and an eval row."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table, VerifiedQuery
from tests.helpers.agent_builders import agent, compile_agents, cross, evaluation
from tests.helpers.diagnostic_filters import coded
from tests.helpers.sql_values import authored, authored_query, statement

QUESTION = "How many orders are there in each state?"


def _asked(eval_question: str) -> list[Diagnostic]:
    query = VerifiedQuery("orders_by_state", QUESTION, authored_query("SELECT 1"))
    view = SemanticView(
        "DB.S.SALES",
        (Table("ORDERS", "DB.S.ORDERS"),),
        metrics=(Metric("ORDER_COUNT", authored("COUNT(*)"), "ORDERS"),),
        verified_queries=(query,),
    )
    views = CompileResult((CompiledView(view, statement("CREATE SEMANTIC VIEW DB.S.SALES")),))
    model = agent("sales_agent", sample_questions=(QUESTION,))
    return coded(cross(views, compile_agents(model), evals=(evaluation(model, eval_question),)), "SST-VAL417")


def test_sst_val417_fires() -> None:
    [diagnostic] = _asked(QUESTION)
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == f"'{QUESTION}' appears as a VQ question, an agent sample_question and an eval row"


def test_sst_val417_silent() -> None:
    # Two of the three sets sharing it is SST-VAL707's or another rule's to report.
    assert _asked("Which state has the most orders in 2026?") == []
