"""The diagnostics loaders report when a guard refuses authored SQL or a name cannot render."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact, StatementPlan
from snowflake_semantic_tools.domain.model.sql_checks import (
    checked_expression,
    checked_query,
    name_problem,
    qualified_name_problem,
)
from snowflake_semantic_tools.domain.render.eval import RenderedEval
from snowflake_semantic_tools.domain.sql import AuthoredExpression, AuthoredQuery
from tests.helpers.sql_values import statement, statements

ORIGIN = Origin("metrics.yml", 3, 5)


def test_a_refused_expression_reports_val418_with_where_and_why() -> None:
    found = checked_expression("1); DROP TABLE x; --", kind="metric", name="m", subject="metric:m", origin=ORIGIN)
    assert isinstance(found, Diagnostic)
    assert found.code == "SST-VAL418" and found.subject == "metric:m" and found.origin == ORIGIN
    assert found.message == (
        "metric 'm': expression failed to compile: ')' closes nothing at '); DROP TABLE x; --', "
        "so SST will not send it to Snowflake"
    )
    assert isinstance(checked_expression("SUM(X)", kind="metric", name="m", subject="metric:m"), AuthoredExpression)


def test_a_refused_query_reports_val418() -> None:
    found = checked_query("DELETE FROM t", kind="verified_query", name="q", subject="verified_query:q")
    assert isinstance(found, Diagnostic) and found.code == "SST-VAL418"
    assert "a query must begin with SELECT or WITH" in found.message
    assert isinstance(checked_query("SELECT 1", kind="verified_query", name="q", subject="s"), AuthoredQuery)


@pytest.mark.parametrize("value", ["order id", "x;DROP", '"unclosed', '""', "1abc"])
def test_a_name_that_cannot_render_reports_prs005(value: str) -> None:
    found = name_problem(value, artifact="semantic_view:v", subject="semantic_view:v")
    assert found is not None and found.code == "SST-PRS005"
    assert found.message == f"semantic_view:v: '{value}' is not a valid identifier"


def test_names_and_qualified_names_that_render_report_nothing() -> None:
    assert name_problem("orders", artifact="a", subject="a") is None
    assert name_problem('"Mixed Case"', artifact="a", subject="a") is None
    assert qualified_name_problem("DB.S.T", artifact="a", subject="a") is None
    found = qualified_name_problem("DB..T", artifact="a", subject="a")
    assert found is not None and found.code == "SST-PRS005"


def test_a_document_artifact_must_name_the_statements_that_publish_it() -> None:
    with pytest.raises(TypeError, match="needs the statements"):
        RenderedArtifact.create(key="agent:a", artifact_type="agent", target=QualifiedName.parse("D.S.A"), ddl="{}")
    created = RenderedArtifact.create(
        key="agent:a",
        artifact_type="agent",
        target=QualifiedName.parse("D.S.A"),
        ddl="{}  \n",
        statements=StatementPlan(default=statements("CREATE AGENT D.S.A")),
    )
    assert created.ddl == "{}\n"


def test_a_rendered_eval_writes_its_source_statements_as_one_script() -> None:
    value = RenderedEval(
        "[]\n", statements("CREATE TABLE T (A INT)", "INSERT INTO T SELECT 1"), statement("CALL X()"), "", "", ""
    )
    assert value.source_table_sql == "CREATE TABLE T (A INT);\n\nINSERT INTO T SELECT 1;\n"
