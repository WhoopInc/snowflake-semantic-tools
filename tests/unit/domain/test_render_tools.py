"""The tool renderers: search services, routines, and stages, each value held to its grammar."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.render.tool import render_tool, routine_signature, search_service_statement
from snowflake_semantic_tools.domain.sql import UnsafeSqlError


def member(**kwargs: Any) -> ToolMember:
    base: dict[str, Any] = {
        "group": "platform",
        "name": "search",
        "type": "cortex_search_service",
        "ownership": ToolOwnership.DEFINE,
        "origin": Origin("tools.yml"),
        "source_file": "tools.yml",
    }
    base.update(kwargs)
    return ToolMember(**base)


def test_search_renderer_is_deterministic_and_uses_replay_grants() -> None:
    value = member(
        description="Searches docs.\nUse for product questions.",
        search_column="body",
        attribute_columns=("category",),
        warehouse="WH",
        target_lag="1 hour",
        embedding_model="model",
        where="IS_PUBLIC",
    )
    first = render_tool(value, QualifiedName.parse("DB.S.SEARCH"), QualifiedName.parse("DB.S.DOCS"))
    second = render_tool(value, QualifiedName.parse("DB.S.SEARCH"), QualifiedName.parse("DB.S.DOCS"))
    assert first == second
    assert "ON BODY" in first.ddl
    assert "ATTRIBUTES CATEGORY" in first.ddl
    assert "EMBEDDING_MODEL = 'model'" in first.ddl
    assert "AS SELECT CATEGORY, BODY FROM DB.S.DOCS WHERE IS_PUBLIC" in first.ddl
    assert first.grant_preservation is GrantPreservation.REPLAY
    assert first.object_type == "CORTEX SEARCH SERVICE"
    assert str(first.smoke[0].sql) == "DESCRIBE CORTEX SEARCH SERVICE DB.S.SEARCH"


def test_routine_renderers_include_copy_grants_and_sidecar_body() -> None:
    procedure = member(
        name="lookup",
        type="procedure",
        language="sql",
        body="BEGIN\n  RETURN 1;\nEND;",
        returns="NUMBER",
        warehouse="WH",
        signature=(ToolParameter("order_id", "NUMBER", True),),
        execute_as="owner",
        secrets=MappingProxyType({"token": "DB.S.SECRET"}),
    )
    rendered = render_tool(procedure, QualifiedName.parse("DB.S.LOOKUP"), None)
    assert "CREATE OR REPLACE PROCEDURE DB.S.LOOKUP(ORDER_ID NUMBER)" in rendered.ddl
    assert "COPY GRANTS" in rendered.ddl
    assert "EXECUTE AS OWNER" in rendered.ddl
    assert "SECRETS = ('token' = DB.S.SECRET)" in rendered.ddl
    assert rendered.grant_preservation is GrantPreservation.CLAUSE

    function = member(
        name="normalise",
        type="function",
        language="python",
        runtime_version="3.11",
        handler="run",
        body="return value",
        returns="VARCHAR",
        signature=(ToolParameter("value", "VARCHAR", True),),
    )
    rendered = render_tool(function, QualifiedName.parse("DB.S.NORMALISE"), None)
    assert "CREATE OR REPLACE FUNCTION DB.S.NORMALISE(VALUE VARCHAR)" in rendered.ddl
    assert "EXECUTE AS" not in rendered.ddl


def test_stage_renderer_never_replaces_or_prunes_content() -> None:
    stage = member(name="files", type="stage", description="Agent files")
    rendered = render_tool(stage, QualifiedName.parse("DB.S.FILES"), None)
    assert rendered.ddl == "CREATE STAGE IF NOT EXISTS DB.S.FILES COMMENT = 'Agent files'\n"
    assert rendered.grant_preservation is GrantPreservation.NONE


def test_tool_renderer_rejects_unsupported_and_incomplete_models() -> None:
    with pytest.raises(ValueError, match="not publishable"):
        render_tool(member(type="agent"), QualifiedName.parse("DB.S.A"), None)
    with pytest.raises(ValueError, match="missing required"):
        render_tool(member(), QualifiedName.parse("DB.S.SEARCH"), None)
    with pytest.raises(ValueError, match="missing required"):
        render_tool(
            member(name="p", type="procedure", language="sql", body="RETURN 1", returns="NUMBER"),
            QualifiedName.parse("DB.S.P"),
            None,
        )
    with pytest.raises(ValueError, match="runtime_version and handler"):
        render_tool(
            member(
                name="python_function",
                type="function",
                language="python",
                body="return 1",
                returns="NUMBER",
            ),
            QualifiedName.parse("DB.S.PYTHON_FUNCTION"),
            None,
        )
    with pytest.raises(ValueError, match="dollar-quote"):
        render_tool(
            member(
                name="f",
                type="function",
                language="python",
                runtime_version="3.11",
                handler="run",
                body="$$",
                returns="NUMBER",
            ),
            QualifiedName.parse("DB.S.F"),
            None,
        )


def test_search_renderer_covers_optional_attributes_embedding_comment_and_where_absence() -> None:
    value = member(
        search_column="body",
        warehouse="WH",
        target_lag="1 hour",
    )
    rendered = render_tool(value, QualifiedName.parse("DB.S.SEARCH"), QualifiedName.parse("DB.S.DOCS"))
    assert "ATTRIBUTES" not in rendered.ddl
    assert "EMBEDDING_MODEL" not in rendered.ddl
    assert "COMMENT" not in rendered.ddl
    assert " WHERE " not in rendered.ddl


def test_tool_renderers_cover_all_optional_routine_and_stage_clauses() -> None:
    procedure = member(
        name="procedure",
        type="procedure",
        language="python",
        runtime_version="3.11",
        handler="run",
        body="return 1",
        returns="NUMBER",
        warehouse="WH",
        packages=("snowflake-snowpark-python",),
        imports=("@DB.S.STAGE/module.py",),
        external_access_integrations=("NETWORK",),
    )
    rendered = render_tool(procedure, QualifiedName.parse("DB.S.PROCEDURE"), None)
    assert "PACKAGES = ('snowflake-snowpark-python')" in rendered.ddl
    assert "IMPORTS = ('@DB.S.STAGE/module.py')" in rendered.ddl
    assert "EXTERNAL_ACCESS_INTEGRATIONS = (NETWORK)" in rendered.ddl
    assert "RUNTIME_VERSION = '3.11'" in rendered.ddl
    assert "HANDLER = 'run'" in rendered.ddl

    stage = member(name="files", type="stage")
    rendered = render_tool(stage, QualifiedName.parse("DB.S.FILES"), None)
    assert rendered.ddl == "CREATE STAGE IF NOT EXISTS DB.S.FILES\n"


def test_a_search_service_takes_an_exact_comment_and_refuses_a_where_that_is_not_one_expression() -> None:
    value = member(search_column="body", warehouse="WH", target_lag="1 hour", description="a  b", where="IS_PUBLIC  ")
    marked = search_service_statement(
        value, QualifiedName.parse("DB.S.SEARCH"), QualifiedName.parse("DB.S.DOCS"), comment="[sst:m:f] a  b  "
    )
    assert str(marked).splitlines()[-2:] == [
        "  COMMENT = '[sst:m:f] a  b  '",
        "  AS SELECT BODY FROM DB.S.DOCS WHERE IS_PUBLIC",
    ]
    injected = replace(value, where="1=1 UNION SELECT secret FROM other")
    with pytest.raises(UnsafeSqlError, match="statement keyword"):
        render_tool(injected, QualifiedName.parse("DB.S.SEARCH"), QualifiedName.parse("DB.S.DOCS"))


def test_routine_values_are_held_to_their_grammars() -> None:
    table_function = member(
        name="lines",
        type="function",
        language="sql",
        body="SELECT 1, 2",
        returns="TABLE (order_id NUMBER(38, 0), total VARCHAR)",
    )
    rendered = render_tool(table_function, QualifiedName.parse("DB.S.LINES"), None)
    assert "RETURNS TABLE (ORDER_ID NUMBER(38, 0), TOTAL VARCHAR)" in rendered.ddl
    assert str(rendered.smoke[0].sql) == "DESCRIBE FUNCTION DB.S.LINES()"
    for bad in (
        {"returns": "NUMBER); DROP TABLE x; --"},
        {"returns": "TABLE (x NOT_A_TYPE)"},
        {"language": "sql; DROP TABLE x"},
        {"signature": (ToolParameter("id", "NUMBER) AS 'x'; --", True),)},
        {"body": "$$; DROP TABLE x; $$"},
    ):
        with pytest.raises(ValueError):
            render_tool(replace(table_function, **bad), QualifiedName.parse("DB.S.LINES"), None)
    procedure = replace(table_function, type="procedure", returns="NUMBER", warehouse="WH", execute_as="anyone")
    with pytest.raises(ValueError, match="not a keyword"):
        render_tool(procedure, QualifiedName.parse("DB.S.LINES"), None)


def test_a_routine_is_named_with_its_argument_types() -> None:
    target = QualifiedName.parse("DB.S.LOOKUP")
    assert str(routine_signature("procedure", target, ("NUMBER", "VARCHAR(10)"))) == "DB.S.LOOKUP(NUMBER, VARCHAR(10))"
    with pytest.raises(ValueError, match="not a routine type"):
        routine_signature("TABLE", target, ())
