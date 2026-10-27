"""Mapping live tools to members: what a definition carries, and when it falls back to a reference."""

from __future__ import annotations

from typing import Any, cast

from snowflake_semantic_tools.domain.importing.model import ImportedObject
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.tool import LiveTool, ToolPlacement, import_tool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import index

_MODELS = {("DB", "MARTS", "PRODUCTS"): "products", ("DB", "MARTS", "ORDERS"): "orders"}
_PLACE = ToolPlacement("tools", Destination(("sales",)))
_OVERRIDDEN = ToolPlacement("tools", Destination((), overridden=True))


def _search(definition: str, **described: str) -> LiveTool:
    return LiveTool(
        QualifiedName.parse("DB.T.MENU_SEARCH"),
        "cortex_search_service",
        None,
        {"definition": definition, "search_column": "BODY", **described},
    )


def _routine(member_type: str = "procedure", **described: str) -> LiveTool:
    return LiveTool(QualifiedName.parse("DB.T.REFRESH"), member_type, "Refresh.", described, (("DAYS", "NUMBER"),))


def _tool(imported: ImportedObject) -> dict[str, Any]:
    document = imported.files[0].document
    assert document is not None
    return cast(dict[str, Any], document["tool"])


def _codes(imported: ImportedObject) -> list[str]:
    return [item.code for item in imported.diagnostics]


def test_a_defined_search_service_selecting_only_its_search_column_lists_no_other_columns() -> None:
    live = _search("SELECT body FROM DB.MARTS.PRODUCTS", refresh_mode="incremental")
    imported = import_tool(live, index(models=_MODELS), _PLACE, define=True)
    assert _tool(imported) == {
        "name": "menu_search",
        "type": "cortex_search_service",
        "on": {"table": "{{ ref('products') }}", "search_column": "{{ ref('products', 'body') }}"},
        "refresh_mode": "INCREMENTAL",
    }
    assert imported.diagnostics == ()


def test_a_definition_whose_live_schema_no_folder_routes_to_names_it_in_config() -> None:
    live = _search("SELECT body FROM DB.MARTS.PRODUCTS", warehouse="WH")
    imported = import_tool(live, index(models=_MODELS), _OVERRIDDEN, define=True)
    assert _tool(imported)["config"] == {"warehouse": "WH", "database": "DB", "schema": "T"}
    assert imported.files[0].path == "tools/menu_search/menu_search.yml"


def test_a_search_query_that_is_not_a_plain_select_of_one_model_is_referenced_instead() -> None:
    queries = (
        "SELECT 'unterminated FROM DB.MARTS.PRODUCTS",
        "WITH x AS (SELECT 1) SELECT body FROM x",
        "SELECT UPPER(body) FROM DB.MARTS.PRODUCTS",
        "SELECT body FROM 'x'.MARTS.PRODUCTS",
        "SELECT body FROM DB.MARTS.UNKNOWN",
    )
    for query in queries:
        imported = import_tool(_search(query), index(models=_MODELS), _PLACE, define=True)
        assert _codes(imported) == ["SST-IMP013"], query
        assert _tool(imported)["relations"] == {"dev": "DB.T.MENU_SEARCH"}, query
        assert "columns" not in _tool(imported), query
        assert "is not a SELECT of one dbt model's columns" in imported.diagnostics[0].message


def test_a_referenced_procedure_names_its_relation_and_signature_without_search_columns() -> None:
    imported = import_tool(_routine(), index(), _PLACE, define=False)
    assert _tool(imported) == {
        "name": "refresh",
        "type": "procedure",
        "description": "Refresh.",
        "relations": {"dev": "DB.T.REFRESH"},
        "signature": [{"name": "days", "type": "NUMBER"}],
    }


def test_a_routine_whose_body_cannot_be_read_back_whole_is_referenced() -> None:
    for described, reason in (
        ({"language": "SQL"}, "its sql body cannot be read back whole"),
        ({"language": "COBOL", "body": "x"}, "its cobol body cannot be read back whole"),
        ({"language": "", "body": "x"}, "its unknown body cannot be read back whole"),
        ({"body": "SELECT 'oops"}, "its body does not lex"),
    ):
        imported = import_tool(_routine(**described), index(), _PLACE, define=True)
        assert _codes(imported) == ["SST-IMP013"]
        assert reason in imported.diagnostics[0].message
        assert len(imported.files) == 1
        assert _tool(imported)["relations"] == {"dev": "DB.T.REFRESH"}


def test_a_defined_python_procedure_carries_its_runtime_handler_returns_packages_and_owner_rights() -> None:
    live = _routine(
        language="PYTHON",
        body="def run(session, days):\n    return session.table('DB.MARTS.ORDERS')\n",
        runtime_version="3.11",
        handler="run",
        returns="VARCHAR",
        packages="['snowflake-snowpark-python', 'pandas']",
        **{"execute as": "OWNER"},
    )
    imported = import_tool(live, index(models=_MODELS), _OVERRIDDEN, define=True)
    tool = _tool(imported)
    assert tool["body"] == "{{ file('refresh.py') }}"
    assert tool["language"] == "python"
    assert (tool["runtime_version"], tool["handler"], tool["returns"]) == ("3.11", "run", "VARCHAR")
    assert tool["packages"] == ["snowflake-snowpark-python", "pandas"]
    assert tool["config"] == {"execute_as": "owner", "database": "DB", "schema": "T"}
    assert imported.files[1].path == "tools/refresh/refresh.py"
    assert imported.files[1].text == "def run(session, days):\n    return session.table('DB.MARTS.ORDERS')\n"


def test_a_defined_sql_function_writes_model_relations_as_refs_and_no_owner_rights() -> None:
    live = _routine("function", body="SELECT COUNT(*) FROM DB.MARTS.ORDERS JOIN DB.X.Y", **{"execute as": "OWNER"})
    imported = import_tool(live, index(models=_MODELS), _PLACE, define=True)
    tool = _tool(imported)
    assert "config" not in tool and "returns" not in tool and "handler" not in tool and "packages" not in tool
    assert imported.files[1].text == "SELECT COUNT(*) FROM {{ ref('orders') }} JOIN DB.X.Y\n"


def test_a_procedure_running_with_the_rights_its_folder_inherits_writes_no_config() -> None:
    live = _routine(body="SELECT 1", **{"execute as": "OWNER"})
    placement = ToolPlacement("tools", Destination(("sales",)), {"execute_as": "owner"})
    assert "config" not in _tool(import_tool(live, index(), placement, define=True))
