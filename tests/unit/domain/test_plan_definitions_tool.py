"""A tool's two definitions: what SST renders for a procedure, function or search service, and what DESCRIBE shows.

The DESCRIBE shapes are those a live account returned: a routine's `property`/`value` rows, its
signature as `(NAME TYPE)` and its lists as `['a', 'b']`; a search service's one row, its columns
comma-separated and upper-cased, its comment as the description was written.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.tool import ToolColumn, ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.plan.definitions.tool import (
    desired_routine,
    desired_search,
    live_routine,
    live_search,
    search_query,
)
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions

_MARKER = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"
_ORIGIN = Origin("tools/t.yml", 1, 1)


def _member(**fields: object) -> ToolMember:
    return replace(ToolMember("Order_Status", "procedure", ToolOwnership.DEFINE, _ORIGIN, "tools/t.yml"), **fields)  # type: ignore[arg-type]


_PROCEDURE = _member(
    description="Returns one order's\n  status.",
    signature=(ToolParameter("order_id", "VARCHAR"),),
    returns="VARCHAR",
    language="sql",
    body="BEGIN\n    RETURN 'ok';\nEND;\n\n",
)
_PROCEDURE_ROWS = {
    "signature": "(ORDER_ID VARCHAR)",
    "returns": "VARCHAR",
    "language": "SQL",
    "execute as": "CALLER",
    "body": "\nBEGIN\n    RETURN 'ok';\nEND;\n",
}
_FUNCTION = _member(
    name="tier",
    type="function",
    signature=(ToolParameter("points", "NUMBER(38, 0)"),),
    returns="TABLE (tier VARCHAR, at NUMBER)",
    language="python",
    runtime_version="3.12",
    handler="tier",
    packages=("pandas",),
    body="def tier(points):\n    return 'Crumb'",
)
_FUNCTION_ROWS = {
    "signature": "(POINTS NUMBER(38,0))",
    "returns": "TABLE (TIER VARCHAR, AT NUMBER)",
    "language": "PYTHON",
    "runtime_version": "3.12",
    "handler": "tier",
    "packages": "['pandas']",
    "imports": "[]",
    "body": "\ndef tier(points):\n    return 'Crumb'\n",
}


def test_a_published_procedure_and_its_render_agree() -> None:
    live = live_routine("ORDER_STATUS", _PROCEDURE_ROWS, f"Returns one order's status. {_MARKER}", procedure=True)
    assert diff_definitions(live, desired_routine(_PROCEDURE, procedure=True)) == ()
    [member] = live.members
    assert (member.kind, member.name) == ("procedure", "order_status")


def test_a_published_function_and_its_render_agree() -> None:
    live = live_routine("TIER", _FUNCTION_ROWS, None, procedure=False)
    desired = desired_routine(_FUNCTION, procedure=False)
    assert diff_definitions(live, desired) == ()
    assert dict(desired.members[0].properties)["packages"] == ("pandas",)


def test_a_body_and_comment_edit_shows_as_a_line_diff_and_a_new_comment() -> None:
    edited = replace(_PROCEDURE, body="BEGIN\n    RETURN 'done';\nEND;", description="Returns one order.")
    live = live_routine("ORDER_STATUS", _PROCEDURE_ROWS, f"Returns one order's status. {_MARKER}", procedure=True)
    [diff] = diff_definitions(live, desired_routine(edited, procedure=True))
    assert [(item.name, item.before, item.after) for item in diff.properties] == [
        ("comment", "Returns one order's status.", "Returns one order."),
        ("body", "BEGIN\n    RETURN 'ok';\nEND;", "BEGIN\n    RETURN 'done';\nEND;"),
    ]


def test_what_a_routine_does_not_describe_is_not_compared() -> None:
    # No `execute as` row, no handler rows: the render's values for them go unchecked.
    rows = {
        key: value for key, value in _FUNCTION_ROWS.items() if key not in ("handler", "packages", "runtime_version")
    }
    edited = replace(_FUNCTION, handler="other", packages=())
    assert (
        diff_definitions(live_routine("TIER", rows, None, procedure=False), desired_routine(edited, procedure=False))
        == ()
    )
    procedure = {key: value for key, value in _PROCEDURE_ROWS.items() if key != "execute as"}
    owner = replace(_PROCEDURE, execute_as="owner")
    assert diff_definitions(
        live_routine("ORDER_STATUS", procedure, None, procedure=True), desired_routine(owner, procedure=True)
    )
    assert (
        diff_definitions(
            live_routine("ORDER_STATUS", procedure, "Returns one order's status.", procedure=True),
            desired_routine(owner, procedure=True),
        )
        == ()
    )
    bare = _member(signature=(), returns=None, language=None, body=None, description=None)
    assert dict(desired_routine(bare, procedure=True).members[0].properties)["signature"] == "()"


_SEARCH = _member(
    name="menu_search",
    type="cortex_search_service",
    description="Searches menu items:\nwhat is in each.",
    search_column="product_description",
    attribute_columns=("product_type",),
    columns=(ToolColumn("product_id", "", "", False, False), ToolColumn("product_type", "", "", False, True)),
    warehouse="compute_wh",
    target_lag="1 day",
    embedding_model="snowflake-arctic-embed-m-v1.5",
    refresh_mode="full",
)
_STATEMENT = (
    "CREATE OR REPLACE CORTEX SEARCH SERVICE MENU_DB.TOOLS.MENU_SEARCH\n  ON PRODUCT_DESCRIPTION\n"
    "  COMMENT = 'x'\n  AS SELECT PRODUCT_ID, PRODUCT_TYPE, PRODUCT_DESCRIPTION FROM MENU_DB.MARTS.PRODUCTS"
)
_SEARCH_ROW = {
    "search_column": "PRODUCT_DESCRIPTION",
    "attribute_columns": "PRODUCT_TYPE",
    "columns": "PRODUCT_ID,PRODUCT_TYPE,PRODUCT_DESCRIPTION",
    "warehouse": "COMPUTE_WH",
    "target_lag": "1 day",
    "embedding_model": "snowflake-arctic-embed-m-v1.5",
    "refresh_mode": "FULL",
    "definition": "SELECT PRODUCT_ID, PRODUCT_TYPE, PRODUCT_DESCRIPTION FROM MENU_DB.MARTS.PRODUCTS",
    "comment": f"Searches menu items:\nwhat is in each. {_MARKER}",
}


def test_a_published_search_service_and_its_render_agree() -> None:
    query = search_query(_STATEMENT)
    assert query == "SELECT PRODUCT_ID, PRODUCT_TYPE, PRODUCT_DESCRIPTION FROM MENU_DB.MARTS.PRODUCTS"
    assert diff_definitions(live_search("MENU_SEARCH", _SEARCH_ROW), desired_search(_SEARCH, query)) == ()


def test_a_search_service_compares_only_the_settings_the_project_sets() -> None:
    unset = replace(_SEARCH, embedding_model=None, refresh_mode=None, attribute_columns=("product_type", "allergens"))
    [diff] = diff_definitions(live_search("MENU_SEARCH", _SEARCH_ROW), desired_search(unset, search_query(_STATEMENT)))
    assert [item.name for item in diff.properties] == ["attribute_columns", "columns"]
    empty = live_search("MENU_SEARCH", {})
    assert dict(empty.members[0].properties)["columns"] == ()
    assert dict(desired_search(_member(), "").members[0].properties)["columns"] == ()
