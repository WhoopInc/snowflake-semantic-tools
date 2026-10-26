"""Compile-time and connected agent checks at the edges the broader suites skip."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.enrich import EnrichRequest, select_models
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.agent_builders import (
    TOOL_ORIGIN,
    TOOLS_FILE,
    agent,
    catalog,
    compile_agents,
    compile_tools,
    observe,
    search_member,
    search_tool,
)
from tests.helpers.snowflake_fake import FakeSnowflake


def test_a_selector_with_a_misplaced_graph_operator_is_named_as_written() -> None:
    model = DbtModel("model.shop.orders", "orders", "DB.SCH.ORDERS", (), (), (), package_name="shop")
    found, diagnostics = select_models(
        DbtCatalog("v12", "1.11.2", "shop", (model,)), EnrichRequest(selected=("or+ders",))
    )
    assert found == ()
    assert [(item.code, item.context["model"]) for item in diagnostics] == [("SST-DBT002", "or+ders")]


_REFERENCED_SEARCH = ToolMember(
    "docs_search",
    "cortex_search_service",
    ToolOwnership.REFERENCE,
    TOOL_ORIGIN,
    TOOLS_FILE,
    description="Shared documents.",
    relations=MappingProxyType({"dev": "DB.X.SVC", "prod": "DB.X.SVC"}),
)


class _NoGrants(FakeSnowflake):
    """A catalog whose SHOW GRANTS fails, which proves nothing about who may use an object."""

    def show_grants(
        self, object_type: str, qualified_name: QualifiedName, routine_signature: tuple[str, ...] = ()
    ) -> tuple[GrantRow, ...]:
        raise SnowflakePortError("down")


def test_a_referenced_search_service_is_checked_for_usage_by_the_session_role() -> None:
    result = compile_agents(agent("sales_agent", search_tool()), tools=catalog(_REFERENCED_SEARCH))
    port = FakeSnowflake()
    port.existing = {"DB.X.SVC"}
    assert [item.code for item in observe(port, result)] == ["SST-VAL616"]
    port.grants["DB.X.SVC"] = (GrantRow("USAGE", "ROLE", "TEST_ROLE"),)
    assert observe(port, result) == ()


def test_a_referenced_search_service_whose_grants_cannot_be_read_reports_nothing() -> None:
    result = compile_agents(agent("sales_agent", search_tool()), tools=catalog(_REFERENCED_SEARCH))
    port = _NoGrants()
    port.existing = {"DB.X.SVC"}
    assert observe(port, result) == ()


def test_a_search_service_that_refreshes_full_needs_no_change_tracking() -> None:
    port = FakeSnowflake()
    port.show_rows["TABLE DB.MARTS.PRODUCT_DOCS"] = {"change_tracking": "OFF"}
    assert "SST-VAL618" in [item.code for item in observe(port, compile_tools((search_member(),)))]
    assert "SST-VAL618" not in [
        item.code for item in observe(port, compile_tools((search_member(refresh_mode="FULL"),)))
    ]
