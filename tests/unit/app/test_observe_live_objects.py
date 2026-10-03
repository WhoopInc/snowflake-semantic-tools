"""The connected checks of agents and tools against the live objects they publish over or call."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, OwnershipMarker
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.agent import render_agent_spec
from tests.helpers.agent_builders import (
    agent,
    analyst_tool,
    catalog,
    compile_agents,
    compile_tools,
    compiled_agents,
    generic_tool,
    observe,
    procedure_member,
    search_member,
    search_tool,
)
from tests.helpers.snowflake_fake import FakeSnowflake


def _codes(found: tuple[Diagnostic, ...] | list[Diagnostic]) -> list[str]:
    return [item.code for item in found]


class _Columns(FakeSnowflake):
    """A catalog whose indexed relation holds the column types given."""

    def __init__(self, kind: str = "VARCHAR") -> None:
        super().__init__()
        self.kind = kind

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        del qualified_name
        return (("DOC_ID", "VARCHAR"), ("DOC_NAME", self.kind))


class _Broken(_Columns):
    """A catalog every read of which fails, which proves nothing about the live objects."""

    def current_role(self) -> str:
        raise SnowflakePortError("down")

    def show_row(self, object_type: str, qualified_name: QualifiedName) -> dict[str, str] | None:
        raise SnowflakePortError("down")

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        raise SnowflakePortError("down")


def test_a_secure_agent_owned_by_another_role_shared_and_tagged_with_no_tag_object() -> None:
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "AGENT_ADMIN", "is_secure": "true"}
    owned = observe(port, compile_agents(agent("sales_agent", secure=True, tags=(("COST_CENTER", "x"),))))
    assert _codes(owned) == ["SST-VAL506", "SST-VAL508"]
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE", "is_secure": "yes"}
    port.grants["DB.S.SALES_AGENT"] = (GrantRow("USAGE", "SHARE", "PARTNER"), GrantRow("USAGE", "ROLE", "ANALYST"))
    port.existing = {"DB.S.COST_CENTER"}
    shared = observe(port, compile_agents(agent("sales_agent", tags=(("COST_CENTER", "x"),))))
    assert _codes(shared) == ["SST-VAL507"]


def test_an_agent_published_unchanged_must_still_match_its_rendered_spec() -> None:
    result = compile_agents(agent("sales_agent", analyst_tool(query_timeout=600)))
    [compiled] = compiled_agents(result)
    spec = render_agent_spec(compiled.resolved.model, compiled.resolved.tools)
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE"}
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {"agent_spec": json.dumps({**spec, "models": {"x": 1}})}
    port.markers["DB.S.SALES_AGENT"] = OwnershipMarker("a" * 64, compiled.definition_fingerprint)
    port.object_parameters[("WAREHOUSE", "WH", "STATEMENT_TIMEOUT_IN_SECONDS")] = "300"
    assert _codes(observe(port, result)) == ["SST-VAL509", "SST-VAL534"]
    # With a change pending, the live spec differs by design; zero is no timeout at all.
    port.markers["DB.S.SALES_AGENT"] = OwnershipMarker("a" * 64, "b" * 64)
    port.object_parameters[("WAREHOUSE", "WH", "STATEMENT_TIMEOUT_IN_SECONDS")] = "0"
    assert _codes(observe(port, result)) == []


def test_a_called_routine_must_exist_match_its_schema_be_usable_and_confirm_its_resources() -> None:
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    # Without a live spec, the generic tool's resource key is not confirmed either.
    assert _codes(observe(FakeSnowflake(), result)) == ["SST-VAL533", "SST-VAL531"]
    port = FakeSnowflake()
    port.existing = {"DB.DEV.LOOKUP"}
    port.show_rows["PROCEDURE DB.DEV.LOOKUP"] = {"arguments": "LOOKUP(NUMBER, NUMBER) RETURN VARCHAR"}
    port.grants["DB.DEV.LOOKUP"] = (GrantRow("USAGE", "ROLE", "ANALYST"),)
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE"}
    live = {"tool_resources": {"lookup": {"procedure": "DB.DEV.LOOKUP"}}}
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {"agent_spec": json.dumps(live)}
    assert _codes(observe(port, result)) == ["SST-VAL533", "SST-VAL532", "SST-VAL616"]
    port.show_rows["PROCEDURE DB.DEV.LOOKUP"] = {"arguments": "LOOKUP(VARCHAR) RETURN VARCHAR"}
    port.grants["DB.DEV.LOOKUP"] = (GrantRow("USAGE", "ROLE", "TEST_ROLE"),)
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {
        "agent_spec": json.dumps({"tool_resources": {"lookup": {"identifier": "x"}}})
    }
    assert _codes(observe(port, result)) == []


def test_a_pinned_extension_must_be_readable_by_the_consuming_role() -> None:
    skill = AgentSkill("vendor", "CORTEX_EXTENSION", "vendor_pack", "V2", ref="extension")
    port = FakeSnowflake()
    port.grants["DB.EXT.VENDOR_PACK"] = (GrantRow("READ", "ROLE", "ANALYST"),)
    assert _codes(observe(port, compile_agents(agent("sales_agent", skills=(skill,))))) == ["SST-VAL542"]
    port.grants["DB.EXT.VENDOR_PACK"] = (GrantRow("READ", "ROLE", "TEST_ROLE"),)
    assert _codes(observe(port, compile_agents(agent("sales_agent", skills=(skill,))))) == []


def test_a_search_tool_marks_no_vector_column_searchable() -> None:
    tools = catalog(search_member())
    results = (compile_tools(tools.members), compile_agents(agent("sales_agent", search_tool()), tools=tools))
    assert "SST-VAL525" in _codes(observe(_Columns("VECTOR(FLOAT, 768)"), *results))
    assert "SST-VAL525" not in _codes(observe(_Columns(), *results))


def test_a_search_service_is_checked_against_its_live_columns_source_and_grants() -> None:
    port = _Columns()
    port.descriptions["CORTEX SEARCH SERVICE DB.S.DOCS_SEARCH"] = {"columns": "DOC_ID, BODY"}
    port.show_rows["TABLE DB.MARTS.PRODUCT_DOCS"] = {"change_tracking": "OFF"}
    port.grants["DB.S.DOCS_SEARCH"] = (GrantRow("OWNERSHIP", "ROLE", "DEPLOYER"), GrantRow("USAGE", "ROLE", "ANALYST"))
    assert _codes(observe(port, compile_tools((search_member(),)))) == ["SST-VAL613", "SST-VAL618", "SST-VAL619"]
    port.descriptions["CORTEX SEARCH SERVICE DB.S.DOCS_SEARCH"] = {"columns": "doc_id,doc_name,body"}
    port.show_rows["TABLE DB.MARTS.PRODUCT_DOCS"] = {"change_tracking": "ON"}
    port.grants["DB.S.DOCS_SEARCH"] = (GrantRow("OWNERSHIP", "ROLE", "DEPLOYER"),)
    assert _codes(observe(port, compile_tools((search_member(),)))) == []


def test_a_read_that_fails_proves_nothing() -> None:
    tools = catalog(search_member(), procedure_member())
    results = (
        compile_tools(tools.members),
        compile_agents(agent("sales_agent", search_tool(), generic_tool(), secure=True), tools=tools),
    )
    # Only the generic tool's resource key stays unconfirmed, as it would without a live spec.
    assert _codes(observe(_Broken(), *results)) == ["SST-VAL533"]
