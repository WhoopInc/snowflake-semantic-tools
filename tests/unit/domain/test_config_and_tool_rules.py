"""Configuration rules about tools and stated policy, and the tool catalog rules SST owns."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolOwnership
from snowflake_semantic_tools.domain.validate.config import (
    config_tool_references,
    unreferenced_tool_members,
    unstated_policy,
    validate_config,
)
from snowflake_semantic_tools.domain.validate.tool import rebuilt_source, reference_ddl, validate_tool_catalog
from tests.helpers.agent_builders import TOOL_ORIGIN, TOOLS_FILE, catalog, procedure_member, search_member


def test_a_member_no_tool_call_names_is_referenced_by_nothing() -> None:
    tools = catalog(search_member("docs"), procedure_member("lookup"), procedure_member("audit"))
    found = unreferenced_tool_members(tools, [("PLATFORM", "docs"), ("lookup",), ("other", "audit")])
    assert [item.context["name"] for item in found] == ["audit"]


def test_a_tool_call_in_a_configuration_value_must_name_a_declared_member() -> None:
    tools = catalog(search_member("docs"))
    tree = {
        "agents": {"+tool": "{{ tool('platform', 'Docs') }} then {{ tool('platform', 'ghost') }}"},
        "count": 3,
        "note": "{{ tool('other', 'docs') }}",
    }
    found = config_tool_references(tree, tools, file="config/sst.yml")
    assert [(item.subject, item.context["group"], item.context["name"]) for item in found] == [
        ("config:agents.+tool", "platform", "ghost"),
        ("config:note", "other", "docs"),
    ]


def test_the_syntax_check_policy_must_be_stated() -> None:
    assert [item.subject for item in unstated_policy({})] == ["config:validation"]
    assert [item.subject for item in unstated_policy({"validation": {}})] == [
        "config:validation.snowflake_syntax_check"
    ]
    assert unstated_policy({"validation": {"snowflake_syntax_check": False}}) == ()


def test_the_default_orchestration_model_must_be_allowed() -> None:
    def codes(tree: dict[str, object]) -> list[str]:
        return [item.code for item in validate_config(tree) if item.code == "SST-CFG025"]

    assert codes({"agents": {"+orchestration_model": "claude"}}) == ["SST-CFG025"]
    assert codes({"agents": {"+orchestration_model": "auto"}}) == []
    allowed = {"agents": {"+orchestration_model": "claude"}, "snowflake": {"orchestration_models": ["claude"]}}
    assert codes(allowed) == []


def test_an_immutable_group_defines_nothing() -> None:
    found = validate_tool_catalog(catalog(search_member(), immutable=True), DbtCatalog("v12", None, None, ()))
    assert "SST-VAL606" in [item.code for item in found]
    referenced = ToolCatalog(
        (
            ToolGroup(
                "platform",
                TOOL_ORIGIN,
                TOOLS_FILE,
                immutable=True,
                members=(procedure_member("lookup", reference=True),),
            ),
        ),
        "dev",
        frozenset(("dev",)),
    )
    assert "SST-VAL606" not in [
        item.code for item in validate_tool_catalog(referenced, DbtCatalog("v12", None, None, ()))
    ]


def test_a_search_service_over_a_rebuilt_model_must_refresh_full() -> None:
    member = search_member()
    found = rebuilt_source(member, "DB.S.DOCS", "table")
    assert found is not None and (found.code, found.context["detail"]) == ("SST-VAL617", "table")
    assert rebuilt_source(member, "DB.S.DOCS", "incremental") is None
    assert rebuilt_source(search_member(refresh_mode="full"), "DB.S.DOCS", "view") is None
    assert rebuilt_source(procedure_member(), "DB.S.DOCS", "table") is None
    assert rebuilt_source(member, "DB.S.DOCS", None) is None


def test_a_referenced_member_is_never_rendered() -> None:
    members = (search_member(), procedure_member("lookup", reference=True))
    assert [(item.code, item.subject) for item in reference_ddl(members)] == [("SST-VAL612", "tool:lookup")]
    assert reference_ddl((procedure_member("lookup", reference=False),)) == ()
    assert procedure_member("x").ownership is ToolOwnership.REFERENCE


def test_a_member_both_defined_and_referenced_in_one_group_is_undecidable() -> None:
    tools = catalog(procedure_member("lookup", reference=False), procedure_member("LOOKUP", reference=True))
    found = [
        item for item in validate_tool_catalog(tools, DbtCatalog("v12", None, None, ())) if item.code == "SST-CFG019"
    ]
    assert [item.context["name"] for item in found] == ["lookup"]
