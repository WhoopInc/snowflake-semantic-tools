"""Tool and agent names: those validate accepts key the object compile renders as observe keys it."""

from __future__ import annotations

from dataclasses import replace

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.render.tool import render_tool
from snowflake_semantic_tools.domain.sql import qname, routine_ref
from snowflake_semantic_tools.domain.validate.agent import agent_rules
from snowflake_semantic_tools.domain.validate.sql import name_problem, quoted_name_problem, stage_segment_problem
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import agent

_NAMES = st.from_regex(r"[A-Za-z_][A-Za-z0-9_$]{0,20}", fullmatch=True) | st.sampled_from(
    ('"MixedStage"', '"ORDERS"', '"a""b"', "select", "zz$stage", "my-tool", "")
)
_CATALOG = DbtCatalog("v12", None, None, ())


def _stage(name: str) -> ToolMember:
    return ToolMember(name, "stage", ToolOwnership.DEFINE, Origin("tools.yml"), "tools.yml")


def _accepted(name: str) -> bool:
    found = validate_tool_catalog(ToolCatalog((_stage(name),), "dev", frozenset(("dev",))), _CATALOG)
    refused = name_problem(name, artifact="tool", subject="tool")
    return refused is None and not found.has_errors


@given(_NAMES)
def test_every_tool_name_validate_accepts_keys_the_object_observe_finds(name: str) -> None:
    if not _accepted(name):
        return
    rendered = render_tool(_stage(name), QualifiedName.from_parts("DB", "S", name), None)
    target = rendered.target
    # SHOW prints the name Snowflake stored, which is what observe keys the object by.
    shown = ShowRow(target.name.value, "DB", "S", "OWNER", "now", object_type="STAGE")
    assert rendered.key == artifact_key("tool", shown.qualified_name.artifact_component)


def test_quoted_names_are_refused_and_unparsed_ones_left_to_name_problem() -> None:
    assert quoted_name_problem('"MixedCase"', artifact="a", subject="a") is not None
    assert quoted_name_problem("plain", artifact="a", subject="a") is None
    assert quoted_name_problem("my-tool", artifact="a", subject="a") is None
    assert validate_tool_catalog(ToolCatalog((_stage('"Q"'),), "dev", frozenset(("dev",))), _CATALOG).has_errors
    referenced = ToolMember(
        '"Q"', "procedure", ToolOwnership.REFERENCE, Origin("tools.yml"), "tools.yml", relations={"dev": "DB.S.Q"}
    )
    found = validate_tool_catalog(ToolCatalog((referenced,), "dev", frozenset(("dev",))), _CATALOG)
    assert "SST-PRS005" not in [item.code for item in found]


def test_an_agent_name_no_stage_path_holds_is_refused() -> None:
    assert stage_segment_problem("sales_agent", artifact="a", subject="a") is None
    assert stage_segment_problem("..", artifact="a", subject="a") is not None
    codes = [item.code for item in agent_rules(agent('"Mixed"'), frozenset(("auto",)), ())]
    assert codes.count("SST-PRS005") == 1
    assert [item.code for item in agent_rules(agent(".."), frozenset(("auto",)), ())].count("SST-PRS005") == 1


def test_a_routine_is_addressed_with_its_types_and_empty_parentheses_without_any() -> None:
    target = QualifiedName.parse("DB.S.COUNT")
    assert str(routine_ref(target, ())) == f"{qname(target)}()"
    assert str(routine_ref(target, ("NUMBER", "VARCHAR"))) == "DB.S.COUNT(NUMBER, VARCHAR)"


def test_a_routine_renders_its_description_on_one_line_before_execute_as() -> None:
    target = QualifiedName.parse("DB.S.ORDER_STATUS")
    member = ToolMember(
        "order_status",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Look an\n  order up.",
        body="BEGIN RETURN 1; END;",
        returns="NUMBER",
        language="sql",
        warehouse="WH",
    )
    lines = render_tool(member, target, None).ddl.splitlines()
    assert lines[lines.index("  COMMENT = 'Look an order up.'") + 1] == "  EXECUTE AS CALLER"
    undescribed = render_tool(replace(member, description=""), target, None)
    assert "COMMENT" not in undescribed.ddl
    assert render_tool(member, target, None).fingerprint != undescribed.fingerprint
