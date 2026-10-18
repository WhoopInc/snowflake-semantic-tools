"""`sst diff` member by member: what a target holds of each artifact its state records, and the names it labels."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.diff import compiled_definitions, live_definitions, name_map
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.tool import render_tool
from tests.helpers.artifact_builders import applied_entry
from tests.helpers.described_objects import described_routine
from tests.helpers.snowflake_fake import FakeSnowflake

_TABLE = QualifiedName.parse("DB.S.SST_STATE")
_ORIGIN = Origin("tools/t.yml", 1, 1)


def _tool(name: str, kind: str, **fields: object) -> CompiledTool:
    member = ToolMember(name, kind, ToolOwnership.DEFINE, _ORIGIN, "tools/t.yml")
    member = replace(member, **fields)  # type: ignore[arg-type]
    return CompiledTool(member, render_tool(member, QualifiedName.parse(f"DB.TOOLS.{name.upper()}"), None))


_PROCEDURE = _tool(
    "order_status",
    "procedure",
    description="Returns a status.",
    signature=(ToolParameter("id", "VARCHAR"),),
    returns="VARCHAR",
    language="sql",
    warehouse="wh",
    body="BEGIN RETURN 'ok'; END;",
)
_STAGE = _tool("exports", "stage", description="Exports.")
_RESULT = CompileResult((_PROCEDURE, _STAGE))


def _port() -> FakeSnowflake:
    state = {
        _PROCEDURE.artifact_key: applied_entry("DB.TOOLS.ORDER_STATUS", "1" * 64),
        _STAGE.artifact_key: applied_entry("DB.TOOLS.EXPORTS", "2" * 64),
        "tool:dropped_from_project": applied_entry("DB.TOOLS.GONE", "3" * 64),
    }
    port = FakeSnowflake(state=state)
    port.routine_descriptions["PROCEDURE DB.TOOLS.ORDER_STATUS(VARCHAR)"] = described_routine(_PROCEDURE.member)
    return port


def test_a_held_routine_reads_as_compiled_with_its_comment_from_show_and_a_stage_is_not_read() -> None:
    port = _port()
    marker = f"[sst:{'a' * 64}:{'b' * 64}]"
    port.show_rows["PROCEDURE DB.TOOLS.ORDER_STATUS"] = {"description": f"Returns a status. {marker}"}
    definitions, unread = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    assert (set(definitions), unread) == ({_PROCEDURE.artifact_key}, {})
    desired = compiled_definitions(_RESULT, {})
    assert diff_definitions(definitions[_PROCEDURE.artifact_key], desired[_PROCEDURE.artifact_key]) == ()


def test_a_routine_show_lists_no_comment_for_reads_as_uncommented_and_a_refused_read_says_why() -> None:
    port = _port()
    definitions, _ = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    [diff] = diff_definitions(
        definitions[_PROCEDURE.artifact_key], compiled_definitions(_RESULT, {})[_PROCEDURE.artifact_key]
    )
    assert [(prop.name, prop.before) for prop in diff.properties] == [("comment", "")]
    port.fail("describe_routine", SnowflakePortError("denied"))
    definitions, unread = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    assert (definitions, unread) == ({}, {_PROCEDURE.artifact_key: "denied"})


def test_each_name_a_target_publishes_is_labelled_and_one_two_artifacts_claim_is_not() -> None:
    twin = _tool(
        "orders_status",
        "procedure",
        **{
            field: getattr(_PROCEDURE.member, field)
            for field in ("signature", "returns", "language", "warehouse", "body")
        },
    )
    clash = CompiledTool(twin.member, replace(twin.rendered, target=_PROCEDURE.rendered.target))
    names = name_map(CompileResult((_PROCEDURE, _STAGE)))
    assert names[("DB", "TOOLS", "ORDER_STATUS")] == "tool:order_status"
    assert ("DB", "TOOLS", "ORDER_STATUS") not in name_map(CompileResult((_PROCEDURE, clash)))
