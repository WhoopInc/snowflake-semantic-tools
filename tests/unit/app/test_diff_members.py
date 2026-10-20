"""`sst diff` member by member: what a target holds of each artifact its state records, and the names it labels."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.skills import CompileSkills
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.diff import _agent_references, compiled_definitions, live_definitions, name_map
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.plan.diff import ArtifactState, compare_states, member_differences
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.tool import render_tool
from tests.helpers.artifact_builders import applied_entry, empty_state, target
from tests.helpers.described_objects import described_routine
from tests.helpers.skill_inputs import CATALOG_CHANNEL, compile_extensions, publish_extensions, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

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


def test_a_held_routine_and_stage_read_as_compiled_with_their_comments_from_show() -> None:
    port = _port()
    marker = f"[sst:{'a' * 64}:{'b' * 64}]"
    port.show_rows["PROCEDURE DB.TOOLS.ORDER_STATUS"] = {"description": f"Returns a status. {marker}"}
    port.show_rows["STAGE DB.TOOLS.EXPORTS"] = {"name": "EXPORTS", "comment": f"Exports. {marker}"}
    definitions, unread = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    assert (set(definitions), unread) == ({_PROCEDURE.artifact_key, _STAGE.artifact_key}, {})
    desired = compiled_definitions(_RESULT, {})
    assert diff_definitions(definitions[_PROCEDURE.artifact_key], desired[_PROCEDURE.artifact_key]) == ()
    assert diff_definitions(definitions[_STAGE.artifact_key], desired[_STAGE.artifact_key]) == ()


def test_a_stage_on_two_targets_differs_by_its_comment_not_its_fingerprint() -> None:
    def held(comment: str) -> FakeSnowflake:
        port = _port()
        port.show_rows["STAGE DB.TOOLS.EXPORTS"] = {"comment": comment}
        return port

    def differences(before: FakeSnowflake, after: FakeSnowflake) -> list[tuple[str, int]]:
        # Each target's fingerprint embeds its own name, so the two always differ.
        states = [{_STAGE.artifact_key: ArtifactState(fingerprint)} for fingerprint in ("1" * 64, "2" * 64)]
        sides = [
            live_definitions(Fanout(port), _TABLE, "dev", CompileResult((_STAGE,)), names={}, eval_stage="E")[0]
            for port in (before, after)
        ]
        found = member_differences(compare_states(*states), *sides, across_targets=True)
        return [(item.key, len(item.member_diff or ())) for item in found]

    assert differences(held("Exports. [sst:" + "a" * 64 + ":" + "1" * 64 + "]"), held("Exports.")) == []
    assert differences(held("Exports."), held("Weekly exports.")) == [(_STAGE.artifact_key, 1)]
    # A stage SHOW does not list is unread, with why, and compared by fingerprint alone.
    _, unread = live_definitions(Fanout(_port()), _TABLE, "dev", _RESULT, names={}, eval_stage="E")
    assert unread == {_STAGE.artifact_key: "no stage DB.TOOLS.EXPORTS to show"}


def test_a_routine_show_lists_no_comment_for_reads_as_uncommented_and_a_refused_read_says_why() -> None:
    port = _port()
    definitions, _ = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    [diff] = diff_definitions(
        definitions[_PROCEDURE.artifact_key], compiled_definitions(_RESULT, {})[_PROCEDURE.artifact_key]
    )
    assert [(prop.name, prop.before) for prop in diff.properties] == [("comment", "")]
    port.fail("describe_routine", SnowflakePortError("denied"))
    definitions, unread = live_definitions(Fanout(port), _TABLE, "dev", _RESULT, names={}, eval_stage="EVAL_CONFIGS")
    assert (definitions, unread[_PROCEDURE.artifact_key]) == ({}, "denied")


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


def test_an_agent_labels_the_outside_objects_its_tools_and_skills_name_and_nothing_else() -> None:
    tool = SimpleNamespace(member=None, name="LOOKUP", resources={"identifier": "DB.EXT.LOOKUP", "max_results": 5})
    skills = (SimpleNamespace(path="DB.EXT.GLOSSARY", name=""), SimpleNamespace(path="@DB.S.STAGE/skill/", name="x"))
    agent = SimpleNamespace(resolved=SimpleNamespace(tools=(tool,), model=SimpleNamespace(skills=skills)))
    assert [(name.sql, label) for name, label in _agent_references(agent)] == [  # type: ignore[arg-type]
        ("DB.EXT.LOOKUP", "tool:LOOKUP"),
        ("DB.EXT.GLOSSARY", "extension:glossary"),
    ]
    # A model whose relation did not resolve labels nothing.
    unresolved = SimpleNamespace(
        artifact_key="semantic_view:v",
        rendered_artifact=SimpleNamespace(target=QualifiedName.parse("DB.S.V"), physical_resources=()),
        dbt_relations=(("orders", ""),),
    )
    assert dict(name_map(CompileResult((unresolved,)))) == {("DB", "S", "V"): "semantic_view:v"}


def test_a_skill_reads_as_the_version_state_records_not_the_one_the_catalog_serves() -> None:
    first = compile_extensions(skill_catalog(skill()))
    second = compile_extensions(skill_catalog(skill(files={"reference/steps.md": "Steps, revised.\n"})))
    port = FakeSnowflake(existing=())
    _, published = publish_extensions(port, first, empty_state())
    _, changed = publish_extensions(port, second, published)
    publish_extensions(port, first, changed)
    # The catalog serves the reverted-away VERSION$2; state records the revert, VERSION$1.
    assert port.execute_script(
        (statement("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET DEFAULT_VERSION = VERSION$2"),)
    ).ok
    result = CompileSkills(skill_catalog(skill()), CATALOG_CHANNEL).run_result()
    definitions, unread = live_definitions(
        Fanout(port), _TABLE, target().name, result, names={}, eval_stage="EVAL_CONFIGS"
    )
    assert unread == {}
    desired = compiled_definitions(result, {})
    assert diff_definitions(definitions["skill:month-close"], desired["skill:month-close"]) == ()
