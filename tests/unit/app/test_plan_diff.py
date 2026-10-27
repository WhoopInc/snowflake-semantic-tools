"""Plan's member diff: each planned update's live definition read and compared with what SST renders."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff import compiled_fingerprints, diff_updates, from_state
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, Metric, SemanticView, Table
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.plan.definitions.tool import search_query
from snowflake_semantic_tools.domain.plan.member_diff import CHANGED, MemberDiff, PropertyDiff
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.tool import render_tool
from tests.helpers.described_objects import described_routine, described_search, described_view
from tests.helpers.plan_codes import entry, live
from tests.helpers.plan_codes import view as rendered_view
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, statement


def _model(comment: str = "Orders placed.") -> SemanticView:
    return SemanticView(
        fqn="DB.SCH.SALES",
        tables=(Table("ORDERS", "DB.MARTS.ORDERS", primary_key=("ORDER_ID",)),),
        columns=(Column("ORDERS", "ORDER_ID", ColumnKind.DIMENSION, authored("ORDERS.ORDER_ID")),),
        metrics=(Metric("ORDER_COUNT", authored("COUNT(ORDERS.ORDER_ID)"), "ORDERS", comment=comment),),
    )


def _change(action: Action = Action.UPDATE, name: str = "sales") -> Change:
    return Change(
        key=f"semantic_view:{name}",
        artifact_type="semantic_view",
        action=action,
        reason=ChangeReason.FINGERPRINT_DIFFERS,
        rendered=rendered_view(name),
        observed=None,
        depends_on=(),
        order=0,
    )


def _compiled(model: SemanticView) -> dict[str, CompiledView]:
    item = CompiledView(model, statement(f"CREATE SEMANTIC VIEW {model.fqn}"))
    return {item.artifact_key: item}


def test_an_update_carries_what_it_changes_member_by_member() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model("Orders."))
    [change], refused = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert refused == ()
    assert change.member_diff == (
        MemberDiff("metric", "orders.order_count", CHANGED, (PropertyDiff("comment", "Orders.", "Orders placed."),)),
    )


def test_an_update_whose_members_all_agree_carries_an_empty_diff() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model())
    [change], _ = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert change.member_diff == ()


def test_the_views_yaml_is_read_where_snowflake_returns_it() -> None:
    port = FakeSnowflake()
    port.view_documents["DB.SCH.SALES"] = {
        "name": "SALES",
        "tables": [
            {
                "name": "ORDERS",
                "base_table": {"database": "DB", "schema": "MARTS", "table": "ORDERS"},
                "primary_key": {"columns": ["ORDER_ID"]},
                "dimensions": [{"name": "ORDER_ID", "expr": "ORDERS.ORDER_ID"}],
                "metrics": [{"name": "ORDER_COUNT", "expr": "COUNT(ORDERS.ORDER_ID)", "description": "Orders."}],
            }
        ],
        "max_staleness": "1 hour",
    }
    [change], refused = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert refused == ()
    assert {(item.name, prop.name) for item in change.member_diff or () for prop in item.properties} == {
        ("orders.order_count", "comment"),
        ("max_staleness", "text"),
    }


def test_a_yaml_document_that_is_no_view_is_a_refused_read() -> None:
    port = FakeSnowflake()
    port.view_documents["DB.SCH.SALES"] = {"name": "SALES"}
    [change], [diagnostic] = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert change.member_diff is None and diagnostic.code == "SST-PLN041"


def test_the_object_observed_is_the_one_read() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.OLD_SALES"] = described_view(_model())
    # A renamed view: the update replaces the object observed, not the one it now names.
    observed = live(rendered_view("old_sales"))
    [change], refused = diff_updates((replace(_change(), observed=observed),), _compiled(_model()), Fanout(port))
    assert (change.member_diff, refused) == ((), ())


def test_only_updates_of_a_comparable_type_with_a_compiled_artifact_are_read() -> None:
    port = FakeSnowflake()
    changes = (
        _change(Action.CREATE),
        _change(Action.NOOP),
        replace(_change(), artifact_type="search_service"),
        _change(name="menu"),
    )
    assert diff_updates(changes, _compiled(_model()), Fanout(port)) == (changes, ())


def test_a_refused_read_leaves_the_update_uncompared_and_says_why() -> None:
    port = FakeSnowflake()
    port.fail("describe_semantic_view", SnowflakePortError("insufficient privileges"))
    [change], [diagnostic] = diff_updates((_change(),), _compiled(_model()), Fanout(port))
    assert change.member_diff is None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-PLN041",
        Severity.INFO,
        "semantic_view:sales",
    )


def test_a_compile_and_change_that_do_not_fit_are_a_caller_bug() -> None:
    port = FakeSnowflake()
    port.view_descriptions["DB.SCH.SALES"] = described_view(_model())
    with pytest.raises(TypeError, match="did not compile as a CompiledView"):
        stand_in = SimpleNamespace(artifact_key="semantic_view:sales")
        diff_updates((_change(),), {"semantic_view:sales": stand_in}, Fanout(port))
    with pytest.raises(ValueError, match="names no object"):
        diff_updates((replace(_change(), rendered=None),), _compiled(_model()), Fanout(port))


_ORIGIN = Origin("tools/t.yml", 1, 1)
_TARGET = QualifiedName.parse("DB.TOOLS.ORDER_STATUS")


def _tool(**fields: object) -> CompiledTool:
    member = ToolMember("order_status", "procedure", ToolOwnership.DEFINE, _ORIGIN, "tools/t.yml")
    member = replace(member, **fields)  # type: ignore[arg-type]
    source = QualifiedName.parse("DB.MARTS.PRODUCTS") if member.type == "cortex_search_service" else None
    return CompiledTool(member, render_tool(member, _TARGET, source))


def _tool_change(tool: CompiledTool, *, observed: bool = True) -> Change:
    seen = (
        replace(live(tool.rendered), comment="Returns a status.", routine_signature=("VARCHAR",)) if observed else None
    )
    return Change(
        tool.artifact_key, "tool", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, tool.rendered, seen, (), 0
    )


def test_a_procedure_is_read_by_the_signature_observed_with_the_comment_shown() -> None:
    tool = _tool(
        description="Returns a status.",
        signature=(ToolParameter("id", "VARCHAR"),),
        returns="VARCHAR",
        language="sql",
        warehouse="wh",
        body="BEGIN RETURN 'shipped'; END;",
    )
    port = FakeSnowflake()
    described = described_routine(replace(tool.member, body="BEGIN RETURN 'open'; END;"))
    port.routine_descriptions["PROCEDURE DB.TOOLS.ORDER_STATUS(VARCHAR)"] = described
    [change], refused = diff_updates((_tool_change(tool),), {tool.artifact_key: tool}, Fanout(port))
    assert refused == ()
    assert change.member_diff == (
        MemberDiff(
            "procedure",
            "order_status",
            CHANGED,
            (PropertyDiff("body", "BEGIN RETURN 'open'; END;", "BEGIN RETURN 'shipped'; END;"),),
        ),
    )
    # Unobserved, a routine is read by its declared signature, and its comment is unknown.
    [unseen], _ = diff_updates((_tool_change(tool, observed=False),), {tool.artifact_key: tool}, Fanout(port))
    assert [item.label for item in unseen.member_diff or ()] == ["procedure order_status"]


def test_a_stage_tool_is_not_compared_and_a_missing_search_service_is_unavailable() -> None:
    stage = _tool(type="stage", description="Exports.")
    port = FakeSnowflake()
    [change], refused = diff_updates((_tool_change(stage),), {stage.artifact_key: stage}, Fanout(port))
    assert (change.member_diff, refused) == (None, ())
    search = _tool(type="cortex_search_service", search_column="body", warehouse="wh", target_lag="1 hour")
    [change], [note] = diff_updates((_tool_change(search),), {search.artifact_key: search}, Fanout(port))
    assert change.member_diff is None
    assert "no search service DB.TOOLS.ORDER_STATUS to describe" in note.message
    published = described_search(replace(search.member, target_lag="1 day"), search_query(search.rendered.ddl))
    port.descriptions["CORTEX SEARCH SERVICE DB.TOOLS.ORDER_STATUS"] = published
    [change], _ = diff_updates((_tool_change(search),), {search.artifact_key: search}, Fanout(port))
    assert [(prop.name, prop.before, prop.after) for item in change.member_diff or () for prop in item.properties] == [
        ("target_lag", "1 day", "1 hour")
    ]


def _agent(payload: str, **model: object) -> CompiledAgent:
    """A compiled agent holding only what its comparison reads: its payload and its properties."""
    agent = CompiledAgent.__new__(CompiledAgent)
    object.__setattr__(agent, "payload", payload)
    properties = {"comment": None, "profile": AgentProfile(), "alias": None, "secure": None, **model}
    object.__setattr__(agent, "resolved", SimpleNamespace(model=SimpleNamespace(**properties)))
    return agent


def test_an_agent_serving_a_specification_that_is_not_json_is_unavailable() -> None:
    agent = _agent('{"models": {}}')
    rendered = rendered_view("analyst")
    port = FakeSnowflake()
    port.agent_version_rows[rendered.target.sql] = (AgentVersion("VERSION$1", None, True, specification="[1]"),)
    change = replace(_change(), key="agent:analyst", artifact_type="agent", rendered=rendered)
    [unread], [note] = diff_updates((change,), {"agent:analyst": agent}, Fanout(port))
    assert unread.member_diff is None
    assert "is not a JSON object" in note.message


def test_an_agent_is_compared_with_the_version_it_serves_and_one_serving_none_is_unavailable() -> None:
    agent = _agent('{"instructions": {"response": "Brief."}}')
    rendered = rendered_view("analyst")
    port = FakeSnowflake()
    served = AgentVersion("VERSION$2", None, True, specification='{"instructions": {"response": "Long."}}')
    port.agent_version_rows[rendered.target.sql] = (AgentVersion("VERSION$1", None, specification="{}"), served)
    change = replace(_change(), key="agent:analyst", artifact_type="agent", rendered=rendered)
    [compared], _ = diff_updates((change,), {"agent:analyst": agent}, Fanout(port))
    assert compared.member_diff == (
        MemberDiff("instruction", "response", CHANGED, (PropertyDiff("text", "Long.", "Brief."),)),
    )
    port.agent_version_rows[rendered.target.sql] = ()
    [unread], [note] = diff_updates((change,), {"agent:analyst": agent}, Fanout(port))
    assert (unread.member_diff, "may not read the specification" in note.message) == (None, True)


def test_an_agent_compares_its_comment_profile_alias_and_secure_as_show_lists_them() -> None:
    agent = _agent(
        "{}",
        comment="Never answers questions about one customer.",
        profile=AgentProfile(display_name="Concierge", color="purple"),
        alias="stable",
        secure=False,
    )
    rendered = rendered_view("analyst")
    port = FakeSnowflake()
    port.agent_version_rows[rendered.target.sql] = (AgentVersion("VERSION$1", "RELEASED", True, specification="{}"),)
    port.show_rows[f"AGENT {rendered.target.sql}"] = {
        "comment": f"Does not answer questions about one customer. [sst:{'a' * 64}:{'b' * 64}]",
        "profile": '{"color":"teal","display_name":"Concierge"}',
        "is_secure": "false",
    }
    change = replace(_change(), key="agent:analyst", artifact_type="agent", rendered=rendered)
    [compared], _ = diff_updates((change,), {"agent:analyst": agent}, Fanout(port))
    assert compared.member_diff == (
        MemberDiff(
            "agent",
            "properties",
            CHANGED,
            (
                PropertyDiff(
                    "comment",
                    "Does not answer questions about one customer.",
                    "Never answers questions about one customer.",
                ),
                PropertyDiff(
                    "profile",
                    '{"color": "teal", "display_name": "Concierge"}',
                    '{"color": "purple", "display_name": "Concierge"}',
                ),
                PropertyDiff("alias", "RELEASED", "STABLE"),
            ),
        ),
    )
    # The same properties, and SECURE as SHOW spells it, are no difference; one SHOW does not list
    # is not compared.
    port.show_rows[f"AGENT {rendered.target.sql}"] = {
        "comment": "Never answers questions about one customer.",
        "profile": '{"display_name":"Concierge","color":"purple"}',
        "is_secure": "N",
    }
    port.agent_version_rows[rendered.target.sql] = (AgentVersion("VERSION$1", "STABLE", True, specification="{}"),)
    [same], _ = diff_updates((change,), {"agent:analyst": agent}, Fanout(port))
    assert same.member_diff == ()
    secure = _agent("{}", secure=True)
    del port.show_rows[f"AGENT {rendered.target.sql}"]
    [unread], _ = diff_updates((change,), {"agent:analyst": secure}, Fanout(port))
    assert [(item.kind, item.name) for item in unread.member_diff or ()] == [("agent", "properties")]
    assert [prop.name for item in unread.member_diff or () for prop in item.properties] == ["alias"]


def test_a_compiled_artifact_is_fingerprinted_member_by_member_when_its_type_is_compared() -> None:
    (view,) = _compiled(_model()).values()
    keys = [key for key, _ in compiled_fingerprints(view)]
    assert "metric:orders.order_count" in keys and "dimension:orders.order_id" in keys
    stage = _tool(type="stage")
    assert compiled_fingerprints(stage) == ()
    # An artifact that is not what its type compiles to, as a test double may be, has none.
    assert compiled_fingerprints(SimpleNamespace(artifact_type="agent", artifact_key="agent:a")) == ()
    assert compiled_fingerprints(SimpleNamespace(artifact_type="lineage")) == ()


def test_an_update_not_compared_names_the_members_state_s_fingerprints_say_changed() -> None:
    before = compiled_fingerprints(next(iter(_compiled(_model()).values())))
    after = compiled_fingerprints(next(iter(_compiled(_model("Orders taken.")).values())))
    change = replace(_change(), rendered=replace(rendered_view("sales"), member_fingerprints=after))
    recorded = {change.key: replace(entry(change.rendered, "m"), member_fingerprints=before)}  # type: ignore[arg-type]
    (named,) = from_state((change,), recorded)
    assert named.changed_members == ("metric:orders.order_count",)
    compared = replace(change, member_diff=())
    unrecorded = {change.key: entry(change.rendered, "m")}  # type: ignore[arg-type]
    noop = replace(change, action=Action.NOOP)
    assert from_state((compared, noop), recorded) == (compared, noop)
    assert from_state((change,), unrecorded) == (change,)
    assert from_state((replace(change, rendered=rendered_view("sales")),), recorded)[0].changed_members is None


def test_an_agent_s_properties_are_named_from_state_only_when_its_entry_recorded_them() -> None:
    # An entry an earlier SST wrote records no properties member, so its absence there is not a
    # change; once both record it, a differing fingerprint names it.
    spec = ("instruction:response", "1" * 64)
    change = replace(
        _change(),
        key="agent:analyst",
        artifact_type="agent",
        rendered=replace(rendered_view("analyst"), member_fingerprints=(("agent:properties", "2" * 64), spec)),
    )
    older = {change.key: replace(entry(change.rendered, "m"), member_fingerprints=(spec,))}  # type: ignore[arg-type]
    assert from_state((change,), older)[0].changed_members == ()
    recorded = {change.key: replace(older[change.key], member_fingerprints=(("agent:properties", "3" * 64), spec))}
    assert from_state((change,), recorded)[0].changed_members == ("agent:properties",)
