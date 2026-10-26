"""Mapping live agents and CoWork profiles: each tool type, what is kept, and what is reported."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from snowflake_semantic_tools.domain.importing.agent import (
    AgentPlacement,
    LiveAgent,
    import_agent,
    import_cowork_profile,
)
from snowflake_semantic_tools.domain.importing.model import Dependency
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.references import Placed, ReferencedTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import index

_HELPER = Placed("agent", "helper", QualifiedName.parse("DB.S.HELPER"), ())


def _live(spec: Mapping[str, Any], **fields: Any) -> LiveAgent:
    values: dict[str, Any] = {"comment": None, "profile": {}, "secure": None, **fields}
    return LiveAgent(QualifiedName.parse("DB.S.BOT"), values["comment"], values["profile"], values["secure"], spec)


def _placed(*, overridden: bool = False, **inherited: object) -> AgentPlacement:
    return AgentPlacement("agents", "tools", Destination(("sales",), overridden=overridden), inherited)


def _spec(imported: Any) -> dict[str, Any]:
    return cast(dict[str, Any], imported.files[0].document.get("spec", {}))


def _document(imported: Any) -> Mapping[str, object]:
    document: Mapping[str, object] | None = imported.files[0].document
    assert document is not None
    return document


def _details(imported: Any) -> list[str]:
    return [item.message for item in imported.diagnostics if item.code == "SST-IMP013"]


def _tool(tool_type: str, name: str | None = None, **fields: Any) -> dict[str, Any]:
    return {"tool_spec": {"type": tool_type, **({"name": name} if name else {}), **fields}}


def test_an_agent_with_nothing_but_its_name_writes_only_its_name() -> None:
    imported = import_agent(_live({}), index(), _placed())
    assert imported.files[0].document == {"name": "bot"}
    assert imported.files[0].path == "agents/sales/bot/agent.yml"
    assert imported.diagnostics == ()


def test_an_agent_records_security_unlike_its_folder_and_keeps_unrendered_sections() -> None:
    imported = import_agent(
        _live({"experimental": {"x": 1}}, secure=True, profile={"display_name": "Bot", "theme": "dark"}),
        index(),
        _placed(overridden=True),
    )
    document = _document(imported)
    assert document["config"] == {"database": "DB", "schema": "S", "secure": True}
    assert document["profile"] == {"display_name": "Bot"}
    assert document["spec"] == {"passthrough": {"experimental": {"x": 1}}}
    assert any("profile key 'theme' is not one SST sets" in detail for detail in _details(imported))


def test_an_agent_whose_security_matches_its_folder_writes_no_config() -> None:
    imported = import_agent(_live({}, secure=True), index(), _placed(secure=True))
    assert "config" not in _document(imported)


def test_unmodelled_model_orchestration_and_instruction_keys_keep_whole_sections_in_passthrough() -> None:
    spec = {
        "models": {"orchestration": "auto", "fallback": "other"},
        "orchestration": {"budget": {"seconds": 5}, "planner": "fast"},
        "instructions": {"response": "Hi.", "system": "Be terse."},
    }
    imported = import_agent(_live(spec), index(), _placed())
    assert _spec(imported) == {"passthrough": spec}
    assert _document(imported)["config"] == {"orchestration_model": "auto"}
    details = _details(imported)
    assert any("`models` holds keys SST does not model" in detail for detail in details)
    assert any("`orchestration` holds planner, which SST does not model for an agent" in detail for detail in details)
    assert any("`instructions` holds keys SST does not model" in detail for detail in details)


def test_a_sample_question_that_is_not_text_is_dropped() -> None:
    spec = {"instructions": {"sample_questions": [{"question": 7}, "Revenue?", {"question": ""}]}}
    assert _spec(import_agent(_live(spec), index(), _placed())) == {
        "instructions": {"sample_questions": [{"question": "Revenue?"}]}
    }


def test_builtin_and_unknown_tools_are_written_or_left_out_as_sst_renders_them() -> None:
    spec = {
        "tools": [
            _tool("web_search", description="Search the web.", strict=True),
            _tool("data_to_chart", "charts"),
            _tool("mystery", "odd"),
        ],
        "tool_resources": {"web_search": {"region": "us"}},
    }
    imported = import_agent(_live(spec), index(), _placed())
    assert _spec(imported)["tools"] == [
        {"type": "web_search", "description": "Search the web.", "tool_spec_passthrough": {"strict": True}},
        {"type": "data_to_chart", "name": "charts"},
    ]
    details = _details(imported)
    assert any("built-in tool 'web_search' has resources" in detail for detail in details)
    assert any("tool 'odd' of type 'mystery' is not one SST renders" in detail for detail in details)


def test_an_analyst_tool_without_a_view_or_with_unrendered_parts_is_reported() -> None:
    spec = {
        "tools": [
            _tool("cortex_analyst_text_to_sql", "nothing", description=3),
            _tool("cortex_analyst_text_to_sql", "SALES", input_schema={"type": "object"}),
        ],
        "tool_resources": {
            "nothing": {"semantic_view": "not a name"},
            "SALES": {"semantic_view": "DB.SV.SALES", "extra_flag": True, "execution_environment": {"warehouse": "WH"}},
        },
    }
    imported = import_agent(
        _live(spec),
        index(Placed("semantic_view", "sales", QualifiedName.parse("DB.SV.SALES"), ())),
        _placed(warehouse="wh"),
    )
    assert _spec(imported)["tools"] == [
        {"type": "cortex_analyst_text_to_sql", "name": "nothing"},
        {"type": "cortex_analyst_text_to_sql", "semantic_view": "{{ semantic_view('sales') }}"},
    ]
    details = _details(imported)
    assert any("Cortex Analyst tool 'nothing' names no semantic view" in detail for detail in details)
    assert any("Cortex Analyst tool 'SALES' has an input schema" in detail for detail in details)
    assert any("has resources SST does not render: extra_flag" in detail for detail in details)


def test_a_search_tool_without_a_service_keeps_its_unrendered_resources() -> None:
    spec = {
        "tools": [_tool("cortex_search", "menu")],
        "tool_resources": {"menu": {"max_results": 3, "filter": {}, "boost": 2}},
    }
    imported = import_agent(_live(spec), index(), _placed())
    assert _spec(imported)["tools"] == [
        {"type": "cortex_search", "name": "menu", "max_results": 3, "passthrough": {"boost": 2}}
    ]
    assert any("Cortex Search tool 'menu' names no search service" in detail for detail in _details(imported))


def test_generic_tools_reuse_one_member_per_routine_and_an_existing_member_when_the_project_has_one() -> None:
    known = QualifiedName.parse("DB.T.KNOWN")
    spec = {
        "tools": [
            _tool("generic", "first", input_schema={"type": "object"}),
            _tool("generic", "second", input_schema="not a schema"),
            _tool("generic", "third"),
            _tool("generic", "fourth"),
        ],
        "tool_resources": {
            "first": {"identifier": "DB.T.SCORE", "type": "FUNCTION", "retries": 2},
            "second": {"identifier": "DB.T.SCORE", "type": "function"},
            "third": {"identifier": "DB.T.KNOWN"},
            "fourth": {"identifier": 42},
        },
    }
    project = index(tools={known.folded: ReferencedTool("known_member", "procedure", known)})
    imported = import_agent(_live(spec), project, _placed())
    assert _spec(imported)["tools"] == [
        {
            "type": "generic",
            "name": "first",
            "identifier": "{{ tool('score') }}",
            "input_schema": {"type": "object"},
            "passthrough": {"retries": 2},
        },
        {"type": "generic", "name": "second", "identifier": "{{ tool('score') }}"},
        {"type": "generic", "name": "third", "identifier": "{{ tool('known_member') }}"},
        {"type": "generic", "name": "fourth"},
    ]
    members = [file for file in imported.files if file.path.startswith("tools/")]
    assert [file.path for file in members] == ["tools/sales/score/score.yml"]
    assert members[0].document == {"tool": {"name": "score", "type": "function", "relations": {"dev": "DB.T.SCORE"}}}
    assert any("generic tool 'fourth' names no procedure or function" in detail for detail in _details(imported))


def test_an_agent_toolset_names_a_project_agent_or_a_referencing_member() -> None:
    spec = {
        "tools": [_tool("agent_toolset", "ours"), _tool("agent_toolset", "theirs"), _tool("agent_toolset", "none")],
        "tool_resources": {"ours": {"agent_name": "DB.S.HELPER"}, "theirs": {"agent_name": "OTHER.S.BOT"}},
    }
    imported = import_agent(_live(spec), index(_HELPER), _placed())
    assert _spec(imported)["tools"] == [
        {"type": "agent_toolset", "name": "ours", "agent": "{{ agent('helper') }}"},
        {"type": "agent_toolset", "name": "theirs", "identifier": "{{ tool('bot') }}"},
        {"type": "agent_toolset", "name": "none"},
    ]
    assert any("agent toolset 'none' names no agent" in detail for detail in _details(imported))


def test_a_tools_warehouse_matching_the_inherited_one_is_not_written() -> None:
    spec = {
        "tools": [_tool("cortex_analyst_text_to_sql", "sales")],
        "tool_resources": {"sales": {"semantic_view": "DB.SV.SALES", "execution_environment": {"warehouse": "wh"}}},
    }
    imported = import_agent(_live(spec), index(), _placed(warehouse="WH"))
    assert "warehouse" not in _spec(imported)["tools"][0]


def test_skills_resolve_to_project_skills_or_plugins_and_report_what_the_project_lacks() -> None:
    spec = {
        "skills": [
            {"name": "lookup", "source": {"type": "CORTEX_EXTENSION", "path": "DB.X.HELPERS"}},
            {"source": {"type": "CORTEX_EXTENSION", "path": "DB.X.HELPERS"}},
            {"source": {"type": "CORTEX_EXTENSION", "path": "DB.X.SOLO"}},
            {"name": "gone", "source": {"type": "CORTEX_EXTENSION", "path": "DB.X.GONE"}},
            {"name": "broken", "source": {"type": "CORTEX_EXTENSION", "path": "not a name"}},
            {"source": {"type": "STAGE", "path": "@DB.X.ST/skills/known/"}},
        ]
    }
    project = index(
        Placed("plugin", "helpers", QualifiedName.parse("DB.X.HELPERS"), ()),
        Placed("skill", "solo", QualifiedName.parse("DB.X.SOLO"), ()),
        skill_names=frozenset({"known"}),
    )
    imported = import_agent(_live(spec), project, _placed())
    assert _spec(imported)["skills"] == [
        {
            "name": "{{ skill('lookup') }}",
            "source": {"type": "CORTEX_EXTENSION", "path": "{{ plugin('helpers') }}"},
        },
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ plugin('helpers') }}"}},
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('solo') }}"}},
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('gone') }}"}},
        {"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('known') }}"}},
    ]
    assert [item.code for item in imported.diagnostics if item.code == "SST-IMP012"] == ["SST-IMP012"]
    assert imported.dependencies == (Dependency("skill", "DB.X.GONE"),)
    details = _details(imported)
    assert any("skill 'broken' has a source SST cannot read" in detail for detail in details)
    assert any("skill 'known' is read from the stage path" in detail for detail in details)


def test_a_cowork_profile_maps_routed_agents_builtin_tools_and_what_it_cannot_render() -> None:
    spec = {
        "instructions": {"response": "Be brief."},
        "tools": [
            _tool("agent_router", "router"),
            _tool("web_search"),
            _tool("code_execution", "runner"),
            _tool("cortex_search", "menu"),
        ],
        "tool_resources": {
            "router": {
                "subagents": [
                    {"source": {"fqn": "DB.S.HELPER", "sample_questions": ["How?", ""]}, "display_name": "Helper"},
                    {"name": "OTHER.S.FAR", "description": "Far away."},
                    {"display_name": "Nobody"},
                ]
            }
        },
        "skills": [{"source": {"type": "STAGE", "path": "@DB.X.ST/skills/notes"}}],
        "experimental": True,
    }
    imported = import_cowork_profile(_live(spec, comment="Desk.", profile={"color": "blue"}), index(_HELPER), _placed())
    document = _document(imported)
    assert document["comment"] == "Desk."
    assert document["display"] == {"color": "blue"}
    assert document["spec"] == {
        "instructions": {"response": "Be brief."},
        "subagents": [
            {"agent": "{{ agent('helper') }}", "display_name": "Helper", "sample_questions": ["How?"]},
            {"agent": "{{ tool('far') }}", "description": "Far away."},
            {"display_name": "Nobody"},
        ],
        "tools": [{"type": "web_search"}, {"type": "code_execution", "name": "runner"}],
        "skills": [{"source": {"type": "CORTEX_EXTENSION", "path": "{{ skill('notes') }}"}}],
        "passthrough": {"experimental": True},
    }
    assert imported.files[0].path == "agents/sales/bot/cowork_profile.yml"
    details = _details(imported)
    assert any("tool 'menu' of type 'cortex_search' is not one a profile renders" in detail for detail in details)
    assert any("a subagent names no agent" in detail for detail in details)


def test_a_bare_cowork_profile_keeps_any_orchestration_in_passthrough() -> None:
    imported = import_cowork_profile(_live({"orchestration": {"budget": {"seconds": 5}}}), index(), _placed())
    assert imported.files[0].document == {
        "name": "bot",
        "spec": {"passthrough": {"orchestration": {"budget": {"seconds": 5}}}},
    }
    assert any("which SST does not model for a cowork_profile" in detail for detail in _details(imported))
