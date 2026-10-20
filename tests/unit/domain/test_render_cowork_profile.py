"""A CoWork profile's specification and definition render exactly (a Snowflake private preview)."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentProfile, AgentSkill
from snowflake_semantic_tools.domain.model.cowork_profile import (
    CoworkProfileModel,
    CoworkProfileTool,
    ResolvedCoworkProfile,
    ResolvedSubagent,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.cowork_profile import (
    desired_cowork_profile_definition,
    render_cowork_profile_json,
    render_cowork_profile_spec,
)
from snowflake_semantic_tools.domain.state import canonical_json

ORIGIN = Origin("cowork_profiles/everyone/cowork_profile.yml", 1, 1)
SALES = ResolvedSubagent(
    QualifiedName.parse("DB.AGENTS.SALES"),
    "Sales",
    "Pipeline and quota.",
    ("What closed last week?",),
    depends_on="agent:sales",
)
PARTNER = ResolvedSubagent(
    QualifiedName.parse("OTHER.SCH.PARTNER"), "Partner", "Partner deals.", external=True, member="partner"
)
# The router entry `SALES` renders: its name is the agent routing runs, `source.fqn`.
SALES_ENTRY = {
    "name": "DB.AGENTS.SALES",
    "display_name": "Sales",
    "description": "Pipeline and quota.",
    "source": {
        "type": "DATA_AGENT",
        "fqn": "DB.AGENTS.SALES",
        "sample_questions": [{"question": "What closed last week?"}],
    },
}


def _profile(**fields: Any) -> CoworkProfileModel:
    return CoworkProfileModel("everyone", ORIGIN, (ORIGIN.file,), **fields)


def test_a_bare_profile_renders_its_model_router_and_subagents_only() -> None:
    resolved = ResolvedCoworkProfile(_profile(), (SALES,))
    assert render_cowork_profile_spec(resolved) == {
        "models": {"orchestration": "auto"},
        "tools": [{"tool_spec": {"type": "agent_router", "name": "router"}}],
        "tool_resources": {"router": {"subagents": [SALES_ENTRY]}},
    }


def test_every_section_renders_in_order_and_passthrough_replaces_its_namesake() -> None:
    model = _profile(
        orchestration_model="claude-4-sonnet",
        orchestration_instructions="Route data questions.",
        response_instructions="Be brief.",
        sample_questions=("Who owns pipeline?",),
        tools=(CoworkProfileTool("web_search", None, ORIGIN), CoworkProfileTool("code_execution", "run", ORIGIN)),
        skills=(AgentSkill("semantics", "CORTEX_EXTENSION", "DB.S.SEMANTICS", "SST_ABC", ref="skill"),),
        passthrough=MappingProxyType({"models": {"orchestration": "override"}, "mcp_servers": []}),
    )
    document = render_cowork_profile_spec(ResolvedCoworkProfile(model, (SALES, PARTNER)))
    assert list(document) == ["models", "instructions", "tools", "tool_resources", "skills", "mcp_servers"]
    assert document["models"] == {"orchestration": "override"}
    assert document["instructions"] == {
        "orchestration": "Route data questions.",
        "response": "Be brief.",
        "sample_questions": [{"question": "Who owns pipeline?"}],
    }
    assert document["tools"] == [
        {"tool_spec": {"type": "agent_router", "name": "router"}},
        {"tool_spec": {"type": "web_search", "name": "web_search", "description": ""}},
        {"tool_spec": {"type": "code_execution", "name": "run", "description": ""}},
    ]
    # A built-in tool has no resources, so the router's subagent list is the only entry.
    assert document["tool_resources"] == {
        "router": {
            "subagents": [
                SALES_ENTRY,
                {
                    "name": "OTHER.SCH.PARTNER",
                    "display_name": "Partner",
                    "description": "Partner deals.",
                    "source": {"type": "DATA_AGENT", "fqn": "OTHER.SCH.PARTNER", "sample_questions": []},
                },
            ]
        }
    }
    assert document["skills"] == [
        {"name": "semantics", "source": {"type": "CORTEX_EXTENSION", "path": "DB.S.SEMANTICS", "version": "SST_ABC"}}
    ]


def test_the_payload_is_the_spec_indented_with_its_keys_in_order_and_a_final_newline() -> None:
    resolved = ResolvedCoworkProfile(_profile(response_instructions="Réponds."), (SALES,))
    payload = render_cowork_profile_json(resolved)
    assert payload.endswith("}\n")
    assert '"response": "Réponds."' in payload
    assert json.loads(payload) == render_cowork_profile_spec(resolved)
    assert payload.startswith('{\n  "models": {\n    "orchestration": "auto"\n  },\n  "instructions"')


def test_the_definition_covers_the_comment_display_alias_tags_enabled_meta_and_spec() -> None:
    model = _profile(
        comment="Everyone's profile.",
        display=AgentProfile("Everyone", "icon", "blue"),
        alias="PROD",
        tags=(("DB.S.OWNER", "data"),),
        meta=MappingProxyType({"team": "analytics"}),
    )
    spec = {"models": {"orchestration": "auto"}}
    expected = {
        "comment": "Everyone's profile.",
        "display": {"display_name": "Everyone", "avatar": "icon", "color": "blue"},
        "alias": "PROD",
        "tags": [["DB.S.OWNER", "data"]],
        "enabled": True,
        "meta": {"team": "analytics"},
        "spec": spec,
    }
    assert desired_cowork_profile_definition(model, spec) == bytes(canonical_json(expected))
    assert desired_cowork_profile_definition(_profile(), spec) != desired_cowork_profile_definition(model, spec)


def test_a_resolved_profile_depends_on_its_agents_then_its_skills_each_once() -> None:
    again = ResolvedSubagent(QualifiedName.parse("DB.AGENTS.SALES_COPY"), "Copy", "Copy.", depends_on="agent:sales")
    resolved = ResolvedCoworkProfile(_profile(), (SALES, PARTNER, again), ("skill:semantics", "plugin:toolkit"))
    assert resolved.depends_on == ("agent:sales", "skill:semantics", "plugin:toolkit")


def test_a_profile_reads_its_display_block_as_the_profile_an_agent_object_carries() -> None:
    display = AgentProfile("Everyone")
    assert _profile(display=display).profile is display
