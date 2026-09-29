"""Pure complete-spec renderer for Cortex Agents."""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from ..model.agent import BUILTIN_AGENT_TOOLS, AgentModel, ResolvedAgentTool
from ..state.model import canonical_json


def render_agent_spec(model: AgentModel, tools: tuple[ResolvedAgentTool, ...]) -> dict[str, object]:
    document: dict[str, object] = {"models": {"orchestration": model.orchestration_model}}
    orchestration: dict[str, object] = {}
    budget = {
        key: value
        for key, value in (("seconds", model.budget_seconds), ("tokens", model.budget_tokens))
        if value is not None
    }
    if budget:
        orchestration["budget"] = budget
    if model.tool_not_accessible is not None:
        orchestration["tool_not_accessible"] = model.tool_not_accessible
    if model.analytical_search is not None:
        orchestration["capabilities"] = {"analytical_search": model.analytical_search}
    if orchestration:
        document["orchestration"] = orchestration
    instructions: dict[str, object] = {}
    if model.orchestration_instructions is not None:
        instructions["orchestration"] = model.orchestration_instructions
    if model.response_instructions is not None:
        instructions["response"] = model.response_instructions
    if model.sample_questions:
        instructions["sample_questions"] = [{"question": value} for value in model.sample_questions]
    if instructions:
        document["instructions"] = instructions
    if tools:
        document["tools"] = [_tool_entry(tool) for tool in tools]
        resources = {
            tool.name: dict(tool.resources) for tool in tools if tool.type not in BUILTIN_AGENT_TOOLS and tool.resources
        }
        if resources:
            document["tool_resources"] = resources
    if model.skills:
        document["skills"] = [
            {
                # A plugin reference may omit `name`; its members come from the version.
                **({"name": skill.name} if skill.name else {}),
                "source": {
                    "type": skill.source_type,
                    "path": skill.path,
                    "version": skill.version,
                },
            }
            for skill in model.skills
        ]
    document.update(model.passthrough)
    return document


def render_agent_json(model: AgentModel, tools: tuple[ResolvedAgentTool, ...]) -> str:
    import json

    return json.dumps(render_agent_spec(model, tools), indent=2, ensure_ascii=False) + "\n"


def desired_agent_definition(model: AgentModel, spec: Mapping[str, object]) -> bytes:
    return bytes(
        canonical_json(
            {
                "comment": model.comment,
                "secure": model.secure,
                "profile": {
                    "display_name": model.profile.display_name,
                    "avatar": model.profile.avatar,
                    "color": model.profile.color,
                },
                "alias": model.alias,
                "tags": list(model.tags),
                "enabled": model.enabled,
                "meta": dict(model.meta),
                "spec": dict(spec),
            }
        )
    )


def _tool_entry(tool: ResolvedAgentTool) -> dict[str, object]:
    spec: dict[str, object] = {
        "type": tool.type,
        "name": tool.name,
        "description": tool.description,
    }
    if tool.input_schema:
        spec["input_schema"] = dict(tool.input_schema)
    spec.update(tool.tool_spec_passthrough)
    return {"tool_spec": spec}


EMPTY_MAP: Mapping[str, object] = MappingProxyType({})
