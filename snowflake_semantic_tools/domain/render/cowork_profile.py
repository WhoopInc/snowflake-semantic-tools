"""Pure renderer for Snowflake CoWork profile specifications, and the definition each fingerprints.

CoWork profiles are a Snowflake private preview (PrPr) feature; the specification follows the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile renders as an agent does -- its instructions, built-in tools, skills, passthrough, and
JSON payload through the agent renderer's own sections -- with one `agent_router` tool first and
that tool's subagent list in `tool_resources`. It decides nothing about which agents resolve.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.agent import ResolvedAgentTool
from snowflake_semantic_tools.domain.model.cowork_profile import (
    ROUTER_NAME,
    CoworkProfileModel,
    ResolvedCoworkProfile,
    ResolvedSubagent,
)
from snowflake_semantic_tools.domain.render.agent import (
    agent_render_checks,
    render_instructions,
    render_skills,
    render_spec_json,
    render_tool_entry,
    render_tool_resources,
)
from snowflake_semantic_tools.domain.state import canonical_json

# The tool type that makes an agent a profile, and the source type of each agent it routes to.
ROUTER_TYPE = "agent_router"
SUBAGENT_SOURCE = "DATA_AGENT"
# The router every profile renders first, as the checks an agent's tools get read it.
_ROUTER_TOOL = ResolvedAgentTool(ROUTER_TYPE, ROUTER_NAME, "")


def render_cowork_profile_spec(resolved: ResolvedCoworkProfile) -> dict[str, object]:
    """Render a CoWork profile's complete specification as a JSON-ready document.

    `models.orchestration`, `tools`, and `tool_resources` are always present: the router tool
    comes first, then each of the profile's built-in tools as an agent's built-in renders, and
    the router's entry holds the subagents in order. Each subagent's `name` is its `source.fqn`,
    the agent routing runs. The instructions and skills appear only when they have content, and
    the `passthrough` keys go in last, so they replace any section of the same name.

    Example:
        A profile routing to the agent `DB.S.SALES`, with no instructions, renders:
            {"models": {"orchestration": "auto"},
             "tools": [{"tool_spec": {"type": "agent_router", "name": "router"}}],
             "tool_resources": {"router": {"subagents": [
                 {"name": "DB.S.SALES", "display_name": "Sales", "description": "Pipeline.",
                  "source": {"type": "DATA_AGENT", "fqn": "DB.S.SALES", "sample_questions": []}}]}}}
    """
    model = resolved.model
    document: dict[str, object] = {"models": {"orchestration": model.orchestration_model}}
    instructions = render_instructions(
        model.orchestration_instructions, model.response_instructions, model.sample_questions
    )
    if instructions:
        document["instructions"] = instructions
    tools = profile_tools(model)
    document["tools"] = [
        {"tool_spec": {"type": ROUTER_TYPE, "name": ROUTER_NAME}},
        *(render_tool_entry(tool) for tool in tools),
    ]
    document["tool_resources"] = {
        ROUTER_NAME: {"subagents": [_subagent_entry(subagent) for subagent in resolved.subagents]},
        **render_tool_resources(tools),
    }
    if model.skills:
        document["skills"] = render_skills(model.skills)
    document.update(model.passthrough)
    return document


def render_cowork_profile_json(resolved: ResolvedCoworkProfile) -> str:
    """Render the specification as the profile's JSON payload, as `render_spec_json` serializes it.

    Raises:
        TypeError: a value, such as one from `passthrough`, is not JSON-encodable.
    """
    return render_spec_json(render_cowork_profile_spec(resolved))


def profile_tools(model: CoworkProfileModel) -> tuple[ResolvedAgentTool, ...]:
    """Return the profile's own tools, each a built-in named after its type unless named, as an agent's is."""
    return tuple(ResolvedAgentTool(tool.type, tool.name or tool.type, "") for tool in model.tools)


def cowork_profile_render_checks(resolved: ResolvedCoworkProfile, payload: str) -> tuple[Diagnostic, ...]:
    """Report what the rendered spec `payload` holds that publication will not check or survive.

    The spec is checked as an agent's is, its router first among its tools, so a profile is
    never reported for having no tools.

    Diagnostics:
        Every code `agent_render_checks` lists.
    """
    return agent_render_checks(resolved.model, (_ROUTER_TOOL, *profile_tools(resolved.model)), payload)


def desired_cowork_profile_definition(model: CoworkProfileModel, spec: Mapping[str, object]) -> bytes:
    """Encode the profile's whole definition as the canonical JSON its fingerprint is a hash of.

    Besides the spec, the definition holds the comment, the display block, alias, tags,
    `enabled`, and `meta`, so a change to any of them changes the fingerprint.

    Raises:
        ValueError: a value is NaN or an infinity.
        TypeError: a value, such as one in `meta`, is not JSON-encodable.
    """
    return bytes(
        canonical_json(
            {
                "comment": model.comment,
                "display": {
                    "display_name": model.display.display_name,
                    "avatar": model.display.avatar,
                    "color": model.display.color,
                },
                "alias": model.alias,
                "tags": list(model.tags),
                "enabled": model.enabled,
                "meta": dict(model.meta),
                "spec": dict(spec),
            }
        )
    )


def _subagent_entry(subagent: ResolvedSubagent) -> dict[str, object]:
    # The routing guide asks for all four fields, though the server routes without them
    # (docs/design/snowflake-behavior.md). The router reads `source.sample_questions` as a
    # list of strings and refuses every run on question objects, unlike an agent's
    # instructions, so these stay plain text.
    fqn = subagent.fqn.sql
    return {
        "name": fqn,
        "display_name": subagent.display_name,
        "description": subagent.description,
        "source": {
            "type": SUBAGENT_SOURCE,
            "fqn": fqn,
            "sample_questions": list(subagent.sample_questions),
        },
    }
