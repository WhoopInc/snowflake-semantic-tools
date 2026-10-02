"""Check an agent's rendered specification as a whole, and its instructions against its tools.

The spec checks read the document `render_agent_spec` returned, after every passthrough key
went in, because that is what replaces the live agent's specification whole.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from difflib import SequenceMatcher
from itertools import combinations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import BUILTIN_AGENT_TOOLS, AgentModel, ResolvedAgentTool

NEAR_DUPLICATE_RATIO = 0.9
# Tool types an instruction may name generically ("the cortex_search tool") without
# naming a tool; a built-in's type is also its name, so it is not among them.
_GENERIC_TYPES = frozenset(("cortex_analyst_text_to_sql", "cortex_search", "generic", "mcp", "agent"))
_NAMED_TOOL = re.compile(
    r"`(?P<quoted>[A-Za-z][A-Za-z0-9_]*)`\s+tool\b|\b(?P<snake>[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9_]+)\s+tool\b|"
    r"\btool\s+[`'\"](?P<after>[A-Za-z][A-Za-z0-9_]*)[`'\"]",
)
_EXCLUSION = re.compile(
    r"\b(?:do\s+not|don't|never)\s+use\s+(?:this\s+tool\s+|it\s+)?(?:for|on)\s+(?P<a>[^.;\n]+)|"
    r"\bnot\s+for\s+(?P<b>[^.;\n]+)",
    re.IGNORECASE,
)
_NEGATION = re.compile(r"\b(?:not|never|don't|avoid)\b", re.IGNORECASE)


def spec_completeness(
    model: AgentModel, tools: Sequence[ResolvedAgentTool], document: Mapping[str, object]
) -> Diagnostic | None:
    """Report the top-level sections the model carries that the rendered document lacks.

    A section is lacking when it is absent or null: either way the replacing specification
    deletes it from the live agent. A passthrough key set to null is how it can happen.

    Diagnostics:
        SST-VAL510: a section the model carries is absent from, or null in, the document.
    """
    expected = ["models"]
    orchestration = (model.budget_seconds, model.budget_tokens, model.tool_not_accessible, model.analytical_search)
    if any(value is not None for value in orchestration):
        expected.append("orchestration")
    if (
        model.orchestration_instructions is not None
        or model.response_instructions is not None
        or model.sample_questions
    ):
        expected.append("instructions")
    if tools:
        expected.append("tools")
    if any(tool.type not in BUILTIN_AGENT_TOOLS and tool.resources for tool in tools):
        expected.append("tool_resources")
    if model.skills:
        expected.append("skills")
    missing = [section for section in expected if document.get(section) is None]
    if not missing:
        return None
    return D("SST-VAL510", artifact=model.name, value=", ".join(missing), subject=model.key)


def resource_keys(model: AgentModel, document: Mapping[str, object]) -> list[Diagnostic]:
    """Check each `tool_resources` key against the rendered tools it must name.

    Diagnostics:
        SST-VAL529: a key names a built-in tool, which takes no resources.
        SST-VAL530: a key names no rendered `tools[].tool_spec.name`.
    """
    resources = document.get("tool_resources")
    rendered = document.get("tools")
    types: dict[str, object] = {}
    for entry in rendered if isinstance(rendered, list) else []:
        spec = entry.get("tool_spec") if isinstance(entry, Mapping) else None
        if isinstance(spec, Mapping) and isinstance(spec.get("name"), str):
            types[str(spec["name"])] = spec.get("type")
    found: list[Diagnostic] = []
    for key in resources if isinstance(resources, Mapping) else {}:
        if key not in types:
            found.append(D("SST-VAL530", artifact=model.name, key=key, subject=model.key))
        elif types[key] in BUILTIN_AGENT_TOOLS:
            found.append(D("SST-VAL529", artifact=model.name, name=key, subject=model.key))
    return found


def near_duplicate_descriptions(model: AgentModel, tools: Sequence[ResolvedAgentTool]) -> list[Diagnostic]:
    """Report each pair of tools whose descriptions read nearly the same, in tool order.

    Descriptions compare casefolded with whitespace collapsed; two at or above
    `NEAR_DUPLICATE_RATIO` similar are near-duplicates. An empty description is SST-VAL518's.

    Diagnostics:
        SST-VAL519: two tools' descriptions are near-identical.
    """
    described = [(tool, " ".join(tool.description.casefold().split())) for tool in tools if tool.description.strip()]
    return [
        D("SST-VAL519", artifact=model.name, a=first.name, b=second.name, subject=model.key)
        for (first, ours), (second, theirs) in combinations(described, 2)
        if SequenceMatcher(None, ours, theirs).ratio() >= NEAR_DUPLICATE_RATIO
    ]


def instruction_problems(model: AgentModel, tools: Sequence[ResolvedAgentTool]) -> list[Diagnostic]:
    """Check the agent's instructions against the tools it has.

    A tool is named in prose as a backticked name or a snake_case name before the word
    "tool", or a quoted name after it; names compare ignoring case. A tool's description
    excludes a topic with "do not use for <topic>" or "not for <topic>"; an instruction
    sentence routes it there when it names the tool and the topic and negates neither.

    Diagnostics:
        SST-VAL535: the instructions name a tool the agent does not have, once per name.
        SST-VAL536: an instruction sentence routes a topic to a tool whose description excludes it.
    """
    text = "\n".join(value for value in (model.orchestration_instructions, model.response_instructions) if value)
    names = {tool.name.casefold() for tool in tools}
    found: list[Diagnostic] = []
    for named in dict.fromkeys(_named_tools(text)):
        if named.casefold() not in names and named.casefold() not in _GENERIC_TYPES:
            found.append(D("SST-VAL535", artifact=model.name, name=named, subject=model.key))
    sentences = [sentence for sentence in re.split(r"[.;\n]", text) if sentence.strip()]
    for tool in tools:
        for topic in _exclusions(tool.description):
            if any(_routes(sentence, tool.name, topic) for sentence in sentences):
                found.append(D("SST-VAL536", artifact=model.name, value=topic, name=tool.name, subject=model.key))
    return found


def _named_tools(text: str) -> list[str]:
    return [next(value for value in match.groupdict().values() if value) for match in _NAMED_TOOL.finditer(text)]


def _exclusions(description: str) -> list[str]:
    return [(match.group("a") or match.group("b")).strip() for match in _EXCLUSION.finditer(description)]


def _routes(sentence: str, tool: str, topic: str) -> bool:
    words = _words(sentence)
    return f" {_words(tool)} " in f" {words} " and _words(topic) in words and not _NEGATION.search(sentence)


def _words(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9_]+", text.casefold()))
