"""Validation codes 5xx (VAL): Cortex Agents.

The rendered spec's size, its tools (names, descriptions, resources, and inputs), skill
sources, orchestration settings, and display names.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL511",
        Severity.ERROR,
        "Rendered agent spec exceeds the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over the 100,000 limit",
        "trim the spec or move bulk context out of it",
    ),
    spec(
        "SST-VAL512",
        Severity.WARNING,
        "Rendered agent spec is near the size limit",
        "agent '{artifact}': rendered spec is {size} bytes, over 80% of the limit",
        "trim the specification before it reaches the hard limit",
    ),
    spec(
        "SST-VAL513",
        Severity.ERROR,
        "Resolved agent tool name is invalid",
        "agent '{artifact}': resolved tool name '{name}' is {size} chars",
        "use a 1-64 character name",
    ),
    spec(
        "SST-VAL514",
        Severity.ERROR,
        "Agent tool names collide",
        "agent '{artifact}': tool name '{name}' is declared twice",
        "rename one tool",
    ),
    spec(
        "SST-VAL515",
        Severity.WARNING,
        "Agent tool names differ only by case",
        "agent '{artifact}': '{a}' and '{b}' differ only by case",
        "rename one tool",
    ),
    spec(
        "SST-VAL517",
        Severity.ERROR,
        "Web search tool has a non-canonical name",
        "agent '{artifact}': web_search tool is named '{name}'",
        "name it web_search",
    ),
    spec(
        "SST-VAL518",
        Severity.ERROR,
        "Agent tool has no description",
        "agent '{artifact}': tool '{name}' has no description",
        "write a disambiguating tool description",
    ),
    spec(
        "SST-VAL520",
        Severity.ERROR,
        "Analyst tool has an invalid semantic-view declaration",
        "agent '{artifact}': tool '{name}' declares {count} semantic views",
        "declare exactly one semantic_view and omit name",
    ),
    spec(
        "SST-VAL521",
        Severity.ERROR,
        "Agent tool omits a required resource",
        "agent '{artifact}': tool '{name}' omits '{field}'",
        "declare the required resource reference",
    ),
    spec(
        "SST-VAL526",
        Severity.ERROR,
        "Generic tool has no object input schema",
        "agent '{artifact}': tool '{name}' input_schema is {found}",
        "declare input_schema with type object",
    ),
    spec(
        "SST-VAL527",
        Severity.ERROR,
        "Generic tool has no warehouse",
        "agent '{artifact}': tool '{name}' declares no warehouse",
        "declare or inherit a warehouse",
    ),
    spec(
        "SST-VAL528",
        Severity.WARNING,
        "Agent toolset makes the tool surface non-static",
        "agent '{artifact}' declares an agent toolset",
        "account for delegated tools in tests",
    ),
    spec(
        "SST-VAL538",
        Severity.ERROR,
        "Skill source is not pinned",
        "agent '{artifact}': skill source '{name}' does not pin an immutable version",
        "pin a committed extension version",
    ),
    spec(
        "SST-VAL539",
        Severity.ERROR,
        "Skill source uses a mutable stage",
        "agent '{artifact}': skill source '{name}' is a STAGE path into a mutable bundle",
        "reference a versioned Cortex Extension",
    ),
    spec(
        "SST-VAL540",
        Severity.ERROR,
        "SKILL-type extension source has no name",
        "agent '{artifact}': the skill source for skill('{path}') omits name",
        "declare name; it is optional only for a plugin",
    ),
    spec(
        "SST-VAL543",
        Severity.ERROR,
        "Orchestration model is not allowed",
        "agent '{artifact}': models.orchestration '{found}' is not in the allowlist",
        "use an allowed model, or extend the allowlist",
    ),
    spec(
        "SST-VAL545",
        Severity.ERROR,
        "tool_not_accessible is invalid",
        "agent '{artifact}': tool_not_accessible {detail}",
        "use accept, reject, or legacy",
    ),
    spec(
        "SST-VAL546",
        Severity.ERROR,
        "Analytical search has no Cortex Search tool",
        "agent '{artifact}': analytical_search is true and no cortex_search tool is declared",
        "declare a Cortex Search tool, or disable the capability",
    ),
    spec(
        "SST-VAL549",
        Severity.WARNING,
        "Agent display name collides",
        "agent '{artifact}': display_name '{value}' is shared with {other}",
        "give each agent a distinct display name",
    ),
)
