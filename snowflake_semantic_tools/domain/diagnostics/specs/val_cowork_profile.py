"""Validation codes 870-889 (VAL): Snowflake CoWork profiles.

CoWork profiles are a Snowflake private preview (PrPr) feature; these codes follow the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile's subagent list and the tools it carries beside its router, the agents it routes to,
and where it publishes beside the project's agents.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL870",
        Severity.INFO,
        "CoWork profile relies on a Snowflake private preview",
        "cowork profile '{artifact}' relies on CoWork profiles, a Snowflake private preview",
        "expect the profile contract to change before general availability, and recompile after each SST upgrade",
        condition="a CoWork profile compiles; reported once per compiled profile",
        note=(
            "Snowflake private preview: "
            "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles . "
            "SST renders the profile to the preview's contract as measured, the agent_router tool and "
            "its subagent list included, and Snowflake may change that contract before general "
            "availability."
        ),
    ),
    spec(
        "SST-VAL871",
        Severity.ERROR,
        "CoWork profile names no subagents",
        "cowork profile '{artifact}': spec.subagents names no agent",
        "name at least one agent under spec.subagents",
        condition=(
            "an enabled CoWork profile's `spec.subagents` is missing or empty, or none of its entries can be read"
        ),
        note=(
            "Snowflake private preview. Snowflake accepts such a profile, but a profile with no "
            "subagent list fails every run with \"agent_router tool 'router' is missing a "
            'corresponding entry in tool_resources", and an empty list routes to no agent; '
            "measured on a preview account."
        ),
    ),
    spec(
        "SST-VAL872",
        Severity.ERROR,
        "CoWork profile subagent tool is not an agent",
        "cowork profile '{artifact}': spec.subagents[{index}] tool('{name}') is a {found} member, not an agent",
        "name an `agent` tool member with tool(), or an agent this project publishes with agent()",
        condition=(
            "a CoWork profile's `spec.subagents` entry calls `tool()` on a declared tool member whose type is not agent"
        ),
        note=(
            "Snowflake private preview. The router hands a question to the agent a subagent's "
            "source.fqn names, so a subagent must be an agent."
        ),
    ),
    spec(
        "SST-VAL873",
        Severity.ERROR,
        "CoWork profile names a subagent twice",
        "cowork profile '{artifact}': spec.subagents[{index}] names {function}('{name}') again",
        "remove the repeated entry from spec.subagents",
        condition=(
            "two `spec.subagents` entries of a CoWork profile reference the same function and name, ignoring "
            "case, or resolve to the same agent"
        ),
        note=(
            "Loading compares the call each entry writes, `agent()` or `tool()`, and the name it gives; "
            "compiling then compares the agents the entries resolve to, as Snowflake resolves the name. "
            "The later entry is reported."
        ),
    ),
    spec(
        "SST-VAL874",
        Severity.ERROR,
        "CoWork profile subagent is not described",
        "cowork profile '{artifact}': spec.subagents[{index}] {function}('{name}') has no {field}",
        "write the subagent's display_name and description, or give the agent it names a comment",
        condition=(
            "a CoWork profile's subagent has no display name, or no description, after its defaults: an "
            "`agent()` subagent takes the agent's display name, else its name, and the agent's comment; a "
            "`tool()` subagent, which names an agent SST does not publish, takes none"
        ),
        note=(
            "Snowflake private preview. A subagent entry without all of name, display_name, "
            "description, and source makes the profile drop its whole subagent list, with no error, "
            "and the router chooses between agents by their descriptions. "
            "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing"
        ),
    ),
    spec(
        "SST-VAL875",
        Severity.ERROR,
        "CoWork profile routes to a deprecated agent",
        "cowork profile '{artifact}': spec.subagents[{index}] agent('{name}') is deprecated",
        "route to the agent that replaces it, or stop deprecating the agent",
        condition="a CoWork profile's `agent()` subagent names a project agent marked `deprecated`",
    ),
    spec(
        "SST-VAL876",
        Severity.ERROR,
        "CoWork profile tool is not one a profile carries",
        "cowork profile '{artifact}': spec.tools[{index}] has type '{type}', not web_search or code_execution",
        "keep only web_search or code_execution on the profile, and give the tool to an agent it routes to",
        condition="a `spec.tools` entry of a CoWork profile has a type other than web_search or code_execution",
        note=(
            "Snowflake private preview. A profile routes data questions to its agents; analyst, "
            "search, and chart tools belong on an agent. SST renders the profile's agent_router "
            "tool itself, so `spec.tools` cannot declare one either. "
            "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing"
        ),
    ),
    spec(
        "SST-VAL878",
        Severity.INFO,
        "CoWork profile publishes where CoWork does not list it",
        "cowork profile '{artifact}' publishes to {target}, where CoWork does not list profiles",
        "publish to SNOWFLAKE_INTELLIGENCE.PROFILES on the target CoWork users reach",
        condition=(
            "a compiled CoWork profile's database and schema, compared as Snowflake resolves them, are not "
            "SNOWFLAKE_INTELLIGENCE.PROFILES"
        ),
        note=(
            "Snowflake private preview. The profile routes when it is run directly, but CoWork lists only "
            "the agents in SNOWFLAKE_INTELLIGENCE.PROFILES as profiles, so elsewhere it is a rehearsal, "
            "such as one published by a dev target."
        ),
    ),
    spec(
        "SST-VAL879",
        Severity.ERROR,
        "Agent and CoWork profile publish to one schema",
        "cowork profile '{artifact}' publishes to {target}, where agent '{other}' publishes too",
        "publish CoWork profiles and agents to different schemas",
        condition=(
            "a project agent and a CoWork profile resolve to the same database and schema, compared as "
            "Snowflake resolves them; reported once per profile, naming the first such agent"
        ),
        note=(
            "Both kinds publish an AGENT object, and SST tells which kind owns an observed AGENT by the "
            "schema it is in, so in one schema each would be read as the other's."
        ),
    ),
    spec(
        "SST-VAL880",
        Severity.WARNING,
        "Agent publishes where CoWork lists profiles",
        "agent '{artifact}' publishes to SNOWFLAKE_INTELLIGENCE.PROFILES, where CoWork lists every agent as a profile",
        "publish the agent to another schema, and route to it from a CoWork profile",
        condition=(
            "a project agent's database and schema, compared as Snowflake resolves them, are "
            "SNOWFLAKE_INTELLIGENCE.PROFILES"
        ),
        note=(
            "Snowflake private preview. CoWork treats every agent in that schema as a profile. "
            "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles"
        ),
    ),
)
