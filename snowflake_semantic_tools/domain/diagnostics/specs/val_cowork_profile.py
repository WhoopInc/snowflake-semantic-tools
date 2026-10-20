"""Validation codes 870-889 (VAL): Snowflake CoWork profiles.

CoWork profiles are a Snowflake private preview (PrPr) feature; these codes follow the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile's subagent list and the tools it carries beside its router.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
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
        "SST-VAL873",
        Severity.ERROR,
        "CoWork profile names a subagent twice",
        "cowork profile '{artifact}': spec.subagents[{index}] names {function}('{name}') again",
        "remove the repeated entry from spec.subagents",
        condition=(
            "two `spec.subagents` entries of a CoWork profile reference the same function and name, ignoring case"
        ),
        note=(
            "Entries are compared by the call each writes, `agent()` or `tool()`, and the name it "
            "gives; the later entry is reported."
        ),
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
)
