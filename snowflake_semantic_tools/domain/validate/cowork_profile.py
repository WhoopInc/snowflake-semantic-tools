"""Check a CoWork profile on its own: the subagents it routes to and the tools it carries.

CoWork profiles are a Snowflake private preview (PrPr) feature; these checks follow the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

Each check reads only the profile it is given: whether a subagent names an agent that exists
is resolution's question, not this module's.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.cowork_profile import COWORK_PROFILE_TOOLS, CoworkProfileModel


def cowork_profile_diagnostics(model: CoworkProfileModel) -> tuple[Diagnostic, ...]:
    """Check a loaded profile's subagent list, then its tools, each in authored order.

    Subagents are compared by the function their `agent:` calls and the name it gives, ignoring
    case, so `agent('Sales')` repeats `agent('sales')` but not `tool('sales')`.

    Diagnostics:
        SST-VAL871: the profile names no subagent, or none that could be read.
        SST-VAL873: a subagent repeats an earlier one; each repeat is reported.
        SST-VAL876: a tool's type is not one a profile carries beside its router.
    """
    found: list[Diagnostic] = []
    if not model.subagents:
        found.append(D("SST-VAL871", origin=model.origin, subject=model.key, artifact=model.name))
    seen: set[tuple[str, str]] = set()
    for index, subagent in enumerate(model.subagents):
        identity = (subagent.reference, subagent.name.casefold())
        if identity in seen:
            found.append(
                D(
                    "SST-VAL873",
                    origin=subagent.origin,
                    subject=model.key,
                    artifact=model.name,
                    index=index,
                    function=subagent.reference,
                    name=subagent.name,
                )
            )
        seen.add(identity)
    for index, tool in enumerate(model.tools):
        if tool.type not in COWORK_PROFILE_TOOLS:
            found.append(
                D("SST-VAL876", origin=tool.origin, subject=model.key, artifact=model.name, index=index, type=tool.type)
            )
    return tuple(found)
