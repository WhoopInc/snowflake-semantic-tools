"""Check a CoWork profile: on its own, where it publishes, and beside the agents of its project.

CoWork profiles are a Snowflake private preview (PrPr) feature; these checks follow the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

Each check reads only the values it is given: whether a subagent names an agent that exists
is resolution's question, not this module's. A schema is compared as Snowflake resolves it.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.cowork_profile import (
    COWORK_PROFILE_SCHEMA,
    COWORK_PROFILE_TOOLS,
    CoworkProfileModel,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


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


def unlisted_profile(model: CoworkProfileModel, target: QualifiedName) -> tuple[Diagnostic, ...]:
    """Report a profile that publishes where CoWork does not list profiles: a rehearsal of one.

    Diagnostics:
        SST-VAL878: the profile's database and schema are not SNOWFLAKE_INTELLIGENCE.PROFILES.
    """
    if _schema(target) == COWORK_PROFILE_SCHEMA:
        return ()
    schema = f"{target.database.sql}.{target.schema.sql}"
    return (D("SST-VAL878", origin=model.origin, subject=model.key, artifact=model.name, target=schema),)


def schema_problems(
    agents: Sequence[tuple[AgentModel, QualifiedName]], profiles: Sequence[tuple[CoworkProfileModel, QualifiedName]]
) -> tuple[Diagnostic, ...]:
    """Report each agent publishing where CoWork lists profiles, then each profile beside an agent.

    Observation tells the AGENT objects of the two kinds apart by the schema each publishes to,
    so a profile may not share one with an agent; the first agent in its schema is named.

    Args:
        agents, profiles: Each compiled agent, then each compiled profile, with where it
            publishes, in compile order.

    Diagnostics:
        SST-VAL880: an agent publishes to SNOWFLAKE_INTELLIGENCE.PROFILES.
        SST-VAL879: a profile publishes to a schema an agent publishes to.
    """
    found = [
        D("SST-VAL880", origin=model.origin, subject=model.key, artifact=model.name)
        for model, target in agents
        if _schema(target) == COWORK_PROFILE_SCHEMA
    ]
    schemas: dict[tuple[str, str], AgentModel] = {}
    for model, target in agents:
        schemas.setdefault(_schema(target), model)
    for profile, target in profiles:
        agent = schemas.get(_schema(target))
        if agent is not None:
            schema = f"{target.database.sql}.{target.schema.sql}"
            found.append(
                D(
                    "SST-VAL879",
                    origin=profile.origin,
                    subject=profile.key,
                    artifact=profile.name,
                    target=schema,
                    other=agent.name,
                )
            )
    return tuple(found)


def _schema(target: QualifiedName) -> tuple[str, str]:
    return target.database.folded, target.schema.folded
