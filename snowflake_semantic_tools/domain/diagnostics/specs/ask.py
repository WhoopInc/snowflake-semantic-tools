"""Unpublished-run codes (ASK): `sst ask`, and evals run against an agent's edit before it publishes.

Both run the agent's compiled specification as a temporary agent, so what they report is
about that run: an agent that answered with an error, a name that compiles no agent, and the
published objects the edited agent still calls.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Unpublished runs"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-ASK001",
        Severity.WARNING,
        "Agent calls the published dependency, not your edit",
        "{artifact} calls the published {value}, not your edit: {detail}",
        "publish it first (`sst plan`, then `sst apply`) to try the two together",
        condition=(
            "a semantic view, tool, skill, plugin or agent the agent's specification names compiles "
            "to a fingerprint the target's state does not record for it, or the state cannot be read"
        ),
        note=(
            "The temporary agent's specification names objects by their published names, so a "
            "tool resolves to what the target holds now: only the agent's own edit is tried."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-ASK002",
        Severity.ERROR,
        "Unpublished agent run returned an error",
        "{artifact} returned an error: {detail}",
        "fix what the message names, then run it again",
        condition="the temporary agent answered a question with an error message instead of a reply",
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-ASK003",
        Severity.ERROR,
        "No such agent",
        "no agent named '{name}' compiles in this project; it compiles {detail}",
        "name an agent `sst list agents` lists",
        condition="`sst ask` names an agent that is not among the agents the project compiles",
    ),
)
