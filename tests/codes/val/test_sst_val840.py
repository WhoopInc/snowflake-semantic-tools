"""SST-VAL840: a plugin() entry names a skill that is not one of the plugin's members.

Fires when an agent's plugin entry names a member the plugin lacks; the nearest legitimate input
stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val840_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(skill_ref("", "month-close"), skill_ref("other", "finance-kit", ref="plugin"))
        ).diagnostics,
        "SST-VAL840",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith(
        "agent 'router': plugin entry name {{ skill('other') }} must be one of the plugin's members: "
    )
    assert diagnostic.subject == "agent:router"
    assert diagnostic.suggestion == "name a member as {{ skill('<member>') }}, or remove the name to load every member"


def test_sst_val840_silent() -> None:
    assert "SST-VAL840" not in codes(
        compile_agents(agent(skill_ref("", "month-close"), skill_ref("", "finance-kit", ref="plugin"))).diagnostics
    )
