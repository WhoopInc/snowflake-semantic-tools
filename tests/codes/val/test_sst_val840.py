"""SST-VAL840: skill source name does not match its extension.

Fires when an agent names a skill source other than the skill; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val840_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(skill_ref("other", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics,
        "SST-VAL840",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'router': skill source name 'other' must be 'month-close'"
    assert diagnostic.subject == "agent:router"


def test_sst_val840_silent() -> None:
    assert "SST-VAL840" not in codes(
        compile_agents(
            agent(skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics
    )
