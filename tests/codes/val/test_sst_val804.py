"""SST-VAL804: extension version referenced by no agent.

Fires when a published plugin is referenced by no agent; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val804_fires() -> None:
    diagnostic = only(compile_agents(agent(skill_ref("month-close", "month-close"))).diagnostics, "SST-VAL804")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "plugin:finance-kit: version SST_0123456789AB is referenced by no agent"
    assert diagnostic.subject == "plugin:finance-kit"


def test_sst_val804_silent() -> None:
    assert "SST-VAL804" not in codes(
        compile_agents(
            agent(
                skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"), code_execution=True
            )
        ).diagnostics
    )
