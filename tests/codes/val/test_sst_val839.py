"""SST-VAL839: skill version pinned to the commit SHA.

Fires when an agent pins a skill to var('sha_version'); the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val839_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(
                skill_ref("month-close", "month-close", var="sha_version"), skill_ref("", "finance-kit", ref="plugin")
            )
        ).diagnostics,
        "SST-VAL839",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'router': skill source 'month-close' pins var('sha_version'), which names no published version"
    )
    assert diagnostic.subject == "agent:router"


def test_sst_val839_silent() -> None:
    assert "SST-VAL839" not in codes(
        compile_agents(
            agent(skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics
    )
