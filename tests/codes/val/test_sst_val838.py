"""SST-VAL838: version set on a project-published extension.

Fires when an agent sets a version on a project-published skill; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val838_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(skill_ref("month-close", "month-close", version="V1"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics,
        "SST-VAL838",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'router': skill source 'month-close' sets version, which SST manages for skill('month-close')"
    )
    assert diagnostic.subject == "agent:router"


def test_sst_val838_silent() -> None:
    assert "SST-VAL838" not in codes(
        compile_agents(
            agent(skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics
    )
