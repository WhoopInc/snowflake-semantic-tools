"""SST-VAL814: bundle carries a script with no consumer that can run it.

Fires when an agent pins a plugin with a script and no agent pinning it runs code; the nearest
legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val814_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics,
        "SST-VAL814",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "skill 'month-close': 'close.py' is executable and no consuming agent enables code_execution"
    )
    assert diagnostic.subject == "plugin:finance-kit"


def test_sst_val814_silent() -> None:
    assert "SST-VAL814" not in codes(
        compile_agents(
            agent(
                skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"), code_execution=True
            )
        ).diagnostics
    )
