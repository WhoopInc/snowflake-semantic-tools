"""SST-VAL856: agent references an extension that cannot publish.

Fires when an agent references a declared plugin that has errors; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import agent, compile_agents, skill_ref


def test_sst_val856_fires() -> None:
    diagnostic = only(
        compile_agents(
            agent(
                skill_ref("month-close", "month-close"),
                skill_ref("", "finance-kit", ref="plugin"),
                skill_ref("", "ghost-kit", ref="plugin"),
            ),
            unpublished={"plugin:ghost-kit": "it has errors"},
        ).diagnostics,
        "SST-VAL856",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'router': plugin('ghost-kit') has no version to pin because it has errors"
    assert diagnostic.subject == "agent:router"


def test_sst_val856_silent() -> None:
    assert "SST-VAL856" not in codes(
        compile_agents(
            agent(skill_ref("month-close", "month-close"), skill_ref("", "finance-kit", ref="plugin"))
        ).diagnostics
    )
