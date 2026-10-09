"""SST-PLN035: apply creates the absent stage an agent's specification is staged on."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_stage import STAGE, plan_agents, staged_agent
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_pln035_fires() -> None:
    planned = plan_agents(FakeSnowflake(), staged_agent("a"), staged_agent("b"))
    [diagnostic] = coded(planned.diagnostics, "SST-PLN035")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "stage DB.S.AGENT_SPECS does not exist; apply creates it, server-side encrypted, "
        "before it stages an agent there"
    )
    assert diagnostic.subject == "agent:a"
    assert not planned.blocked


def test_sst_pln035_silent() -> None:
    # An absent stage the role cannot create is not announced: the plan blocks its agent instead.
    port = FakeSnowflake()
    port.preflight.lacking["DB.S"] = ("CREATE STAGE",)
    planned = plan_agents(port, staged_agent("a"))
    assert coded(planned.diagnostics, "SST-PLN035") == []
    assert [item.code for item in planned.blocked[0].diagnostics] == ["SST-PLN008"]
    # A stage that already exists is used as it is.
    present = FakeSnowflake()
    present.stage_types[STAGE.sql] = "INTERNAL NO CSE"
    assert coded(plan_agents(present, staged_agent("a")).diagnostics, "SST-PLN035") == []
