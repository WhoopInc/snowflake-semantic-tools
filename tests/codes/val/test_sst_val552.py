"""SST-VAL552: an allowed orchestration model is INTERNAL, or absent from SHOW CORTEX BASE MODELS."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import AgentProbe, CortexBaseModel, ProbeOutcome
from tests.helpers.cortex_models import FakeCortexModels, checked, ga


def test_sst_val552_fires() -> None:
    port = FakeCortexModels((CortexBaseModel("kimi-k3", "INTERNAL"),))
    found = checked(["kimi-k3", "glm-5.3"], port)
    assert [item.code for item in found] == ["SST-VAL552", "SST-VAL552"]
    assert found[0].severity is Severity.WARNING
    assert found[0].message == (
        "orchestration model 'kimi-k3' is not offered to customers: SHOW CORTEX BASE MODELS marks it INTERNAL"
    )
    assert found[1].message == (
        "orchestration model 'glm-5.3' is not offered to customers: it is not in SHOW CORTEX BASE MODELS"
    )


def test_sst_val552_silent() -> None:
    assert checked(["claude-sonnet-4-6"], FakeCortexModels((ga("claude-sonnet-4-6"),))) == []


def test_sst_val552_silent_when_an_agent_run_refuses_the_model() -> None:
    port = FakeCortexModels((), {"not-a-model": AgentProbe(ProbeOutcome.REJECTED, "not authorized")})
    assert [item.code for item in checked(["not-a-model"], port)] == ["SST-VAL551"]
