"""SST-VAL551: an agent run refuses a model `snowflake.orchestration_models` allows."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import AgentProbe, ProbeOutcome
from tests.helpers.cortex_models import FakeCortexModels, checked, ga

REFUSED = "llama3.1-8b is not an allowed model for Agent requests."


def test_sst_val551_fires() -> None:
    port = FakeCortexModels((ga("llama3.1-8b"),), {"llama3.1-8b": AgentProbe(ProbeOutcome.REJECTED, REFUSED)})
    [diagnostic] = checked(["auto", "llama3.1-8b"], port)
    assert diagnostic.code == "SST-VAL551"
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"orchestration model 'llama3.1-8b' was refused when an agent ran on it in target 'prod': {REFUSED}"
    )
    assert diagnostic.subject == "config:snowflake.orchestration_models"


def test_sst_val551_silent() -> None:
    port = FakeCortexModels((ga("claude-sonnet-4-6"),))
    assert checked(["auto", "claude-sonnet-4-6"], port) == []
