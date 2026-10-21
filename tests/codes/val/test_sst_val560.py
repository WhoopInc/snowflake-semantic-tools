"""SST-VAL560: an agent or CoWork profile leaves its orchestration model on auto."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, cross
from tests.helpers.diagnostic_filters import coded


def _left_on(model: str) -> list[Diagnostic]:
    return coded(cross(compile_agents(agent("sales_agent", orchestration_model=model))), "SST-VAL560")


def test_sst_val560_fires() -> None:
    [diagnostic] = _left_on("auto")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': orchestration model is auto, so Snowflake chooses the model and may change it"
    )
    assert diagnostic.suggestion == (
        "pin a model `snowflake.orchestration_models` lists with `agents.+orchestration_model: "
        "claude-sonnet-4-6` or the agent's `config:`; set `agents.+allow_auto_model: true` to keep "
        "auto deliberately"
    )
    assert diagnostic.subject == "agent:sales_agent"
    # The project's severity is what reports: strict mode never promotes it.
    assert ERROR_REGISTRY["SST-VAL560"].strict_exempt


def test_sst_val560_silent() -> None:
    assert _left_on("claude") == []
