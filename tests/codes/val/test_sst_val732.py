"""SST-VAL732: unsupported tool types are skipped during evals.

Fires when the agent has an MCP tool an eval run skips; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentTool
from tests.helpers.eval_inputs import (
    AGENT_FILE,
    codes,
    only,
    sales_agent,
    sales_eval,
    validate,
)


def test_sst_val732_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                agent=sales_agent(tools=(*sales_agent().tools, AgentTool("mcp", Origin(AGENT_FILE), name="docs")))
            )
        ),
        "SST-VAL732",
    )
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "eval config for 'sales': mcp will be skipped, not failed"
    assert diagnostic.subject == "eval:sales"


def test_sst_val732_silent() -> None:
    assert "SST-VAL732" not in codes(validate())
