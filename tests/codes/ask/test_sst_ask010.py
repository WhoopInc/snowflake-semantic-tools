"""SST-ASK010: a run that rejects inaccessible tools was refused for one the agent cannot reach."""

from __future__ import annotations

from snowflake_semantic_tools.app.ask_conversation import turn_diagnostics
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.events import RunError
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_ask010_fires() -> None:
    refused = RunError("TOOL_NOT_ACCESSIBLE: SALES (cortex_analyst_text_to_sql) - does not exist", "399569", "r-1")
    [diagnostic] = turn_diagnostics("agent:sales", Turn(error=refused))
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK010", Severity.ERROR)
    assert diagnostic.message == (
        "agent:sales cannot reach a tool: TOOL_NOT_ACCESSIBLE: SALES (cortex_analyst_text_to_sql) - does not exist "
        "(request r-1)"
    )


def test_sst_ask010_silent() -> None:
    assert [item.code for item in turn_diagnostics("agent:sales", Turn(error=RunError("other", "399504")))] == [
        "SST-ASK002"
    ]
