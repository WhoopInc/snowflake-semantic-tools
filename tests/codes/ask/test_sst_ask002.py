"""SST-ASK002: an unpublished run of an agent failed, or ended other than completed or cancelled.

Fires for a turn that answered Snowflake's error, or timed out; a completed or cancelled turn stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.ask_conversation import turn_diagnostics
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.events import RunError
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_ask002_fires() -> None:
    [diagnostic] = turn_diagnostics("agent:sales", Turn(error=RunError("Agent model 'm' is not available")))
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK002", Severity.ERROR)
    assert diagnostic.message == "agent:sales returned an error: Agent model 'm' is not available"
    assert diagnostic.subject == "agent:sales"
    assert [item.code for item in turn_diagnostics("agent:sales", Turn(status="timed_out"))] == ["SST-ASK002"]


def test_sst_ask002_silent() -> None:
    assert turn_diagnostics("agent:sales", Turn(status="completed")) == ()
    assert turn_diagnostics("agent:sales", Turn(status="cancelled")) == ()
