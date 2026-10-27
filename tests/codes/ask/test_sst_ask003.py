"""SST-ASK003: `sst ask` names an agent the project does not compile.

Fires for a name no compiled agent has, listing those that compile; the agent's own name stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.ask import find_agent
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.ask_builders import compiled_agent


def test_sst_ask003_fires() -> None:
    diagnostic = find_agent(CompileResult((compiled_agent("sales"),)), "finance")
    assert isinstance(diagnostic, Diagnostic)
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK003", Severity.ERROR)
    assert diagnostic.message == "no agent named 'finance' compiles in this project; it compiles sales"


def test_sst_ask003_silent() -> None:
    assert not isinstance(find_agent(CompileResult((compiled_agent("sales"),)), "sales"), Diagnostic)
