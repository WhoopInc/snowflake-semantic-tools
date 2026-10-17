"""SST-ASK002: the temporary agent answered a question with an error message.

Fires for a reply carrying Snowflake's error; a reply with an answer stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.ask import AskAgent
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply
from tests.helpers.ask_builders import ScriptedAgentRun, compiled_agent

SCOPE = SchemaScope(Identifier.parse("DB"), Identifier.parse("AGENTS"))


def test_sst_ask002_fires() -> None:
    port = ScriptedAgentRun({"q": AgentReply(error="Agent model 'm' is not available")})
    [diagnostic] = AskAgent(port).run(compiled_agent("sales"), "q", scope=SCOPE).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK002", Severity.ERROR)
    assert diagnostic.message == "agent:sales returned an error: Agent model 'm' is not available"
    assert diagnostic.subject == "agent:sales"


def test_sst_ask002_silent() -> None:
    assert AskAgent(ScriptedAgentRun()).run(compiled_agent("sales"), "q", scope=SCOPE).diagnostics == ()
