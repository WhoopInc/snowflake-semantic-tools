"""SST-ASK001: an unpublished run calls a dependency the target does not hold as it compiles.

Fires for an agent whose semantic view state records at another fingerprint; the same view
recorded as compiled stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.ask import stale_dependencies
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ask_builders import applied_entry, compiled_agent


def test_sst_ask001_fires() -> None:
    helper = compiled_agent("helper")
    agent = compiled_agent("sales", helper.artifact_key)
    stale = {helper.artifact_key: applied_entry(helper, fingerprint="0" * 64)}
    [diagnostic] = stale_dependencies(agent, CompileResult((agent, helper)), stale)
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK001", Severity.WARNING)
    assert diagnostic.message == (
        "agent:sales calls the published agent:helper (DB.AGENTS.HELPER), not your edit: "
        "what is published differs from what compiles now"
    )
    assert diagnostic.subject == "agent:sales"


def test_sst_ask001_silent() -> None:
    helper = compiled_agent("helper")
    agent = compiled_agent("sales", helper.artifact_key)
    assert stale_dependencies(agent, CompileResult((agent, helper)), {helper.artifact_key: applied_entry(helper)}) == ()
