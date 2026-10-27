"""SST-IMP002: an object named alone is of the other agent kind."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, staged_agent

_ROUTER = {
    "models": {"orchestration": "claude-sonnet-4-6"},
    "tools": [{"tool_spec": {"type": "agent_router", "name": "router"}}],
    "tool_resources": {"router": {"subagents": []}},
}


def test_sst_imp002_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.PROFILES.EVERYONE", _ROUTER)
    [obj] = run(port, "agent", "DB.PROFILES.EVERYONE").objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP002", Severity.ERROR)
    assert diagnostic.message == "DB.PROFILES.EVERYONE is a cowork_profile, not a agent"


def test_sst_imp002_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.PROFILES.EVERYONE", _ROUTER)
    assert "SST-IMP002" not in codes(run(port, "cowork_profile", "DB.PROFILES.EVERYONE"))
    # Over a schema, an object of the other kind is passed over, not refused.
    report = run(port, "agent", "DB.PROFILES")
    assert "SST-IMP002" not in codes(report) and [name.sql for name in report.skipped] == ["DB.PROFILES.EVERYONE"]
