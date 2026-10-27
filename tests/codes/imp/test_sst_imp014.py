"""SST-IMP014: no folder routes to the live location, so the file names its own."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, setup, staged_agent


def test_sst_imp014_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.AGENTS.SALES_BOT")
    [obj] = run(port, "agent", "DB.AGENTS.SALES_BOT").objects
    [diagnostic] = [item for item in obj.diagnostics if item.code == "SST-IMP014"]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "agent:sales_bot: no agents folder places it in DB.AGENTS for target dev; its `config:` names the location"
    )
    document = obj.files[0].document or {}
    assert document["config"] == {"database": "DB", "schema": "AGENTS", "orchestration_model": "claude-sonnet-4-6"}


def test_sst_imp014_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.AGENTS.SALES_BOT")
    routed = setup(None, {"agents": {"sales": {"+schema": "AGENTS"}}})
    report = run(port, "agent", "DB.AGENTS.SALES_BOT", routed)
    assert "SST-IMP014" not in codes(report)
    assert [file.path for file in report.files][0] == "agents/sales/sales_bot/agent.yml"
