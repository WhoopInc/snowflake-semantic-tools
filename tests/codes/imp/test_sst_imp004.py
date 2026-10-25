"""SST-IMP004: a file the import would write is already in the project."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, setup, staged_agent


def test_sst_imp004_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    report = run(port, "agent", "DB.S.SALES_BOT", setup(None, None, "agents/sales_bot/agent.yml"))
    [obj] = report.objects
    [diagnostic] = [item for item in obj.diagnostics if item.code == "SST-IMP004"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales_bot/agent.yml already exists; DB.S.SALES_BOT was not imported"
    assert obj.refused and report.files == ()


def test_sst_imp004_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP004" not in codes(run(port, "agent", "DB.S.SALES_BOT"))
