"""SST-IMP006: the scope holds no object of the type, once the patterns apply."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, staged_agent


def test_sst_imp006_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    report = run(port, "agent", "DB.S", like="FINANCE%")
    [diagnostic] = report.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP006", Severity.INFO)
    assert diagnostic.message == "no agent in DB.S whose name is like 'FINANCE%'"


def test_sst_imp006_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP006" not in codes(run(port, "agent", "DB.S", like="SALES%"))
