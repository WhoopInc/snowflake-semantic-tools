"""SST-IMP001: the object named to import is not found, or the role cannot see it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, staged_agent


def test_sst_imp001_fires() -> None:
    [obj] = run(FakeImportPort(), "agent", "DB.AGENTS.MISSING").objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP001", Severity.ERROR)
    assert diagnostic.message == "agent DB.AGENTS.MISSING is not found, or role ANALYST cannot see it"
    assert diagnostic.subject == "agent:missing" and obj.refused


def test_sst_imp001_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.AGENTS.SALES_BOT")
    assert "SST-IMP001" not in codes(run(port, "agent", "DB.AGENTS.SALES_BOT"))
