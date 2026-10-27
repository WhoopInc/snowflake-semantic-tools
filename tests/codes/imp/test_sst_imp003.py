"""SST-IMP003: the object carries SST's ownership marker, so it is not imported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, staged_agent

_MARKER = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"


def test_sst_imp003_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.AGENTS.SALES_BOT", comment=f"Sales. {_MARKER}")
    [obj] = run(port, "agent", "DB.AGENTS.SALES_BOT").objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP003", Severity.INFO)
    assert diagnostic.message == "DB.AGENTS.SALES_BOT carries SST's ownership marker; it was not imported"
    assert not obj.files and not obj.refused


def test_sst_imp003_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.AGENTS.SALES_BOT", comment="Sales.")
    assert "SST-IMP003" not in codes(run(port, "agent", "DB.AGENTS.SALES_BOT"))
