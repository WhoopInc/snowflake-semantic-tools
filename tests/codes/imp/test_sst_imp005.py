"""SST-IMP005: Snowflake refuses one of the reads an import of the object makes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.import_fakes import FakeImportPort, codes, run, staged_agent


def test_sst_imp005_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    port.refuse("observe_agent_versions")
    [obj] = run(port, "agent", "DB.S.SALES_BOT").objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP005", Severity.ERROR)
    assert diagnostic.message == (
        "role ANALYST cannot read agent DB.S.SALES_BOT (SHOW VERSIONS IN AGENT): observe_agent_versions refused"
    )


def test_sst_imp005_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP005" not in codes(run(port, "agent", "DB.S.SALES_BOT"))
