"""SST-IMP007: the object is what one of the project's artifacts publishes, so it is not imported."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.references import Placed
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import FakeImportPort, codes, index, run, setup, staged_agent

_BOT = Placed("agent", "sales_bot", QualifiedName.parse("DB.S.SALES_BOT"), ())


def test_sst_imp007_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    report = run(port, "agent", "DB.S.SALES_BOT", setup(index(_BOT)))
    [obj] = report.objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP007", Severity.INFO)
    assert diagnostic.message == "DB.S.SALES_BOT is what the project's agent:sales_bot publishes; it was not imported"
    assert not obj.refused and not report.has_errors


def test_sst_imp007_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP007" not in codes(run(port, "agent", "DB.S.SALES_BOT"))
