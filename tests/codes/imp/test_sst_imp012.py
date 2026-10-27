"""SST-IMP012: an imported object names one the project does not compile."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.references import Placed
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import FakeImportPort, codes, index, run, setup, staged_agent


def test_sst_imp012_fires() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    [obj] = run(port, "agent", "DB.S.SALES_BOT").objects
    [diagnostic] = [item for item in obj.diagnostics if item.code == "SST-IMP012"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent:sales_bot names semantic_view DB.SV.SALES, which this project does not compile; "
        "written as {{ semantic_view('sales') }}"
    )
    assert [(item.artifact_type, item.name) for item in obj.dependencies] == [("semantic_view", "DB.SV.SALES")]


def test_sst_imp012_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    sales = Placed("semantic_view", "sales", QualifiedName.parse("DB.SV.SALES"), ())
    assert "SST-IMP012" not in codes(run(port, "agent", "DB.S.SALES_BOT", setup(index(sales))))
