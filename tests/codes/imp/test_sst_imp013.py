"""SST-IMP013: the files publish a property otherwise than the live object holds it."""

from __future__ import annotations

import copy

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.importing.references import Placed
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import ANALYST_SPEC, FakeImportPort, codes, index, run, setup, staged_agent

_SALES = Placed("semantic_view", "sales", QualifiedName.parse("DB.SV.SALES"), ())


def test_sst_imp013_fires() -> None:
    spec = copy.deepcopy(ANALYST_SPEC)
    spec["tools"][0]["tool_spec"]["name"] = "query_sales"
    spec["tool_resources"] = {"query_sales": spec["tool_resources"]["SALES"]}
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT", spec)
    [obj] = run(port, "agent", "DB.S.SALES_BOT", setup(index(_SALES))).objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP013", Severity.WARNING)
    assert diagnostic.message.startswith("agent:sales_bot: Cortex Analyst tool 'query_sales' is named after its view")


def test_sst_imp013_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP013" not in codes(run(port, "agent", "DB.S.SALES_BOT", setup(index(_SALES))))
