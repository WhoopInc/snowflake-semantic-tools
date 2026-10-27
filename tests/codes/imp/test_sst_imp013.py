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
    spec["instructions"]["sample_questions"] = [{"question": "Revenue?", "answer": "Lots."}]
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT", spec)
    [obj] = run(port, "agent", "DB.S.SALES_BOT", setup(index(_SALES))).objects
    [diagnostic] = obj.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP013", Severity.WARNING)
    assert diagnostic.message.startswith("agent:sales_bot: a sample question holds keys besides `question`")


def test_an_analyst_tool_named_otherwise_than_its_view_keeps_its_name_and_reports_nothing() -> None:
    spec = copy.deepcopy(ANALYST_SPEC)
    spec["tools"][0]["tool_spec"]["name"] = "query_sales"
    spec["tool_resources"] = {"query_sales": spec["tool_resources"]["SALES"]}
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT", spec)
    [obj] = run(port, "agent", "DB.S.SALES_BOT", setup(index(_SALES))).objects
    assert "SST-IMP013" not in [item.code for item in obj.diagnostics]
    [agent] = [file.document for file in obj.files if file.path == "agents/sales_bot/agent.yml"]
    assert agent is not None
    [tool] = agent["spec"]["tools"]  # type: ignore[index]
    assert tool["name"] == "query_sales"


def test_sst_imp013_silent() -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    assert "SST-IMP013" not in codes(run(port, "agent", "DB.S.SALES_BOT", setup(index(_SALES))))
