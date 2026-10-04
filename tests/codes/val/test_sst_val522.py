"""SST-VAL522: half of the document-preview pair is declared."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, search_member, search_tool
from tests.helpers.diagnostic_filters import coded


def test_sst_val522_fires() -> None:
    model = agent("sales_agent", search_tool(stage_path="@DB.S.DOCS"))
    [diagnostic] = coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL522")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tool 'docs' declares 'stage_path' without 'relative_path_column'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val522_silent() -> None:
    model = agent("sales_agent", search_tool(stage_path="@DB.S.DOCS", relative_path_column="FILE_PATH"))
    assert coded(compile_agents(model, tools=catalog(search_member())).diagnostics, "SST-VAL522") == []
