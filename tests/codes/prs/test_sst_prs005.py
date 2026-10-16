"""SST-PRS005: a metric name is not one Snowflake identifier, or a tool or agent name is quoted."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.agent import agent_rules
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import agent, catalog, compile_agents, dbt_catalog, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import COUNTED, SmallProject, metric_file


def test_sst_prs005_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=metric_file(COUNTED, name="total count")).load().diagnostics,
        "SST-PRS005",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total count: 'total count' is not a valid identifier"
    assert diagnostic.subject == "metric:total count"


def test_sst_prs005_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=metric_file(COUNTED)).load().diagnostics, "SST-PRS005") == []


def test_sst_prs005_fires_for_a_quoted_tool_member_name() -> None:
    stage = ToolMember('"MixedStage"', "stage", ToolOwnership.DEFINE, Origin("tools.yml", 2), "tools.yml")
    [diagnostic] = coded(validate_tool_catalog(catalog(stage), dbt_catalog()), "SST-PRS005")
    assert diagnostic.message == """tool:"mixedstage": '"MixedStage"' is not a valid identifier"""
    assert diagnostic.origin == Origin("tools.yml", 2)


def test_sst_prs005_fires_for_a_quoted_agent_name() -> None:
    [diagnostic] = coded(tuple(agent_rules(agent('"MixedCase"'), frozenset(("auto",)), ())), "SST-PRS005")
    assert diagnostic.message == """agent:"mixedcase": '"MixedCase"' is not a valid identifier"""
    # Compile refuses the agent, so plan never renders its stage path.
    result = compile_agents(agent('"MixedCase"'))
    assert result.compiled == () and "SST-PRS005" in [item.code for item in result.diagnostics]


def test_sst_prs005_silent_for_unquoted_tool_and_agent_names() -> None:
    stage = ToolMember("zz$stage", "stage", ToolOwnership.DEFINE, Origin("tools.yml"), "tools.yml")
    referenced = procedure_member('"Theirs"', relations={"dev": "DB.S.THEIRS"})
    assert coded(validate_tool_catalog(catalog(stage, referenced), dbt_catalog()), "SST-PRS005") == []
    assert coded(tuple(agent_rules(agent("sales_agent"), frozenset(("auto",)), ())), "SST-PRS005") == []
