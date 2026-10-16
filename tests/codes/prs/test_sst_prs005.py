"""SST-PRS005: a metric name is not one Snowflake identifier, or a tool or agent name is quoted."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.agent import agent_rules
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import agent, catalog, compile_agents, dbt_catalog, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.prs_codes import NAMED_AGENT, NAMED_MEMBER, named_findings
from tests.helpers.seam_projects import COUNTED, SmallProject, manifest, metric_file, model_node


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


def test_sst_prs005_fires_once_for_an_agent_and_a_tool_member_name(tmp_path: Path) -> None:
    exit_code, found = named_findings(tmp_path, "SST-PRS005", agent="zz-hy", member="zz-stage")
    assert exit_code == 1
    assert [(item["message"], item["location"]["file"], item["location"]["line"]) for item in found] == [
        ("agent:zz-hy: 'zz-hy' is not a valid identifier", NAMED_AGENT, 1),
        ("tool:zz-stage: 'zz-stage' is not a valid identifier", NAMED_MEMBER, 2),
    ]
    assert named_findings(tmp_path / "again", "SST-INT902", agent="zz-hy", member="zz-stage")[1] == []


def test_sst_prs005_fires_for_an_agent_name_over_255_characters(tmp_path: Path) -> None:
    long = "a" * 256
    assert [item["message"] for item in named_findings(tmp_path, "SST-PRS005", agent=long, member="zz_files")[1]] == [
        f"agent:{long}: '{long}' is not a valid identifier"
    ]


def test_sst_prs005_silent_for_an_agent_and_a_tool_member_name(tmp_path: Path) -> None:
    assert named_findings(tmp_path, "SST-PRS005", agent="zz_hy", member="zz_stage") == (0, [])


def test_sst_prs005_fires_for_every_column_name_at_its_dbt_yaml(tmp_path: Path) -> None:
    columns = {
        name: {
            "name": name,
            "description": "A column.",
            "data_type": "VARCHAR",
            "meta": {"sst": {"column_type": "dimension"}},
        }
        for name in ("products_id", "Order Id", "Bad-Col", "9lives")
    }
    node = model_node("products", columns=columns, patch_path="fixture://models/products.yml")
    project = SmallProject(tmp_path, document=manifest({"model.fixture.products": node})).load()
    found = coded(project.diagnostics, "SST-PRS005")
    assert [item.message for item in found] == [
        f"dbt_model:products: '{name}' is not a valid identifier" for name in ("Order Id", "Bad-Col", "9lives")
    ]
    assert {item.origin for item in found} == {Origin("models/products.yml")}
