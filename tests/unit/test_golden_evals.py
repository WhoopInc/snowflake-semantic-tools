from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.compile_builders import compiled_as
from tests.helpers.goldens import compile_twice
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy, with_profile_eval

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "reference_project"
EXPECTED = ROOT / "golden" / "expected" / "eval"


def test_reference_eval_matches_exact_source_sql_and_repeat_yaml_goldens() -> None:
    agents, diagnostics = load_agents(FIXTURE)
    assert not diagnostics.has_errors
    source = YamlProjectSource(
        project_paths(FIXTURE),
        target_name="dev",
        manifest_path=FIXTURE / "target" / "manifest.json",
        invoke_dbt=False,
    )
    catalog = source.load_evals(
        agents,
        {
            "jaffle_analytics_agent": (
                "JAFFLE_MENU",
                "JAFFLE_SALES",
                "menu_docs_search",
                "order_tier_lookup",
            )
        },
    )
    result = CompileEvals(
        catalog,
        agent_targets={"jaffle_analytics_agent": QualifiedName.parse("SST_REF_DEV.JAFFLE.JAFFLE_ANALYTICS_AGENT")},
    ).run_result()
    assert not result.diagnostics.has_errors
    assert len(result.compiled) == 1
    rendered = compiled_as(result, CompiledEval).rendered
    assert rendered.source_table_sql == (EXPECTED / "jaffle_analytics_source.sql").read_text(encoding="utf-8") + "\n"
    assert rendered.config_yaml == (EXPECTED / "jaffle_analytics_agent_repeat.yaml").read_text(encoding="utf-8")
    assert rendered.dataset_fingerprint == "dce1f8a88134aee78594fd507dd7679774bea07445fd2b68ab73975e80847b23"
    assert rendered.config_fingerprint == "8ce8141990e1ba00084364615426716bce2f76e3d73b0ffe29d2029bc9002564"


@pytest.fixture(scope="module")
def profile_evals(tmp_path_factory: pytest.TempPathFactory) -> dict[str, CompiledEval]:
    """The reference project with its CoWork profile's eval, compiled; each eval by its key."""
    project = with_profile_eval(project_copy(tmp_path_factory.mktemp("profile_eval"), offline=False))
    result = compile_twice(project, DBT_MANIFEST).result
    assert result.success
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledEval)}


def test_a_cowork_profile_eval_matches_its_source_sql_and_config_goldens(
    profile_evals: dict[str, CompiledEval],
) -> None:
    rendered = profile_evals["eval:cowork_profile:jaffle_concierge"].rendered
    assert rendered.source_table_sql == (EXPECTED / "jaffle_concierge_source.sql").read_text(encoding="utf-8")
    assert rendered.config_yaml == (EXPECTED / "jaffle_concierge_repeat.yaml").read_text(encoding="utf-8")
    # The routing metric scores in SST, so the config Snowflake runs never names it.
    assert "routing_accuracy" not in rendered.config_yaml


def test_a_cowork_profile_eval_leaves_the_agent_eval_beside_it_unchanged(
    profile_evals: dict[str, CompiledEval],
) -> None:
    agent_eval = profile_evals["eval:jaffle_analytics_agent"].rendered
    assert agent_eval.dataset_fingerprint == "dce1f8a88134aee78594fd507dd7679774bea07445fd2b68ab73975e80847b23"
    assert agent_eval.config_fingerprint == "8ce8141990e1ba00084364615426716bce2f76e3d73b0ffe29d2029bc9002564"
