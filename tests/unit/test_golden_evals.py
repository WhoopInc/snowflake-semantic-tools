from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "reference_project"
EXPECTED = ROOT / "golden" / "expected" / "eval"


def test_reference_eval_matches_exact_source_sql_and_repeat_yaml_goldens() -> None:
    agents, diagnostics = load_agents(FIXTURE)
    source = YamlProjectSource(
        FIXTURE,
        target_name="dev",
        manifest_path=FIXTURE / "target" / "manifest.json",
        invoke_dbt=False,
    )
    catalog = source.load_evals(
        agents,
        diagnostics,
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
    rendered = result.compiled[0].rendered
    assert rendered.source_table_sql == (EXPECTED / "jaffle_analytics_source.sql").read_text(encoding="utf-8") + "\n"
    assert rendered.config_yaml == (EXPECTED / "jaffle_analytics_agent_repeat.yaml").read_text(encoding="utf-8")
    assert rendered.dataset_fingerprint == "dce1f8a88134aee78594fd507dd7679774bea07445fd2b68ab73975e80847b23"
    assert rendered.config_fingerprint == "8ce8141990e1ba00084364615426716bce2f76e3d73b0ffe29d2029bc9002564"
