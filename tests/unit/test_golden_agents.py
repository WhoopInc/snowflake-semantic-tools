from __future__ import annotations

from pathlib import Path

import yaml

from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, CompileAgents, CompiledAgent, ExtensionPin
from snowflake_semantic_tools.app.compile.agents.settled import settled_agent
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from tests.helpers.projects import project_paths

ROOT = Path(__file__).parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def test_reference_agents_match_complete_json_goldens() -> None:
    config = yaml.safe_load((FIXTURE / "sst_config.yml").read_text(encoding="utf-8"))
    target = TargetIdentity(
        "dev", "", Identifier.parse("SST_REF_DEV"), Identifier.parse("JAFFLE"), warehouse="SST_REF_WH"
    )
    loaded, diagnostics = load_agents(FIXTURE, config=config)
    # Each agent's settings resolve through its `config:` and the `agents:` block, as compile applies them.
    models = tuple(settled_agent(model, target)[0] for model in loaded)
    tool_catalog = YamlProjectSource(
        project_paths(FIXTURE),
        target_name="dev",
        manifest_path=MANIFEST,
        invoke_dbt=False,
    ).load_tools()
    # Pins come from the compiled skills, exactly as the CLI resolves them.
    skills = CompileSkills(
        load_skill_catalog(FIXTURE, skills_dir="skills", plugins_dir="plugins"),
        CatalogChannel(
            "SST_REF_DEV",
            "JAFFLE",
            QualifiedName.parse("SST_REF_DEV.JAFFLE.SKILL_BUNDLE_SRC"),
            version_prefix=str(config["skills"]["+version_prefix"]),
        ),
    ).run_result()
    pins = {
        item.name: ExtensionPin(item.artifact_key, item.release.target, item.release.alias, (item.name,))
        for item in skills.compiled
        if isinstance(item, CompiledExtension) and item.artifact_type == "skill"
    }
    agent_targets = {
        model.name.casefold(): QualifiedName.from_parts("SST_TEST", "REFERENCE", model.name) for model in models
    }
    result = CompileAgents(
        models,
        diagnostics,
        AgentCompileContext(
            semantic_views={
                "jaffle_sales": QualifiedName.parse("SST_REF_DEV.JAFFLE.JAFFLE_SALES"),
                "jaffle_menu": QualifiedName.parse("SST_REF_DEV.CORE.JAFFLE_MENU"),
                "jaffle_minimal": QualifiedName.parse("SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL"),
            },
            tools=tool_catalog,
            agents=agent_targets,
            # `skills.extensions`: default_prefix plus the UPPER_SNAKE name.
            extensions={"partner-glossary": QualifiedName.parse("SST_REF_DEV.PARTNER.PARTNER_GLOSSARY")},
            variables={"sha_version": "0000000"},
            database="SST_REF_DEV",
            schema="JAFFLE",
            # No agent sets a warehouse, so its tools name none; a generic tool takes the target's.
            warehouse=None,
            target_warehouse="SST_REF_WH",
            query_timeout=int(config["agents"]["+query_timeout"]),
            allowed_models=frozenset(config["snowflake"]["orchestration_models"]),
            skills=pins,
        ),
    ).run_result()
    assert not result.diagnostics.has_errors
    by_name = {compiled.name: compiled for compiled in result.compiled if isinstance(compiled, CompiledAgent)}
    for name in ("jaffle_analytics_agent", "jaffle_delivery_agent", "jaffle_minimal_agent"):
        expected = ROOT / "tests" / "golden" / "expected" / "agent" / f"{name}.json"
        assert by_name[name].payload == expected.read_text(encoding="utf-8").rstrip() + "\n", name
