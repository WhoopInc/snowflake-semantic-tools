"""`agents:` folder routes as compiling a project resolves them, one key at a time."""

from __future__ import annotations

import json
from typing import Any

from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from tests.helpers.compile_builders import view
from tests.helpers.project_inputs import InMemoryProjectInputs

MODELS = ["auto", "block-model", "route-model", "own-model"]
TREE: dict[str, object] = {
    "agents": {
        "+schema": "BLOCK",
        "+orchestration_model": "block-model",
        "+warehouse": "BLOCK_WH",
        "+alias": "block_alias",
        "+tags": [{"name": "TIER", "value": "block"}],
        "finance": {
            "+schema": "FIN",
            "+orchestration_model": "route-model",
            "+budget_seconds": 30,
            "restricted": {"+schema": "FIN_SECURE", "+secure": True, "+warehouse": "SECURE_WH"},
        },
        "retired": {"+enabled": False},
    },
    "snowflake": {"orchestration_models": MODELS},
}


def _agent(name: str, *folder: str, **fields: Any) -> AgentModel:
    file = "/".join(("agents", *folder, "agent.yml"))
    analyst = AgentTool("cortex_analyst_text_to_sql", Origin(file), description="Sales.", semantic_view="SALES")
    return AgentModel(name, Origin(file), (file,), folder=folder, tools=(analyst,), **fields)


def _compiled(*models: AgentModel) -> dict[str, CompiledAgent]:
    inputs = InMemoryProjectInputs(tree=TREE, views=SemanticViewProject((view("SALES"),)), agent_models=models)
    result = CompileProject(inputs).run()
    return {item.name: item for item in result.compiled if isinstance(item, CompiledAgent)}


def _spec(agent: CompiledAgent) -> dict[str, Any]:
    spec: dict[str, Any] = json.loads(agent.payload)
    return spec


def _warehouse(agent: CompiledAgent) -> object:
    [resources] = _spec(agent)["tool_resources"].values()
    return resources["execution_environment"]["warehouse"]


def test_the_closest_route_wins_per_key_and_the_agent_file_wins_over_every_route() -> None:
    agents = _compiled(
        _agent("top", "top"),
        _agent("fin", "finance", "fin"),
        _agent("sec", "finance", "restricted", "sec"),
        _agent("own", "finance", "restricted", "own", orchestration_model="own-model", secure=False, alias="mine"),
        _agent("gone", "retired", "gone"),
    )

    assert sorted(agents) == ["fin", "own", "sec", "top"]
    targets = {name: item.rendered_artifact.target.sql for name, item in agents.items()}
    assert targets == {
        "top": "DB.BLOCK.TOP",
        "fin": "DB.FIN.FIN",
        "sec": "DB.FIN_SECURE.SEC",
        "own": "DB.FIN_SECURE.OWN",
    }

    # The block alone.
    top = agents["top"].resolved.model
    assert (top.orchestration_model, top.budget_seconds, top.secure, top.alias) == (
        "block-model",
        None,
        False,
        "block_alias",
    )
    assert _warehouse(agents["top"]) == "BLOCK_WH"
    # A route overrides the keys it sets and inherits the rest from the block.
    fin = agents["fin"].resolved.model
    assert (fin.orchestration_model, fin.budget_seconds, fin.secure, fin.tags) == (
        "route-model",
        30,
        False,
        (("TIER", "block"),),
    )
    # A nested route folds over its parent, which it does not reset.
    sec = agents["sec"].resolved.model
    assert (sec.orchestration_model, sec.budget_seconds, sec.secure) == ("route-model", 30, True)
    assert _warehouse(agents["sec"]) == "SECURE_WH"
    # What the agent sets itself wins over every route; its location still comes from them.
    own = agents["own"].resolved.model
    assert (own.orchestration_model, own.secure, own.alias, own.budget_seconds) == ("own-model", False, "mine", 30)
