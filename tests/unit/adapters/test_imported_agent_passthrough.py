"""An imported agent whose spec holds keys SST does not model loads, and renders that spec again.

`sst import` keeps what SST does not model in `spec.passthrough`, at whatever depth it sits; the
loader merges passthrough into what SST renders, so the file loads with no SST-PRS023 and the
rendered spec carries every key the live one did.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.dump import dump_yaml
from snowflake_semantic_tools.domain.importing.agent import AgentPlacement, LiveAgent, import_agent
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.agent import render_agent_spec
from tests.helpers.file_trees import write_tree
from tests.helpers.import_fakes import index

LIVE = {
    "models": {"orchestration": "auto", "fallback": "other"},
    "orchestration": {"budget": {"seconds": 5, "steps": 3}, "planner": "fast"},
    "instructions": {"response": "Hi.", "system": "Be terse."},
    "thinking_effort": "high",
}


def test_an_imported_agent_with_unmodelled_keys_loads_and_renders_its_live_spec(tmp_path: Path) -> None:
    placement = AgentPlacement("agents", "tools", Destination(("sales",)), {})
    imported = import_agent(LiveAgent(QualifiedName.parse("DB.S.BOT"), None, {}, None, LIVE), index(), placement)
    write_tree(tmp_path, {file.path: dump_yaml(file.document) for file in imported.files if file.document is not None})
    (agent,), diagnostics = load_agents(tmp_path)
    assert [item.code for item in diagnostics if item.severity.name == "ERROR"] == []
    # Settings resolve at compile, as the model and budget are set here; the rest is passthrough.
    assert {key: setting.value for key, setting in agent.settings.items()} == {
        "orchestration_model": "auto",
        "budget_seconds": 5,
    }
    model = replace(agent, orchestration_model="auto", budget_seconds=5)
    assert render_agent_spec(model, ()) == LIVE
