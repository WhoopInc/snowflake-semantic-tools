"""What the per-code REF tests share: an agent, a tool relation or instructions, read for one code."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.app.compile.agents import CompileAgents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_context import agent_context


def skill_entry_findings(skill: AgentSkill, code: str) -> list[Diagnostic]:
    """The diagnostics of `code` from compiling agent `router` with one skill entry."""
    agent = AgentModel("router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",), skills=(skill,))
    diagnostics = CompileAgents((agent,), DiagnosticBag(), agent_context()).run_result().diagnostics
    return [item for item in diagnostics if item.code == code]


ORIGIN = Origin("tools/platform.yml")


def procedure_relation_findings(
    relations: dict[str, str], code: str, *, targets: tuple[str, ...] = ("dev", "prod")
) -> list[Diagnostic]:
    """The diagnostics of `code` for a referenced procedure `platform.lookup` with these relations."""
    member = ToolMember(
        "platform",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        ORIGIN,
        "tools/platform.yml",
        relations=MappingProxyType(relations),
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", ORIGIN, "tools/platform.yml", members=(member,)),), "dev", frozenset(targets)
    )
    return [item for item in validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ())) if item.code == code]


def loaded_instructions(tmp_path: Path, instructions: str, *sidecars: tuple[str, str]) -> tuple[Diagnostic, ...]:
    """Load agent `router` whose orchestration instructions are `instructions`, beside `sidecars`."""
    root = tmp_path / "agents" / "router"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        f'name: router\nspec:\n  instructions:\n    orchestration: "{instructions}"\n', encoding="utf-8"
    )
    for name, text in sidecars:
        (root / name).write_text(text, encoding="utf-8")
    _, diagnostics = load_agents(tmp_path)
    return tuple(diagnostics)
