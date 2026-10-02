"""SST-PRS024: an agent's passthrough block carries keys SST renders without checking them."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity


def _found(tmp_path: Path, code: str, spec: str) -> list[Diagnostic]:
    folder = tmp_path / "agents" / "sales"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(f"name: sales\nspec:\n{spec}", encoding="utf-8")
    return [item for item in load_agents(tmp_path)[1] if item.code == code]


def test_sst_prs024_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(
        tmp_path, "SST-PRS024", "  tools:\n    - type: data_to_chart\n      tool_spec_passthrough: {x: 1}\n"
    )
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == "agent:sales.tools[0].tool_spec_passthrough: 1 passthrough keys will be rendered unvalidated"
    )
    assert diagnostic.subject == "agent:sales"


def test_sst_prs024_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "SST-PRS024", "  tools:\n    - type: data_to_chart\n") == []
