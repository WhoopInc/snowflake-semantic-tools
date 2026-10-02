"""SST-PRS018: an element of a collection, here an agent's `spec.tools`, has the wrong shape."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity


def _found(tmp_path: Path, code: str, spec: str) -> list[Diagnostic]:
    folder = tmp_path / "agents" / "sales"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(f"name: sales\nspec:\n{spec}", encoding="utf-8")
    return [item for item in load_agents(tmp_path)[1] if item.code == code]


def test_sst_prs018_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(tmp_path, "SST-PRS018", "  tools: [1]\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/agent.yml: 'tools'[0] expects mapping, found int"
    assert diagnostic.subject is None


def test_sst_prs018_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "SST-PRS018", "  tools:\n    - type: data_to_chart\n") == []
