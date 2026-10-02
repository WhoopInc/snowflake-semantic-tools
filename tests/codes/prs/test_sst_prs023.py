"""SST-PRS023: an agent's passthrough block names a key SST renders itself."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity


def _found(tmp_path: Path, code: str, spec: str) -> list[Diagnostic]:
    folder = tmp_path / "agents" / "sales"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(f"name: sales\nspec:\n{spec}", encoding="utf-8")
    return [item for item in load_agents(tmp_path)[1] if item.code == code]


def test_sst_prs023_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(tmp_path, "SST-PRS023", "  passthrough:\n    models: {orchestration: x}\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "sales.spec: passthrough key 'models' is already rendered by SST"
    assert diagnostic.subject == "agent:sales"


def test_sst_prs023_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "SST-PRS023", "  passthrough:\n    experimental: {}\n") == []
