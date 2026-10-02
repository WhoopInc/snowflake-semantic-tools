"""SST-PRS118: an agent's sample question is a bare string rather than a mapping."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity


def _found(tmp_path: Path, code: str, spec: str) -> list[Diagnostic]:
    folder = tmp_path / "agents" / "sales"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(f"name: sales\nspec:\n{spec}", encoding="utf-8")
    return [item for item in load_agents(tmp_path)[1] if item.code == code]


def test_sst_prs118_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(tmp_path, "SST-PRS118", "  instructions:\n    sample_questions:\n      - What sold\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent:sales: sample_questions[0] is a string, expected a mapping"
    assert diagnostic.subject == "agent:sales"


def test_sst_prs118_silent(tmp_path: Path) -> None:
    spec = "  instructions:\n    sample_questions:\n      - question: What sold?\n"
    assert _found(tmp_path, "SST-PRS118", spec) == []
