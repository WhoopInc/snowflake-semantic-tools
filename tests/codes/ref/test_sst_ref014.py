"""SST-REF014: an agent's `{{ file() }}` instruction names a sidecar file that does not exist."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity


def _loaded(tmp_path: Path, instructions: str, *sidecars: tuple[str, str]) -> tuple[Diagnostic, ...]:
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


def test_sst_ref014_fires(tmp_path: Path) -> None:
    [diagnostic] = _loaded(tmp_path, "{{ file('route.md') }}")
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF014", Severity.ERROR)
    assert diagnostic.message == "{ file('route.md') } does not resolve to a file"
    assert diagnostic.origin is not None and diagnostic.origin.file == "agents/router/agent.yml"


def test_sst_ref014_silent(tmp_path: Path) -> None:
    assert _loaded(tmp_path, "{{ file('route.md') }}", ("route.md", "Route sales questions.")) == ()
