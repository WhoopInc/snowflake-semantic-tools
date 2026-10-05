"""SST-CFG041: a `semantic_views:` or `agents:` folder route names no directory under its root."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.semantic.target import _folder_route_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_cfg041_fires(tmp_path: Path) -> None:
    [diagnostic] = _folder_route_diagnostics({"semantic_views": {"finance": {"+schema": "FIN"}}}, tmp_path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG041", Severity.ERROR)
    assert diagnostic.message == f"config key 'finance' in block 'semantic_views' names no directory under '{tmp_path}'"
    assert diagnostic.subject == "config_route:finance"


def test_sst_cfg041_silent(tmp_path: Path) -> None:
    (tmp_path / "finance").mkdir()
    assert (
        _folder_route_diagnostics({"semantic_views": {"finance": {"+schema": "FIN"}, "+schema": "X"}}, tmp_path) == ()
    )


def test_sst_cfg041_fires_for_an_agents_route(tmp_path: Path) -> None:
    (tmp_path / "agents" / "finance").mkdir(parents=True)
    _, diagnostics = load_agents(tmp_path, config={"agents": {"finance": {}, "sales": {"+schema": "S"}}})
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG041", Severity.ERROR)
    assert (
        diagnostic.message == f"config key 'sales' in block 'agents' names no directory under '{tmp_path / 'agents'}'"
    )
    assert diagnostic.subject == "config_route:sales"
