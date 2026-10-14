"""SST-CFG041: a folder route of any routed block names no directory under its root."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.config_schema import route_root
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY

ROUTED = tuple(name for name, artifact in ARTIFACT_REGISTRY.artifacts.items() if artifact.routed_block)


def test_every_artifact_type_routes_by_folder() -> None:
    assert ROUTED == ("semantic_view", "tool", "skill", "plugin", "profile", "agent", "eval")


@pytest.mark.parametrize("name", ROUTED)
def test_sst_cfg041_fires(tmp_path: Path, name: str) -> None:
    artifact = ARTIFACT_REGISTRY.artifacts[name]
    root = tmp_path / route_root({}, artifact)
    [diagnostic] = routed_diagnostics({artifact.routed_block: {"finance": {"+schema": "FIN"}}}, tmp_path, name)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG041", Severity.ERROR)
    assert (
        diagnostic.message
        == f"config key 'finance' in block '{artifact.routed_block}' names no directory under '{root}'"
    )
    assert diagnostic.subject == "config_route:finance"


@pytest.mark.parametrize("name", ROUTED)
def test_sst_cfg041_silent(tmp_path: Path, name: str) -> None:
    artifact = ARTIFACT_REGISTRY.artifacts[name]
    (tmp_path / route_root({}, artifact) / "finance").mkdir(parents=True)
    config = {artifact.routed_block: {"finance": {"+schema": "FIN"}, "+schema": "X"}}
    assert routed_diagnostics(config, tmp_path, name) == ()


def test_sst_cfg041_fires_below_a_route_whose_directory_exists(tmp_path: Path) -> None:
    (tmp_path / "skills" / "finance").mkdir(parents=True)
    config = {"skills": {"finance": {"close": {"+certified": True}}}}
    [diagnostic] = routed_diagnostics(config, tmp_path, "skill")
    assert diagnostic.subject == "config_route:finance.close"


def test_a_key_the_block_declares_is_no_route(tmp_path: Path) -> None:
    # `skills.catalog` is a removed key, which SST-CFG042 reports; it names no folder.
    assert routed_diagnostics({"skills": {"catalog": {"+bundle_stage": "B"}}}, tmp_path, "skill") == ()


def test_sst_cfg041_fires_for_an_agents_route(tmp_path: Path) -> None:
    (tmp_path / "agents" / "finance").mkdir(parents=True)
    _, diagnostics = load_agents(tmp_path, config={"agents": {"finance": {}, "sales": {"+schema": "S"}}})
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG041", Severity.ERROR)
    assert (
        diagnostic.message == f"config key 'sales' in block 'agents' names no directory under '{tmp_path / 'agents'}'"
    )
    assert diagnostic.subject == "config_route:sales"
