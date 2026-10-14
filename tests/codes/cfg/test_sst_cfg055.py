"""SST-CFG055: a root-only `+` key is set in a folder route or in an artifact's `config:`.

No key the schema ships is root-only yet, so each test marks `agents.+alias` root-only in the
key table the checks read.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.config_schema import keys
from snowflake_semantic_tools.domain.validate import config as config_checks
from snowflake_semantic_tools.domain.validate.config import validate_config
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

ORIGIN = Origin("agents/sales/agent.yml", 4, 3)


@pytest.fixture(autouse=True)
def _alias_is_root_only(monkeypatch: pytest.MonkeyPatch) -> None:
    agents = dict(keys.CHILDREN["agents"])
    agents["+alias"] = replace(agents["+alias"], root_only=True)
    children = MappingProxyType({**keys.CHILDREN, "agents": MappingProxyType(agents)})
    monkeypatch.setattr(config_checks, "CHILDREN", children)
    monkeypatch.setattr("snowflake_semantic_tools.domain.model.config_schema.settings.CHILDREN", children)


def test_sst_cfg055_fires() -> None:
    [diagnostic] = validate_config({"agents": {"ops": {"+alias": "promoted"}}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG055", Severity.ERROR)
    assert diagnostic.message == "'agents.ops.+alias' can be set only at the root of agents"
    assert diagnostic.subject == "config:agents.ops.+alias"
    assert diagnostic.suggestion == "set agents.+alias once, at the root of agents, and remove it here"
    [own] = item_config_diagnostics(
        "agents", {"alias": "promoted"}, subject="agent:sales", origin_of=lambda key: ORIGIN
    )
    assert own.code == "SST-CFG055"
    assert (own.subject, own.origin) == ("agent:sales", ORIGIN)


def test_sst_cfg055_silent() -> None:
    assert validate_config({"agents": {"+alias": "promoted", "ops": {"+schema": "OPS"}}}) == ()
    found = item_config_diagnostics("agents", {"schema": "OPS"}, subject="agent:sales", origin_of=lambda key: ORIGIN)
    assert found == ()
