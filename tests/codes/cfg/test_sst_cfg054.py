"""SST-CFG054: a setting is written with no value, in an artifact's `config:` or as a `+` key."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.validate.config import validate_config
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

ORIGIN = Origin("agents/sales/agent.yml", 4, 3)


def test_sst_cfg054_fires() -> None:
    [diagnostic] = validate_config({"agents": {"finance": {"+schema": None}}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG054", Severity.ERROR)
    assert diagnostic.message == "'agents.finance.+schema' is set to null"
    assert diagnostic.subject == "config:agents.finance.+schema"
    [own] = item_config_diagnostics("agents", {"secure": None}, subject="agent:sales", origin_of=lambda key: ORIGIN)
    assert own.code == "SST-CFG054"
    assert own.message == "'config.secure' is set to null"
    assert (own.subject, own.origin) == ("agent:sales", ORIGIN)


def test_sst_cfg054_silent() -> None:
    assert validate_config({"agents": {"finance": {"+schema": "FINANCE"}}}) == ()
    found = item_config_diagnostics("agents", {"secure": False}, subject="agent:sales", origin_of=lambda key: ORIGIN)
    assert found == ()
