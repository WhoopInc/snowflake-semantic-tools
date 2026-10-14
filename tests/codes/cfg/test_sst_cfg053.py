"""SST-CFG053: an artifact's `config:` block sets a key that is not one of its block's settings."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

ORIGIN = Origin("agents/sales/agent.yml", 3, 3)


def _codes(config: object) -> list[str]:
    found = item_config_diagnostics("agents", config, subject="agent:sales", origin_of=lambda key: ORIGIN)
    return [item.code for item in found]


def test_sst_cfg053_fires() -> None:
    [diagnostic] = item_config_diagnostics("agents", {"budget": 5}, subject="agent:sales", origin_of=lambda key: ORIGIN)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG053", Severity.ERROR)
    assert diagnostic.message == "'config.budget' is not a setting of agents"
    assert diagnostic.subject == "agent:sales"
    assert diagnostic.origin == ORIGIN
    assert "budget_seconds" in str(diagnostic.suggestion)
    # The `+` form belongs in sst_config.yml, not under `config:`.
    assert _codes({"+schema": "S"}) == ["SST-CFG053"]


def test_sst_cfg053_silent() -> None:
    assert _codes({"budget_seconds": 5, "schema": "S", "tags": []}) == []
