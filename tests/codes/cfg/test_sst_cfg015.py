"""SST-CFG015: `evals:` declares `+database` or `+schema`, which evals never take."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg015_fires() -> None:
    [diagnostic] = [item for item in validate_config({"evals": {"+schema": "EVALS"}}) if item.code == "SST-CFG015"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "evals: declares +schema, which is structurally invalid"
    assert diagnostic.subject == "config:evals.+schema"


def test_sst_cfg015_silent() -> None:
    assert "SST-CFG015" not in [item.code for item in validate_config({"evals": {"+retry": 1}})]
