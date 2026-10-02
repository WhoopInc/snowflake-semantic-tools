"""SST-CFG042: `evals:` or `skills:` declares a folder route."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg042_fires() -> None:
    [diagnostic] = [item for item in validate_config({"evals": {"finance": {}}}) if item.code == "SST-CFG042"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "evals: declares a folder route 'finance'"
    assert diagnostic.subject == "config:evals.finance"


def test_sst_cfg042_silent() -> None:
    assert "SST-CFG042" not in [item.code for item in validate_config({"evals": {"+retry": 1}})]
