"""SST-CFG025: the agents' default orchestration model is absent from the allowlist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg025_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"agents": {"+orchestration_model": "claude-x"}}) if item.code == "SST-CFG025"
    ]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "orchestration model 'claude-x' is absent from snowflake.orchestration_models"
    assert diagnostic.subject == "config:agents.+orchestration_model"


def test_sst_cfg025_silent() -> None:
    assert "SST-CFG025" not in [
        item.code
        for item in validate_config(
            {
                "agents": {"+orchestration_model": "claude-x"},
                "snowflake": {"orchestration_models": ["auto", "claude-x"]},
            }
        )
    ]
