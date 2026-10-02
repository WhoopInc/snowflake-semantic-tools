"""SST-CFG044: a key reserved for a later release is set."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg044_fires() -> None:
    [diagnostic] = [item for item in validate_config({"dbt": {}}) if item.code == "SST-CFG044"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'dbt' is not supported in this release"
    assert diagnostic.subject == "config:dbt"


def test_sst_cfg044_silent() -> None:
    assert "SST-CFG044" not in [
        item.code for item in validate_config({"snowflake": {"orchestration_models": ["auto"]}})
    ]
