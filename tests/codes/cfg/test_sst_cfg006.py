"""SST-CFG006: a block is present and leaves out a key it requires."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg006_fires() -> None:
    [diagnostic] = [item for item in validate_config({"skills": {"catalog": {}}}) if item.code == "SST-CFG006"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "required config key 'skills.catalog.+bundle_stage' is absent"
    assert diagnostic.subject == "config:skills.catalog.+bundle_stage"


def test_sst_cfg006_silent() -> None:
    assert "SST-CFG006" not in [
        item.code for item in validate_config({"skills": {"catalog": {"+bundle_stage": "BUNDLES"}}})
    ]
