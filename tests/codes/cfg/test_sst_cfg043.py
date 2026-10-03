"""SST-CFG043: a key the schema records as removed is set."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg043_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"generation": {"publish_via": "ddl"}}) if item.code == "SST-CFG043"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'generation.publish_via' was removed: SST 1.0 renders DDL directly"
    assert diagnostic.subject == "config:generation.publish_via"


def test_sst_cfg043_silent() -> None:
    assert "SST-CFG043" not in [item.code for item in validate_config({"apply": {"fail_fast": True}})]
