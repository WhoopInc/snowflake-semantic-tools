"""SST-CFG008: a value has the right type and is outside its allowed domain."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg008_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"enrichment": {"distinct_limit": 0}}) if item.code == "SST-CFG008"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'enrichment.distinct_limit' value 0 is outside 1..1000"
    assert diagnostic.subject == "config:enrichment.distinct_limit"


def test_sst_cfg008_silent() -> None:
    assert "SST-CFG008" not in [item.code for item in validate_config({"enrichment": {"distinct_limit": 25}})]
