"""SST-CFG004: a configuration value has the wrong type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg004_fires() -> None:
    [diagnostic] = validate_config({"validation": {"strict": "yes"}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG004", Severity.ERROR)
    assert diagnostic.message == "config key 'validation.strict' expects boolean, found str"
    assert diagnostic.subject == "config:validation.strict"


def test_sst_cfg004_silent() -> None:
    assert [item.code for item in validate_config({"validation": {"strict": False}})] == []
