"""SST-CFG007: an unknown top-level key is a near miss of a declared one."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.config_schema import validate_config


def test_sst_cfg007_fires() -> None:
    [diagnostic] = validate_config({"validaton": {"strict": True}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG007", Severity.ERROR)
    assert diagnostic.message == "unknown top-level key 'validaton'; did you mean 'validation'?"
    assert diagnostic.subject == "config:validaton"


def test_sst_cfg007_silent() -> None:
    # A key nowhere near a declared one is merely unknown (SST-CFG003), never a near miss.
    assert [item.code for item in validate_config({"zzqqxxw": 1})] == ["SST-CFG003"]
    assert [item.code for item in validate_config({"validation": {"strict": True}})] == []
