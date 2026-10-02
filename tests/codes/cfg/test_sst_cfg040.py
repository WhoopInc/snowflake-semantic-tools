"""SST-CFG040: `vars:` declares `sha_version`, which SST supplies from the commit."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg040_fires() -> None:
    [diagnostic] = [item for item in validate_config({"vars": {"sha_version": "abc"}}) if item.code == "SST-CFG040"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "vars.sha_version is supplied by SST and must not be declared"
    assert diagnostic.subject == "config:vars.sha_version"


def test_sst_cfg040_silent() -> None:
    assert "SST-CFG040" not in [item.code for item in validate_config({"vars": {"region": "eu"}})]
