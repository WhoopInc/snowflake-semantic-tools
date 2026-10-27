"""SST-CFG200: the deprecated `deploy:` is set; it is read as `apply:`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg200_fires() -> None:
    [diagnostic] = [item for item in validate_config({"deploy": {"fail_fast": True}}) if item.code == "SST-CFG200"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "config key 'deploy' is deprecated; use 'apply'"
    assert diagnostic.subject == "config:deploy"


def test_sst_cfg200_silent() -> None:
    assert "SST-CFG200" not in [item.code for item in validate_config({"apply": {"fail_fast": True}})]
