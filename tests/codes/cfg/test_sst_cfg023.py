"""SST-CFG023: `semantic_views.+max_staleness` is below the 120 seconds Snowflake accepts."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg023_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"semantic_views": {"+max_staleness": 60}}) if item.code == "SST-CFG023"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "+max_staleness is 60; the minimum is 120"
    assert diagnostic.subject == "config:semantic_views.+max_staleness"


def test_sst_cfg023_silent() -> None:
    assert "SST-CFG023" not in [
        item.code
        for item in validate_config({"semantic_views": {"+max_staleness": 120, "finance": {"+max_staleness": 600}}})
    ]
