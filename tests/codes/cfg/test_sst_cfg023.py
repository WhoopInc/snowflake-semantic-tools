"""SST-CFG023: `max_staleness` is below the 120 seconds Snowflake accepts, as a `+` key or a view's own."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config
from tests.helpers.reference_project import VIEWS, edited, reported

STALENESS = "      max_staleness: 300\n"


def test_sst_cfg023_fires() -> None:
    [diagnostic] = [
        item for item in validate_config({"semantic_views": {"+max_staleness": 60}}) if item.code == "SST-CFG023"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_views.+max_staleness is 60; the minimum is 120"
    assert diagnostic.subject == "config:semantic_views.+max_staleness"


def test_sst_cfg023_fires_for_a_views_own_setting(tmp_path: Path) -> None:
    [diagnostic] = reported(edited(tmp_path, VIEWS, STALENESS, "      max_staleness: 60\n"), "SST-CFG023")
    assert diagnostic.message == "config.max_staleness is 60; the minimum is 120"
    assert diagnostic.subject == "semantic_view:jaffle_sales"
    assert diagnostic.origin is not None and diagnostic.origin.file == VIEWS


def test_sst_cfg023_silent(tmp_path: Path) -> None:
    assert "SST-CFG023" not in [
        item.code
        for item in validate_config({"semantic_views": {"+max_staleness": 120, "finance": {"+max_staleness": 600}}})
    ]
    assert reported(edited(tmp_path, VIEWS, STALENESS, "      max_staleness: 120\n"), "SST-CFG023") == []
