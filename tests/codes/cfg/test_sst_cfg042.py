"""SST-CFG042: a block sets a key it no longer takes, because another block now holds its settings."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg042_fires() -> None:
    [diagnostic] = validate_config({"skills": {"stage": {"+stage": "PROFILES"}}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG042", Severity.ERROR)
    assert (
        diagnostic.message
        == "skills: 'stage' is no longer read here; Desktop profiles publish through coco_desktop_profiles:"
    )
    assert diagnostic.subject == "config:skills.stage"


def test_sst_cfg042_silent() -> None:
    assert validate_config({"skills": {"+bundle_stage": "B"}, "coco_desktop_profiles": {"+stage": "PROFILES"}}) == ()
