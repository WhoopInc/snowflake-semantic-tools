"""SST-VAL891: a Cortex Code profile publishes outside CORTEX_CODE.CONFIG, where Cortex Code lists none."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded, codes
from tests.helpers.skill_inputs import compile_profiles, profile_catalog


def test_sst_val891_fires() -> None:
    result = compile_profiles(profile_catalog(), schema=("DB", "COCO_PROFILES"))
    [diagnostic] = coded(result.diagnostics, "SST-VAL891")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "coco profile 'analyst' publishes to DB.COCO_PROFILES, where Cortex Code does not list profiles"
    )
    assert diagnostic.subject == "coco_profile:analyst"


def test_sst_val891_silent() -> None:
    # The schema is compared as Snowflake resolves it, so lower case names the same one.
    result = compile_profiles(profile_catalog(), schema=("cortex_code", "config"))
    assert "SST-VAL891" not in codes(result.diagnostics)
