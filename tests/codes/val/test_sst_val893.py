"""SST-VAL893: several Cortex Code profiles are listed and none is DEFAULT, so a role seeing them gets none."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded, codes
from tests.helpers.skill_inputs import compile_profiles, profile, profile_catalog


def test_sst_val893_fires() -> None:
    result = compile_profiles(profile_catalog(profile("analyst"), profile("operator")))
    [diagnostic] = coded(result.diagnostics, "SST-VAL893")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "2 coco profiles publish to CORTEX_CODE.CONFIG (ANALYST, OPERATOR) and none is DEFAULT"
    assert (diagnostic.subject, diagnostic.origin) == (None, Origin("coco_profiles/analyst/coco_profile.yml"))


def test_sst_val893_silent() -> None:
    named = compile_profiles(profile_catalog(profile("default"), profile("operator")))
    assert "SST-VAL893" not in codes(named.diagnostics)
    assert "SST-VAL893" not in codes(compile_profiles(profile_catalog()).diagnostics)
