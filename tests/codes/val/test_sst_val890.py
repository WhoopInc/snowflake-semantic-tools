"""SST-VAL890: a Cortex Code profile compiles, relying on a Snowflake private preview."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded, codes
from tests.helpers.skill_inputs import compile_profiles, profile, profile_catalog


def test_sst_val890_fires() -> None:
    [diagnostic] = coded(compile_profiles(profile_catalog()).diagnostics, "SST-VAL890")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "coco profile 'analyst' relies on Cortex Code agent profiles, a Snowflake private preview"
    )
    assert (diagnostic.subject, diagnostic.origin) == (
        "coco_profile:analyst",
        Origin("coco_profiles/analyst/coco_profile.yml", 1),
    )


def test_sst_val890_silent() -> None:
    # A disabled profile does not compile, and one that does not compile relies on nothing.
    assert "SST-VAL890" not in codes(compile_profiles(profile_catalog(profile(enabled=False))).diagnostics)
    assert "SST-VAL890" not in codes(compile_profiles(profile_catalog(profile(orchestration_model="x"))).diagnostics)
