"""SST-VAL894: a Cortex Code profile's model is not in `snowflake.orchestration_models`."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.coco_profiles import CompiledCocoProfile
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_profiles, profile, profile_catalog


def test_sst_val894_fires() -> None:
    result = compile_profiles(profile_catalog(profile(orchestration_model="claude-4-sonnet")))
    diagnostic = only(result.diagnostics, "SST-VAL894")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "coco profile 'analyst': model 'claude-4-sonnet' is not in the allowlist"
    assert diagnostic.subject == "coco_profile:analyst"
    assert result.compiled == ()


def test_sst_val894_silent() -> None:
    result = compile_profiles(profile_catalog(profile(orchestration_model="auto")))
    assert "SST-VAL894" not in codes(result.diagnostics)
    [compiled] = result.compiled
    assert isinstance(compiled, CompiledCocoProfile)
    assert '"orchestration": "auto"' in compiled.payload
