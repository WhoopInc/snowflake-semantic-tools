"""SST-VAL845: a Cortex Code profile repeats a skill its shared layer already carries.

Fires when a profile lists a skill `shared/` lists; a profile listing only skills of its own stays
quiet. The repeated skill is pinned once, where the shared layer names it.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.coco_profiles import CompiledCocoProfile
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_profiles, profile, profile_catalog, shared


def test_sst_val845_fires() -> None:
    result = compile_profiles(profile_catalog(shared=shared(skills=("month-close",))))
    diagnostic = only(result.diagnostics, "SST-VAL845")
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == "coco profile 'analyst': skill 'month-close' already reaches every profile through shared/"
    )
    assert diagnostic.subject == "coco_profile:analyst"
    [compiled] = result.compiled
    assert isinstance(compiled, CompiledCocoProfile)
    assert [skill.path for skill in compiled.resolved.model.skills] == ["DB.S.MONTH_CLOSE"]


def test_sst_val845_silent() -> None:
    catalog = profile_catalog(profile(skills=()), shared=shared(skills=("month-close",)))
    assert "SST-VAL845" not in codes(compile_profiles(catalog).diagnostics)
