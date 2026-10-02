"""SST-VAL816: SKILL.md and bundle byte split.

A skill that bundles reports how much of it is SKILL.md, read on every turn, and how much is
read only on demand; a skill whose bundle has an error builds nothing to report.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.skill_bundle import build_skill_bundle
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import SKILL_MD, skill


def test_sst_val816_fires() -> None:
    bundle, diagnostics = build_skill_bundle(skill())
    diagnostic = only(diagnostics, "SST-VAL816")
    assert bundle is not None
    assert diagnostic.severity is Severity.INFO
    size = len(SKILL_MD.format(name="month-close").replace("reference/steps.md", "reference__steps.md"))
    assert diagnostic.message == (
        f"skill 'month-close': SKILL.md is {size} of the bundle's {size + 7} bytes; 7 are read only on demand"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val816_silent() -> None:
    broken = skill(files={"SKILL.md": SKILL_MD.format(name="month-close").replace("steps.md", "missing.md")})
    bundle, diagnostics = build_skill_bundle(broken)
    assert bundle is None
    assert "SST-VAL816" not in codes(diagnostics)
