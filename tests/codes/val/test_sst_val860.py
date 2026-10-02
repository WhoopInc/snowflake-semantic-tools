"""SST-VAL860: profile names an unknown plugin.

Fires when a profile names an undefined plugin; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    validate_profile_catalog,
)
from tests.helpers.eval_inputs import codes, only
from tests.helpers.skill_inputs import (
    plugin,
    profile,
    profile_catalog,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val860_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(profile_catalog(profile(plugins=("ghost-kit",))), SKILLS, {"finance-kit": plugin()}),
        "SST-VAL860",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == ("profile 'analyst': plugin 'ghost-kit' is not a plugin under the plugins directory")
    assert diagnostic.subject == "profile:analyst"


def test_sst_val860_silent() -> None:
    assert "SST-VAL860" not in codes(
        validate_profile_catalog(profile_catalog(profile(plugins=("finance-kit",))), SKILLS, {"finance-kit": plugin()})
    )
