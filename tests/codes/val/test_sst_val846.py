"""SST-VAL846: profile names an unknown hook.

Fires when a profile names an undefined hook; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.profile import (
    validate_profile_catalog,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    hook,
    profile,
    profile_catalog,
    skill,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val846_fires() -> None:
    diagnostic = only(
        validate_profile_catalog(profile_catalog(profile(hooks=("nope",)), hooks=(hook(),)), SKILLS), "SST-VAL846"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "profile 'analyst': hook 'nope' is not defined under the hooks directory"
    assert diagnostic.subject == "profile:analyst"


def test_sst_val846_silent() -> None:
    assert "SST-VAL846" not in codes(
        validate_profile_catalog(profile_catalog(profile(hooks=("sql-safety",)), hooks=(hook(),)), SKILLS)
    )
