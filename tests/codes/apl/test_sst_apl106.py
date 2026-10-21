"""SST-APL106: `apply --temporary` leaves out every CoWork profile, a Snowflake private preview feature.

Fires for a profile a temporary run would create; the same profile in a permanent run stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.apply.temporary import temporary_notes
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import agent_and_cowork_profile, changeset
from tests.helpers.diagnostic_filters import codes, only


def test_sst_apl106_fires() -> None:
    notes = temporary_notes(changeset(*agent_and_cowork_profile(temporary=True)), temporary=True)
    diagnostic = only(notes, "SST-APL106")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "cowork_profile:everyone: skipped, because --temporary publishes no CoWork profile"
    assert diagnostic.subject == "cowork_profile:everyone"


def test_sst_apl106_silent() -> None:
    assert "SST-APL106" not in codes(temporary_notes(changeset(*agent_and_cowork_profile(temporary=False))))
