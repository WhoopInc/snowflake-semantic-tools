"""SST-APL105: `apply --temporary` skips the eval of an agent it publishes as temporary.

Fires for an eval whose agent the run creates as a temporary agent; the same eval of a
permanent agent stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.apply.temporary import temporary_notes
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import agent_and_eval, changeset
from tests.helpers.diagnostic_filters import codes, only


def test_sst_apl105_fires() -> None:
    diagnostic = only(temporary_notes(changeset(*agent_and_eval(temporary=True))), "SST-APL105")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == ("eval:sales: skipped, because --temporary publishes agent:sales as a temporary agent")


def test_sst_apl105_silent() -> None:
    assert "SST-APL105" not in codes(temporary_notes(changeset(*agent_and_eval(temporary=False))))
