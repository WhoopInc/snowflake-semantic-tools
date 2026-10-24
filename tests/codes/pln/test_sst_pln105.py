"""SST-PLN105: the account has no CoWork object, so a profile publishes unattached.

CoWork profiles are a Snowflake private preview. Fires for a profile published to
SNOWFLAKE_INTELLIGENCE.PROFILES when SHOW SNOWFLAKE INTELLIGENCES lists nothing; an account with
a CoWork object stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from tests.helpers.cowork_attach import profile, reading
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.plan_codes import plan


def _plan(attachment: AttachmentReading) -> DiagnosticBag:
    changeset = plan((profile(),), attachment=attachment)
    assert [change.action for change in changeset.changes] == [Action.CREATE]
    return changeset.diagnostics


def test_sst_pln105_fires() -> None:
    diagnostic = only(_plan(AttachmentReading(None)), "SST-PLN105")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.subject == "cowork_profile:everyone"
    assert diagnostic.message == (
        "cowork_profile:everyone: the account has no CoWork object, so the CoWork profile publishes unattached"
    )


def test_sst_pln105_silent() -> None:
    assert "SST-PLN105" not in codes(_plan(reading()))
