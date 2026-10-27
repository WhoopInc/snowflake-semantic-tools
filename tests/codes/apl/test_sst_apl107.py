"""SST-APL107: apply committed an agent version but could not pin the default to it by name.

Fires when SET DEFAULT_VERSION is refused after the update; the same update whose pin runs
stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyResult, OutcomeStatus
from tests.helpers.agent_updates import agent_update, committed
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import changeset
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake


def run(*, refused: bool) -> ApplyResult:
    port = FakeSnowflake()
    committed(port)
    if refused:
        port.refused = ("DEFAULT_VERSION = 'VERSION$3'",)
    return apply_plan(changeset(agent_update()), port)


def test_sst_apl107_fires() -> None:
    result = run(refused=True)
    diagnostic = only(result.diagnostics, "SST-APL107")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent:sales: the default version could not be pinned by name (pinning VERSION$3 failed: "
        "recorded refusal: ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'VERSION$3'); it is left "
        "following LAST, and the next `sst plan` pins it"
    )
    assert result.outcomes[0].status is OutcomeStatus.APPLIED
    assert result.success


def test_sst_apl107_silent() -> None:
    assert "SST-APL107" not in codes(run(refused=False).diagnostics)
