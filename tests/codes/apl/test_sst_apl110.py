"""SST-APL110: a comparison could not restore the live version it closed.

Fires when ADD LIVE VERSION FROM LAST is refused after the candidates; a restore that goes
through stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareLifecycle, LifecycleResult
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compare_world import GIT_SHA, compare_subject, publish
from tests.helpers.snowflake_fake import FakeSnowflake


def run(*, refused: bool) -> LifecycleResult:
    port = FakeSnowflake()
    subject = compare_subject()
    publish(port, subject)
    if refused:
        port.refused = ("ADD LIVE VERSION",)
    return CompareLifecycle(port, run_id="r1", git_sha=GIT_SHA).commit(subject, ("claude-opus-4-8",))


def test_sst_apl110_fires() -> None:
    [diagnostic] = run(refused=True).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL110", Severity.ERROR)
    assert diagnostic.message == (
        "agent:sales_agent: restoring the live version of DB.S.SALES_AGENT after the model comparison failed "
        "(ADD LIVE VERSION failed: recorded refusal: ALTER AGENT DB.S.SALES_AGENT ADD LIVE VERSION FROM LAST); "
        "until it is restored a caller naming no version is served the default, the release"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_apl110_silent() -> None:
    assert run(refused=False).diagnostics == ()
