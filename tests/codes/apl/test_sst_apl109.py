"""SST-APL109: a comparison could not commit the hold copy or a candidate's version.

Fires when a candidate's ADD VERSION is refused; the same comparison with nothing refused
stays quiet.
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
        port.refused = ("/claude_opus_4_8/",)
    return CompareLifecycle(port, run_id="r1", git_sha=GIT_SHA).commit(subject, ("claude-opus-4-8",))


def test_sst_apl109_fires() -> None:
    [diagnostic] = run(refused=True).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL109", Severity.ERROR)
    assert diagnostic.message.startswith(
        "agent:sales_agent: the model comparison could not commit the candidate claude-opus-4-8: "
        "ADD VERSION failed: recorded refusal: ALTER AGENT DB.S.SALES_AGENT"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_apl109_silent() -> None:
    assert run(refused=False).diagnostics == ()
