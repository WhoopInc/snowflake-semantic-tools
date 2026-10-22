"""SST-APL108: a comparison's subject is not published exactly as compiled.

Fires when the project renders another definition than state records; the subject as apply
left it stays quiet.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.compare_precheck import published_release
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.compare_world import RELEASE, compare_subject, publish
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl108_fires() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(port, subject)
    edited = replace(subject, rendered=replace(subject.rendered, fingerprint="e" * 64))
    diagnostic = published_release(port, edited, entry, fetched_at="now")
    assert isinstance(diagnostic, Diagnostic)
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL108", Severity.ERROR)
    assert diagnostic.message == (
        "agent:sales_agent: the model comparison needs it published exactly as compiled, and "
        "`sst plan` shows it as update (fingerprint_differs)"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_apl108_silent() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    assert published_release(port, subject, publish(port, subject), fetched_at="now") == RELEASE
