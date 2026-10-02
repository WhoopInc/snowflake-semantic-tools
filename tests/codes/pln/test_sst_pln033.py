"""SST-PLN033: under --partial, an error names no artifact, so nothing can be left out and the run stops."""

from __future__ import annotations

from snowflake_semantic_tools.app.partial import partial_refusal
from snowflake_semantic_tools.domain.diagnostics import D, Severity
from tests.helpers.plan_codes import partial_result as result


def test_sst_pln033_fires() -> None:
    refusal = partial_refusal(result(D("SST-CFG047", subject="config:project.hooks_dir", key="k", value="v")))
    assert refusal is not None
    assert (refusal.code, refusal.severity) == ("SST-PLN033", Severity.INFO)
    assert refusal.message == (
        "--partial publishes nothing: SST-CFG047 on config:project.hooks_dir cannot be traced to the artifacts it "
        "would change"
    )


def test_sst_pln033_silent() -> None:
    assert partial_refusal(result(D("SST-REF044", subject="semantic_view:menu", artifact="menu", found="x"))) is None
