"""SST-APL038: an artifact `sst test` would probe or `sst eval` run is published from an older compile."""

from __future__ import annotations

from snowflake_semantic_tools.app.publication import Checked, Publication, unpublished_count, unpublished_findings
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_apl038_fires() -> None:
    checked = (
        Checked("eval:e", "DB.S.E", Publication.OLDER),
        Checked("eval:f", "DB.S.F", Publication.DRIFTED),
    )
    older, drifted = unpublished_findings(checked, command="eval")
    assert (older.code, older.severity) == ("SST-APL038", Severity.ERROR)
    assert older.message == "eval:e: DB.S.E is published from an older compile"
    assert drifted.code == "SST-APL012"
    assert unpublished_count(checked) == 2


def test_sst_apl038_silent() -> None:
    published = (Checked("eval:e", "DB.S.E", Publication.PUBLISHED),)
    assert unpublished_findings(published, command="eval") == ()
    assert unpublished_count(published) == 0
