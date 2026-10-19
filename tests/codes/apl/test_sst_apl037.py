"""SST-APL037: nothing a test suite would smoke or eval is published on this target."""

from __future__ import annotations

from snowflake_semantic_tools.app.publication import Checked, Publication, unpublished_findings
from snowflake_semantic_tools.domain.diagnostics import Severity


def test_sst_apl037_fires() -> None:
    absent = tuple(Checked(f"semantic_view:{name}", f"DB.S.{name.upper()}", Publication.ABSENT) for name in "abc")
    [diagnostic] = unpublished_findings(absent, suite="smoke")
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL037", Severity.ERROR)
    assert diagnostic.message == "nothing the smoke suite tests is published on this target (3 not published)"


def test_sst_apl037_silent() -> None:
    # One artifact published is enough for each unpublished one to be named instead.
    mixed = (
        Checked("semantic_view:a", "DB.S.A", Publication.ABSENT),
        Checked("semantic_view:b", "DB.S.B", Publication.PUBLISHED),
    )
    assert [item.code for item in unpublished_findings(mixed, suite="smoke")] == ["SST-APL036"]
    older = (Checked("semantic_view:a", "DB.S.A", Publication.OLDER),)
    assert [item.code for item in unpublished_findings(older, suite="evals")] == ["SST-APL038"]
