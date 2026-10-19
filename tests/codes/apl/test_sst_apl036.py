"""SST-APL036: an artifact `sst test` would smoke or eval is not published on this target."""

from __future__ import annotations

from snowflake_semantic_tools.app.publication import Checked, Publication, unpublished_findings
from snowflake_semantic_tools.domain.diagnostics import Severity

_PUBLISHED = Checked("semantic_view:menu", "DB.S.MENU", Publication.PUBLISHED)


def test_sst_apl036_fires() -> None:
    absent = Checked("semantic_view:sales", "DB.S.SALES", Publication.ABSENT)
    [diagnostic] = unpublished_findings((_PUBLISHED, absent), suite="smoke")
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL036", Severity.ERROR)
    assert diagnostic.message == "semantic_view:sales: DB.S.SALES is not published on this target"
    assert diagnostic.suggestion == "run `sst plan`, then `sst apply`"


def test_sst_apl036_silent() -> None:
    assert unpublished_findings((_PUBLISHED,), suite="smoke") == ()
    assert unpublished_findings((), suite="smoke") == ()
